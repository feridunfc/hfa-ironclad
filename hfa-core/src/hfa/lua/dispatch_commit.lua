--[[
hfa-core/src/hfa/lua/dispatch_commit.lua
IRONCLAD Sprint 18 — Atomic dispatch commit

Problem (Sprint 16)
-------------------
_schedule() wrote multiple Redis keys sequentially:
  1. HSET hfa:run:meta     (update with worker_group, shard)
  2. SET  hfa:run:state    (→ "scheduled")
  3. ZADD hfa:cp:running   (add to running set)
  4. XADD hfa:stream:control (RunScheduledEvent)
  5. XADD hfa:stream:runs:{shard} (RunRequestedEvent)

Between steps 1 and 5:
  - Double-dispatch race: another scheduler instance reads "admitted" state
    and schedules the same run to a different worker
  - State inconsistency: run_state="scheduled" but running_zset not yet updated

Sprint 18 fix
-------------
Steps 1-3 are gated by a precondition check inside a Lua script:
  - ONLY commit if run_state is still "admitted" or "queued" 
  - Atomic state transition prevents double-dispatch
  - Steps 4-5 (XADD streams) happen after the commit succeeds

Note: Redis XADD cannot be called inside Lua on all versions, and streams
have length management (MAXLEN) that is complex in Lua. So the commit
covers the critical state + metadata + running_zset, and XADD calls
remain in Python but only execute after commit returns 1.

Arguments
---------
KEYS[1]  run state STRING key       hfa:run:state:{run_id}
KEYS[2]  run meta HASH key          hfa:run:meta:{run_id}
KEYS[3]  running ZSET key           hfa:cp:running

ARGV[1]  run_id (string)
ARGV[2]  tenant_id (string)
ARGV[3]  agent_type (string)
ARGV[4]  worker_group (string)
ARGV[5]  shard (string, int)
ARGV[6]  reschedule_count (string, int)
ARGV[7]  admitted_at (string, float)
ARGV[8]  scheduled_at (string, float)
ARGV[9]  state_ttl (int seconds)
ARGV[10] meta_ttl (int seconds)

Returns
-------
1   committed — state transitioned to "scheduled", meta updated, added to running ZSET
0   rejected  — run is not in a schedulable state (already scheduled or terminal)

Precondition
------------
run_state must be "admitted" or "queued" to proceed.
Any other state (scheduled, running, done, failed, rejected) → return 0.
This prevents double-dispatch and stale-state dispatch.
--]]

local state_key   = KEYS[1]
local meta_key    = KEYS[2]
local running_key = KEYS[3]

local run_id            = ARGV[1]
local tenant_id         = ARGV[2]
local agent_type        = ARGV[3]
local worker_group      = ARGV[4]
local shard             = ARGV[5]
local reschedule_count  = ARGV[6]
local admitted_at       = ARGV[7]
local scheduled_at      = ARGV[8]
local state_ttl         = tonumber(ARGV[9])
local meta_ttl          = tonumber(ARGV[10])

-- 1. Precondition: only proceed from "admitted" or "queued" state
local current_state = redis.call('GET', state_key)
if current_state == false then
    return {'state_conflict', 'missing_state'}
end

local cs = current_state
if type(cs) == 'userdata' then
    cs = tostring(cs)
end

if cs ~= 'admitted' and cs ~= 'queued' then
    if cs == 'scheduled' or cs == 'running' then
        return {'already_running', cs}
    end
    return {'illegal_transition', cs}
end

-- 2. Transition state to "scheduled"
redis.call('SET', state_key, 'scheduled', 'EX', state_ttl)

-- 3. Update run meta with placement decision
redis.call('HSET', meta_key,
    'run_id',            run_id,
    'tenant_id',         tenant_id,
    'agent_type',        agent_type,
    'worker_group',      worker_group,
    'shard',             shard,
    'reschedule_count',  reschedule_count,
    'admitted_at',       admitted_at,
    'scheduled_at',      scheduled_at,
    'queue_state',       'scheduled'
)
redis.call('EXPIRE', meta_key, meta_ttl)

-- 4. Add to running ZSET (score = scheduled_at timestamp)
redis.call('ZADD', running_key, tonumber(scheduled_at), run_id)

return {'committed', run_id}
