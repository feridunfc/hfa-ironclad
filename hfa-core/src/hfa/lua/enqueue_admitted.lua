--[[
hfa-core/src/hfa/lua/enqueue_admitted.lua
IRONCLAD Sprint 18 — Atomic fair-queue enqueue

Problem (Sprint 16)
-------------------
_enqueue_admitted() did two separate Redis writes:
  1. ZADD  hfa:tenant:{id}:queue      (tenant queue)
  2. HSET  hfa:run:meta:{run_id}      (run meta)
  + EXPIRE hfa:run:meta:{run_id}
  + SADD   hfa:cp:tenant:active       (active tenant index)
  + EXPIRE hfa:tenant:{id}:queue

Between steps 1 and 2, a crash could produce a "ghost run":
  - run is in the queue but has no meta → silent data loss on dispatch

Sprint 18 fix
-------------
All five operations run inside a single Lua script — one network round-trip,
one atomic execution on the Redis server. Either all writes succeed or none
are visible to the scheduler.

Arguments
---------
KEYS[1]  tenant queue ZSET key          hfa:tenant:{id}:queue
KEYS[2]  run meta HASH key              hfa:run:meta:{run_id}
KEYS[3]  active tenant SET key          hfa:cp:tenant:active
KEYS[4]  run state STRING key           hfa:run:state:{run_id}

ARGV[1]  score (float)                  priority score
ARGV[2]  run_id (string)               
ARGV[3]  tenant_id (string)            
ARGV[4]  agent_type (string)           
ARGV[5]  priority (string, int)        
ARGV[6]  preferred_region (string)     
ARGV[7]  preferred_placement (string)  
ARGV[8]  admitted_at (string, float)   
ARGV[9]  queue_ttl (int seconds)       
ARGV[10] meta_ttl (int seconds)        
ARGV[11] state_ttl (int seconds)       

Returns
-------
1   enqueued successfully (new entry)
0   already in queue (NX guard — idempotent)

Guarantees
----------
* NX flag: re-enqueueing the same run_id is a no-op (idempotent).
* Active tenant SET is maintained atomically alongside the queue.
* run_state is set to "queued" (allows state machine introspection).
* All TTLs refreshed on every call.
--]]

local queue_key    = KEYS[1]
local meta_key     = KEYS[2]
local active_key   = KEYS[3]
local state_key    = KEYS[4]

local score        = tonumber(ARGV[1])
local run_id       = ARGV[2]
local tenant_id    = ARGV[3]
local agent_type   = ARGV[4]
local priority     = ARGV[5]
local region       = ARGV[6]
local placement    = ARGV[7]
local admitted_at  = ARGV[8]
local queue_ttl    = tonumber(ARGV[9])
local meta_ttl     = tonumber(ARGV[10])
local state_ttl    = tonumber(ARGV[11])

-- 1. Enqueue (NX — no-op if run_id already present)
local added = redis.call('ZADD', queue_key, 'NX', score, run_id)

if added == 0 then
    -- Already queued (idempotent re-enqueue from autoclaim recovery)
    return 0
end

-- 2. Write canonical run meta (source of truth for dispatch reconstruction)
redis.call('HSET', meta_key,
    'run_id',              run_id,
    'tenant_id',           tenant_id,
    'agent_type',          agent_type,
    'priority',            priority,
    'preferred_region',    region,
    'preferred_placement', placement,
    'admitted_at',         admitted_at,
    'queue_state',         'queued'
)

-- 3. Set run state to "queued" (enables state machine introspection)
redis.call('SET', state_key, 'queued', 'EX', state_ttl)

-- 4. Add tenant to active index
redis.call('SADD', active_key, tenant_id)

-- 5. Refresh TTLs
redis.call('EXPIRE', queue_key, queue_ttl)
redis.call('EXPIRE', meta_key, meta_ttl)

return 1
