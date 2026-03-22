--[[
IRONCLAD Sprint 23 — End-to-end atomic dispatch commit

Atomically performs:
  * CAS on run state (admitted|queued -> scheduled)
  * metadata update
  * running ZSET insert
  * control stream XADD (RunScheduled)
  * shard stream XADD (RunRequested)

Any rejection returns before side effects. There is no post-commit Python publish gap.

KEYS[1]  state_key
KEYS[2]  meta_key
KEYS[3]  running_zset
KEYS[4]  control_stream
KEYS[5]  shard_stream

ARGV[1]  run_id
ARGV[2]  tenant_id
ARGV[3]  agent_type
ARGV[4]  worker_group
ARGV[5]  shard
ARGV[6]  reschedule_count
ARGV[7]  admitted_at
ARGV[8]  scheduled_at
ARGV[9]  state_ttl
ARGV[10] meta_ttl
ARGV[11] control_stream_maxlen
ARGV[12] shard_stream_maxlen
ARGV[13] priority
ARGV[14] payload_json
ARGV[15] idempotency_key
ARGV[16] trace_parent
ARGV[17] trace_state
ARGV[18] policy
ARGV[19] region
ARGV[20] scheduled_event_id
ARGV[21] requested_event_id
ARGV[22] scheduled_event_ts
ARGV[23] requested_event_ts
]]

local state_key      = KEYS[1]
local meta_key       = KEYS[2]
local running_key    = KEYS[3]
local control_stream = KEYS[4]
local shard_stream   = KEYS[5]

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
local control_maxlen    = tonumber(ARGV[11])
local shard_maxlen      = tonumber(ARGV[12])
local priority          = ARGV[13]
local payload_json      = ARGV[14]
local idempotency_key   = ARGV[15]
local trace_parent      = ARGV[16]
local trace_state       = ARGV[17]
local policy            = ARGV[18]
local region            = ARGV[19]
local sched_event_id    = ARGV[20]
local req_event_id      = ARGV[21]
local sched_event_ts    = ARGV[22]
local req_event_ts      = ARGV[23]

local current_state = redis.call('GET', state_key)
if current_state == false then
    return {'state_conflict', 'missing_state'}
end

local cs = current_state
if cs ~= 'admitted' and cs ~= 'queued' then
    if cs == 'scheduled' or cs == 'running' then
        return {'already_running', cs}
    end
    return {'illegal_transition', cs}
end

redis.call('SET', state_key, 'scheduled', 'EX', state_ttl)
redis.call('HSET', meta_key,
    'run_id', run_id,
    'tenant_id', tenant_id,
    'agent_type', agent_type,
    'worker_group', worker_group,
    'shard', shard,
    'reschedule_count', reschedule_count,
    'admitted_at', admitted_at,
    'scheduled_at', scheduled_at,
    'queue_state', 'scheduled'
)
redis.call('EXPIRE', meta_key, meta_ttl)
redis.call('ZADD', running_key, tonumber(scheduled_at), run_id)

if control_maxlen ~= nil and control_maxlen > 0 then
    redis.call('XADD', control_stream, 'MAXLEN', '~', control_maxlen, '*',
        'event_id', sched_event_id,
        'timestamp', sched_event_ts,
        'trace_parent', trace_parent,
        'trace_state', trace_state,
        'event_type', 'RunScheduled',
        'run_id', run_id,
        'tenant_id', tenant_id,
        'agent_type', agent_type,
        'worker_group', worker_group,
        'shard', shard,
        'region', region,
        'policy', policy,
        'scheduled_at', scheduled_at
    )
else
    redis.call('XADD', control_stream, '*',
        'event_id', sched_event_id,
        'timestamp', sched_event_ts,
        'trace_parent', trace_parent,
        'trace_state', trace_state,
        'event_type', 'RunScheduled',
        'run_id', run_id,
        'tenant_id', tenant_id,
        'agent_type', agent_type,
        'worker_group', worker_group,
        'shard', shard,
        'region', region,
        'policy', policy,
        'scheduled_at', scheduled_at
    )
end

if shard_maxlen ~= nil and shard_maxlen > 0 then
    redis.call('XADD', shard_stream, 'MAXLEN', '~', shard_maxlen, '*',
        'event_id', req_event_id,
        'timestamp', req_event_ts,
        'trace_parent', trace_parent,
        'trace_state', trace_state,
        'event_type', 'RunRequested',
        'run_id', run_id,
        'tenant_id', tenant_id,
        'agent_type', agent_type,
        'priority', priority,
        'payload', payload_json,
        'idempotency_key', idempotency_key
    )
else
    redis.call('XADD', shard_stream, '*',
        'event_id', req_event_id,
        'timestamp', req_event_ts,
        'trace_parent', trace_parent,
        'trace_state', trace_state,
        'event_type', 'RunRequested',
        'run_id', run_id,
        'tenant_id', tenant_id,
        'agent_type', agent_type,
        'priority', priority,
        'payload', payload_json,
        'idempotency_key', idempotency_key
    )
end

return {'committed', run_id}
