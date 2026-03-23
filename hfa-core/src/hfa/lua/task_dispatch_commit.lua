-- V23.3 task_dispatch_commit.lua
-- Atomically commits a READY DAG task to SCHEDULED and emits both control and
-- shard events. This is the task-level analogue of run dispatch commit.
--
-- KEYS:
-- 1  task_state_key
-- 2  task_meta_key
-- 3  task_running_zset_key
-- 4  control_stream_key
-- 5  shard_stream_key
--
-- ARGV:
-- 1  task_id
-- 2  run_id
-- 3  tenant_id
-- 4  agent_type
-- 5  worker_group
-- 6  shard
-- 7  priority
-- 8  admitted_at
-- 9  scheduled_at
-- 10 task_state_ttl
-- 11 task_meta_ttl
-- 12 control_stream_maxlen
-- 13 shard_stream_maxlen
-- 14 trace_parent
-- 15 trace_state
-- 16 policy
-- 17 region
-- 18 payload_json

local current = redis.call('GET', KEYS[1])
if not current then
  return {'state_conflict', 'missing_state'}
end

if current == 'scheduled' or current == 'running' then
  return {'already_running', current}
end

if current == 'done' or current == 'failed' or current == 'blocked' or current == 'skipped' then
  return {'illegal_transition', current}
end

if current ~= 'ready' then
  return {'state_conflict', current}
end

local task_id = ARGV[1]
local run_id = ARGV[2]
local tenant_id = ARGV[3]
local agent_type = ARGV[4]
local worker_group = ARGV[5]
local shard = tostring(ARGV[6])
local priority = tostring(ARGV[7])
local admitted_at = tostring(ARGV[8])
local scheduled_at = tostring(ARGV[9])
local task_state_ttl = tonumber(ARGV[10]) or 86400
local task_meta_ttl = tonumber(ARGV[11]) or 86400
local control_stream_maxlen = tonumber(ARGV[12]) or 10000
local shard_stream_maxlen = tonumber(ARGV[13]) or 10000
local trace_parent = ARGV[14] or ''
local trace_state = ARGV[15] or ''
local policy = ARGV[16] or 'LEAST_LOADED'
local region = ARGV[17] or ''
local payload_json = ARGV[18] or '{}'

redis.call('SET', KEYS[1], 'scheduled', 'EX', task_state_ttl)
redis.call('HSET', KEYS[2],
  'task_id', task_id,
  'run_id', run_id,
  'tenant_id', tenant_id,
  'agent_type', agent_type,
  'worker_group', worker_group,
  'shard', shard,
  'priority', priority,
  'admitted_at', admitted_at,
  'scheduled_at', scheduled_at,
  'dispatch_policy', policy,
  'dispatch_region', region,
  'payload_json', payload_json,
  'trace_parent', trace_parent,
  'trace_state', trace_state
)
redis.call('EXPIRE', KEYS[2], task_meta_ttl)
redis.call('ZADD', KEYS[3], scheduled_at, task_id)
redis.call('EXPIRE', KEYS[3], task_meta_ttl)

redis.call('XADD', KEYS[4], 'MAXLEN', '~', control_stream_maxlen, '*',
  'event_type', 'TaskScheduled',
  'task_id', task_id,
  'run_id', run_id,
  'tenant_id', tenant_id,
  'agent_type', agent_type,
  'worker_group', worker_group,
  'shard', shard,
  'region', region,
  'policy', policy,
  'scheduled_at', scheduled_at,
  'trace_parent', trace_parent,
  'trace_state', trace_state
)

redis.call('XADD', KEYS[5], 'MAXLEN', '~', shard_stream_maxlen, '*',
  'event_type', 'TaskRequested',
  'task_id', task_id,
  'run_id', run_id,
  'tenant_id', tenant_id,
  'agent_type', agent_type,
  'worker_group', worker_group,
  'shard', shard,
  'priority', priority,
  'payload_json', payload_json,
  'requested_at', scheduled_at,
  'trace_parent', trace_parent,
  'trace_state', trace_state
)

return {'committed', current}
