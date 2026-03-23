
-- V23.2 task_admit.lua
-- Atomically seeds a DAG task and emits it to the tenant READY queue iff it has no dependencies.
--
-- KEYS:
-- 1  run_graph_key
-- 2  run_tasks_key
-- 3  task_meta_key
-- 4  task_state_key
-- 5  task_remaining_deps_key
-- 6  tenant_ready_queue_key
-- 7  task_ready_emitted_key
-- 8  task_children_key
-- 9  tenant_active_set_key
--
-- ARGV:
-- 1  task_id
-- 2  run_id
-- 3  tenant_id
-- 4  agent_type
-- 5  priority
-- 6  admitted_at
-- 7  estimated_cost_cents
-- 8  preferred_region
-- 9  preferred_placement
-- 10 payload_json
-- 11 trace_parent
-- 12 trace_state
-- 13 dependency_count
-- 14 graph_ttl
-- 15 task_meta_ttl
-- 16 task_state_ttl
-- 17 ready_ttl
-- 18.. child_task_ids

local task_id = ARGV[1]
local run_id = ARGV[2]
local tenant_id = ARGV[3]
local agent_type = ARGV[4]
local priority = tostring(ARGV[5])
local admitted_at = tostring(ARGV[6])
local estimated_cost_cents = tostring(ARGV[7])
local preferred_region = ARGV[8] or ''
local preferred_placement = ARGV[9] or 'LEAST_LOADED'
local payload_json = ARGV[10] or '{}'
local trace_parent = ARGV[11] or ''
local trace_state = ARGV[12] or ''
local dependency_count = tonumber(ARGV[13]) or 0
local graph_ttl = tonumber(ARGV[14]) or 86400
local task_meta_ttl = tonumber(ARGV[15]) or 86400
local task_state_ttl = tonumber(ARGV[16]) or 86400
local ready_ttl = tonumber(ARGV[17]) or 86400

if redis.call('EXISTS', KEYS[4]) == 1 then
  return {'already_exists'}
end

redis.call('SADD', KEYS[1], run_id)
redis.call('EXPIRE', KEYS[1], graph_ttl)
redis.call('SADD', KEYS[2], task_id)
redis.call('EXPIRE', KEYS[2], graph_ttl)

redis.call('HSET', KEYS[3],
  'task_id', task_id,
  'run_id', run_id,
  'tenant_id', tenant_id,
  'agent_type', agent_type,
  'priority', priority,
  'admitted_at', admitted_at,
  'estimated_cost_cents', estimated_cost_cents,
  'preferred_region', preferred_region,
  'preferred_placement', preferred_placement,
  'payload_json', payload_json,
  'trace_parent', trace_parent,
  'trace_state', trace_state
)
redis.call('EXPIRE', KEYS[3], task_meta_ttl)

redis.call('SET', KEYS[5], dependency_count, 'EX', task_state_ttl)

for i = 18, #ARGV do
  redis.call('SADD', KEYS[8], ARGV[i])
end
if #ARGV >= 18 then
  redis.call('EXPIRE', KEYS[8], task_meta_ttl)
end

if dependency_count <= 0 then
  redis.call('SET', KEYS[4], 'ready', 'EX', task_state_ttl)
  redis.call('ZADD', KEYS[6], 'NX', admitted_at, task_id)
  redis.call('EXPIRE', KEYS[6], ready_ttl)
  redis.call('SET', KEYS[7], '1', 'EX', ready_ttl)
  redis.call('SADD', KEYS[9], tenant_id)
  return {'seeded_root'}
else
  redis.call('SET', KEYS[4], 'pending', 'EX', task_state_ttl)
  return {'seeded_waiting'}
end
