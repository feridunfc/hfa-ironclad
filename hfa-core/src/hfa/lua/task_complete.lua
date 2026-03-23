-- task_complete.lua
-- Atomic task completion + direct-child unlock for DAG execution.
--
-- KEYS:
-- 1 = task_state_key
-- 2 = task_meta_key
-- 3 = run_meta_key
-- 4 = task_children_key
-- 5 = task_key_prefix      (e.g. hfa:dag:task:)
-- 6 = ready_queue_key
-- 7 = ready_emitted_prefix (e.g. hfa:dag:task:)
-- 8 = completion_stream_key
-- 9 = running_zset_key
--
-- ARGV:
-- 1 = task_id
-- 2 = run_id
-- 3 = tenant_id
-- 4 = terminal_state       (done|failed|skipped)
-- 5 = expected_state       (running)
-- 6 = finished_at_ms
-- 7 = state_ttl_s
-- 8 = meta_ttl_s
-- 9 = ready_queue_score
-- 10 = reason_code
-- 11 = worker_group
-- 12 = shard
-- 13 = trace_parent
-- 14 = trace_state
-- 15 = completion_stream_maxlen
--
-- RETURN: {status, detail, unlocked_count}

local task_state_key           = KEYS[1]
local task_meta_key            = KEYS[2]
local run_meta_key             = KEYS[3]
local task_children_key        = KEYS[4]
local task_key_prefix          = KEYS[5]
local ready_queue_key          = KEYS[6]
local ready_emitted_prefix     = KEYS[7]
local completion_stream_key    = KEYS[8]
local running_zset_key         = KEYS[9]

local task_id                  = ARGV[1]
local run_id                   = ARGV[2]
local tenant_id                = ARGV[3]
local terminal_state           = ARGV[4]
local expected_state           = ARGV[5]
local finished_at_ms           = ARGV[6]
local state_ttl_s              = tonumber(ARGV[7])
local meta_ttl_s               = tonumber(ARGV[8])
local ready_queue_score        = tonumber(ARGV[9])
local reason_code              = ARGV[10]
local worker_group             = ARGV[11]
local shard                    = ARGV[12]
local trace_parent             = ARGV[13]
local trace_state              = ARGV[14]
local completion_stream_maxlen = tonumber(ARGV[15])

if terminal_state ~= 'done' and terminal_state ~= 'failed' and terminal_state ~= 'skipped' then
  return {'illegal_terminal_state', terminal_state, 0}
end

local current_state = redis.call('GET', task_state_key)
if not current_state then
  return {'state_conflict', 'missing_state', 0}
end

if current_state == 'done' or current_state == 'failed' or current_state == 'skipped' or current_state == 'blocked' then
  return {'already_terminal', current_state, 0}
end

if current_state ~= expected_state then
  return {'state_conflict', current_state, 0}
end

redis.call('SET', task_state_key, terminal_state, 'EX', state_ttl_s)
redis.call('HSET', task_meta_key,
  'task_id', task_id,
  'run_id', run_id,
  'tenant_id', tenant_id,
  'finished_at_ms', finished_at_ms,
  'terminal_state', terminal_state,
  'completion_reason', reason_code,
  'worker_group', worker_group,
  'shard', shard,
  'trace_parent', trace_parent,
  'trace_state', trace_state
)
redis.call('EXPIRE', task_meta_key, meta_ttl_s)
redis.call('ZREM', running_zset_key, task_id)

if terminal_state == 'done' then
  redis.call('HINCRBY', run_meta_key, 'tasks_done', 1)
elseif terminal_state == 'failed' then
  redis.call('HINCRBY', run_meta_key, 'tasks_failed', 1)
elseif terminal_state == 'skipped' then
  redis.call('HINCRBY', run_meta_key, 'tasks_skipped', 1)
end

local unlocked = 0

if terminal_state == 'done' then
  local children = redis.call('SMEMBERS', task_children_key)
  for _, child in ipairs(children) do
    local child_remaining_key = task_key_prefix .. child .. ':remaining_deps'
    local child_state_key = task_key_prefix .. child .. ':state'
    local ready_marker_key = ready_emitted_prefix .. child .. ':ready_emitted'

    local remaining = redis.call('DECR', child_remaining_key)

    if remaining == 0 then
      local child_state = redis.call('GET', child_state_key)
      if child_state == 'pending' then
        redis.call('SET', child_state_key, 'ready', 'EX', state_ttl_s)
        if redis.call('SETNX', ready_marker_key, finished_at_ms) == 1 then
          redis.call('EXPIRE', ready_marker_key, state_ttl_s)
          redis.call('ZADD', ready_queue_key, ready_queue_score, child)
          unlocked = unlocked + 1
        end
      end
    end
  end
end

if completion_stream_key ~= '' then
  if completion_stream_maxlen and completion_stream_maxlen > 0 then
    redis.call('XADD', completion_stream_key, 'MAXLEN', '~', completion_stream_maxlen, '*',
      'event_type', 'TaskCompleted',
      'task_id', task_id,
      'run_id', run_id,
      'tenant_id', tenant_id,
      'terminal_state', terminal_state,
      'reason_code', reason_code,
      'unlocked_count', tostring(unlocked),
      'trace_parent', trace_parent,
      'trace_state', trace_state
    )
  else
    redis.call('XADD', completion_stream_key, '*',
      'event_type', 'TaskCompleted',
      'task_id', task_id,
      'run_id', run_id,
      'tenant_id', tenant_id,
      'terminal_state', terminal_state,
      'reason_code', reason_code,
      'unlocked_count', tostring(unlocked),
      'trace_parent', trace_parent,
      'trace_state', trace_state
    )
  end
end

return {'completed', terminal_state, tostring(unlocked)}
