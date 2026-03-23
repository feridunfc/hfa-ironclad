
-- V24.2A owner-aware task completion + output persistence
-- KEYS:
-- 1=task_state_key 2=task_meta_key 3=run_graph_key 4=task_children_key 5=task_output_key
-- 6=task_key_prefix 7=tenant_ready_queue 8=ready_emitted_prefix 9=completion_stream 10=task_running_zset
-- ARGV:
-- 1=task_id 2=run_id 3=tenant_id 4=terminal_state 5=expected_state 6=finished_at_ms
-- 7=state_ttl 8=meta_ttl 9=dag_ttl 10=ready_score 11=reason_code 12=worker_group 13=shard
-- 14=trace_parent 15=trace_state 16=stream_maxlen 17=worker_instance_id 18=task_output

local task_state_key = KEYS[1]
local task_meta_key = KEYS[2]
local task_children_key = KEYS[4]
local task_output_key = KEYS[5]
local task_key_prefix = KEYS[6]
local tenant_ready_queue = KEYS[7]
local completion_stream = KEYS[9]
local task_running_zset = KEYS[10]

local task_id = ARGV[1]
local run_id = ARGV[2]
local tenant_id = ARGV[3]
local terminal_state = ARGV[4]
local expected_state = ARGV[5]
local finished_at_ms = ARGV[6]
local state_ttl = tonumber(ARGV[7])
local meta_ttl = tonumber(ARGV[8])
local dag_ttl = tonumber(ARGV[9])
local ready_score = tonumber(ARGV[10])
local reason_code = ARGV[11]
local worker_group = ARGV[12]
local shard = ARGV[13]
local trace_parent = ARGV[14]
local trace_state = ARGV[15]
local stream_maxlen = tonumber(ARGV[16])
local caller_worker_id = ARGV[17]
local task_output = ARGV[18]

local function is_terminal(state)
    return state == 'done' or state == 'failed' or state == 'blocked_by_failure'
end

local current_state = redis.call('GET', task_state_key)
if not current_state then
    return {0, 'missing_task', 0, 0}
end

if is_terminal(current_state) then
    return {0, 'already_terminal', 0, 1}
end

if current_state ~= expected_state then
    return {0, 'illegal_transition', 0, 0}
end

local current_owner = redis.call('HGET', task_meta_key, 'worker_instance_id')
if current_owner and current_owner ~= '' and current_owner ~= caller_worker_id then
    return {0, 'task_owner_mismatch', 0, 0}
end

redis.call('SET', task_state_key, terminal_state, 'EX', state_ttl)
redis.call('HSET', task_meta_key, 'completed_at_ms', finished_at_ms, 'completion_reason', reason_code)
redis.call('EXPIRE', task_meta_key, meta_ttl)
redis.call('ZREM', task_running_zset, task_id)

if terminal_state == 'done' and task_output and task_output ~= '' then
    redis.call('SET', task_output_key, task_output, 'EX', dag_ttl)
end

local unlocked = 0
if terminal_state == 'done' then
    local children = redis.call('SMEMBERS', task_children_key)
    for _, child in ipairs(children) do
        local dep_key = task_key_prefix .. child .. ':deps_remaining'
        local child_state_key = task_key_prefix .. child .. ':state'
        local remaining = redis.call('DECR', dep_key)
        if remaining < 0 then
            redis.call('SET', dep_key, 0)
            remaining = 0
        end
        if remaining == 0 then
            local child_state = redis.call('GET', child_state_key)
            if child_state == 'pending' then
                redis.call('SET', child_state_key, 'ready', 'EX', state_ttl)
                redis.call('ZADD', tenant_ready_queue, 'NX', ready_score, child)
                unlocked = unlocked + 1
            end
        end
    end
end

redis.call('XADD', completion_stream, 'MAXLEN', '~', stream_maxlen, '*',
    'event_type', 'TaskCompleted',
    'task_id', task_id,
    'run_id', run_id,
    'tenant_id', tenant_id,
    'terminal_state', terminal_state,
    'reason_code', reason_code,
    'worker_group', worker_group,
    'shard', shard,
    'trace_parent', trace_parent,
    'trace_state', trace_state,
    'worker_instance_id', caller_worker_id,
    'unlocked_count', tostring(unlocked),
    'has_output', (task_output and task_output ~= '') and '1' or '0',
    'at_ms', finished_at_ms
)

return {1, terminal_state, unlocked, 0}
