
-- task_claim_start.lua
-- KEYS: 1=task_state_key 2=task_meta_key 3=running_zset_key 4=reservation_key
-- ARGV: 1=task_id 2=worker_instance_id 3=claimed_at_ms 4=state_ttl 5=meta_ttl 6=heartbeat_score 7=expected_scheduler_epoch

local task_state_key = KEYS[1]
local task_meta_key = KEYS[2]
local running_zset_key = KEYS[3]
local reservation_key = KEYS[4]

local task_id = ARGV[1]
local worker_instance_id = ARGV[2]
local claimed_at_ms = ARGV[3]
local state_ttl = tonumber(ARGV[4])
local meta_ttl = tonumber(ARGV[5])
local heartbeat_score = tonumber(ARGV[6])
local expected_scheduler_epoch = ARGV[7]

local current_state = redis.call('GET', task_state_key)
if not current_state then
    return {"task_missing_in_redis", ""}
end

if current_state ~= 'scheduled' then
    if current_state == 'running' then
        return {"task_already_owned", current_state}
    else
        return {"task_running_state_conflict", current_state}
    end
end

if redis.call('EXISTS', reservation_key) == 0 then
    return {"reservation_missing", ""}
end

local reserved_worker_id = redis.call('HGET', reservation_key, 'worker_id')
local reserved_task_id = redis.call('HGET', reservation_key, 'task_id')
local reserved_epoch = redis.call('HGET', reservation_key, 'scheduler_epoch')

if reserved_worker_id ~= worker_instance_id then
    return {"reservation_worker_mismatch", reserved_worker_id or ""}
end

if reserved_task_id ~= task_id then
    return {"reservation_task_mismatch", reserved_task_id or ""}
end

if expected_scheduler_epoch ~= '' and reserved_epoch ~= expected_scheduler_epoch then
    return {"reservation_epoch_mismatch", reserved_epoch or ""}
end

redis.call('SET', task_state_key, 'running', 'EX', state_ttl)
redis.call('HSET', task_meta_key,
    'worker_instance_id', worker_instance_id,
    'claimed_at_ms', claimed_at_ms,
    'last_heartbeat_at_ms', claimed_at_ms,
    'scheduler_epoch', reserved_epoch or ''
)
redis.call('EXPIRE', task_meta_key, meta_ttl)
redis.call('ZADD', running_zset_key, heartbeat_score, task_id)
redis.call('DEL', reservation_key)

return {"task_claimed", "running"}
