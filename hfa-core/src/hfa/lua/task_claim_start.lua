
-- task_claim_start.lua
-- Purpose: Atomic ownership claim and transition to 'running' state.
--
-- KEYS
--   1 task_state_key      (hfa:task:<task_id>:state)
--   2 task_meta_key       (hfa:task:<task_id>:meta)
--   3 running_zset_key    (hfa:dag:tenant:<tenant_id>:running)
--
-- ARGV
--   1 task_id
--   2 worker_instance_id
--   3 claimed_at_ms
--   4 state_ttl
--   5 meta_ttl
--   6 heartbeat_score

local task_state_key = KEYS[1]
local task_meta_key = KEYS[2]
local running_zset_key = KEYS[3]

local task_id = ARGV[1]
local worker_instance_id = ARGV[2]
local claimed_at_ms = ARGV[3]
local state_ttl = tonumber(ARGV[4])
local meta_ttl = tonumber(ARGV[5])
local heartbeat_score = tonumber(ARGV[6])

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

redis.call('SET', task_state_key, 'running', 'EX', state_ttl)

redis.call('HSET', task_meta_key,
    'worker_instance_id', worker_instance_id,
    'claimed_at_ms', claimed_at_ms,
    'last_heartbeat_at_ms', claimed_at_ms
)
redis.call('EXPIRE', task_meta_key, meta_ttl)

redis.call('ZADD', running_zset_key, heartbeat_score, task_id)

return {"task_claimed", "running"}
