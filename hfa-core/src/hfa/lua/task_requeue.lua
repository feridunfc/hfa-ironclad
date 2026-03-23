
-- KEYS:
-- 1 = task_state_key
-- 2 = task_meta_key
-- 3 = tenant_ready_queue
-- 4 = task_running_zset
-- 5 = completion_stream
--
-- ARGV:
-- 1 = task_id
-- 2 = tenant_id
-- 3 = expected_state
-- 4 = now_ms
-- 5 = ready_score
-- 6 = max_retry
-- 7 = reason_code
-- 8 = stream_maxlen

local task_id = ARGV[1]
local tenant_id = ARGV[2]
local expected_state = ARGV[3]
local now_ms = ARGV[4]
local ready_score = ARGV[5]
local max_retry = tonumber(ARGV[6])
local reason_code = ARGV[7]
local stream_maxlen = tonumber(ARGV[8])

local state = redis.call("GET", KEYS[1])
if not state then
    return {"TASK_STATE_CONFLICT", "missing_state"}
end

if state ~= expected_state then
    if state == "ready" or state == "pending" then
        return {"TASK_ALREADY_REQUEUED", state}
    end
    if state == "done" or state == "failed" or state == "blocked_by_failure" then
        return {"TASK_TERMINAL", state}
    end
    return {"TASK_STATE_CONFLICT", state}
end

local retries = tonumber(redis.call("HGET", KEYS[2], "requeue_count") or "0")
retries = retries + 1

if retries > max_retry then
    redis.call("SET", KEYS[1], "failed")
    redis.call("HSET", KEYS[2],
        "failed_at_ms", now_ms,
        "failure_reason", "STALE_RETRY_EXHAUSTED",
        "requeue_count", tostring(retries),
        "last_requeue_reason", reason_code
    )
    redis.call("ZREM", KEYS[4], task_id)
    redis.call("XADD", KEYS[5], "MAXLEN", "~", stream_maxlen, "*",
        "event_type", "TaskRecoveryFailed",
        "task_id", task_id,
        "tenant_id", tenant_id,
        "reason_code", "STALE_RETRY_EXHAUSTED",
        "requeue_count", tostring(retries),
        "at_ms", now_ms
    )
    return {"TASK_RETRY_EXHAUSTED", tostring(retries)}
end

redis.call("SET", KEYS[1], "ready")
redis.call("HSET", KEYS[2],
    "requeue_count", tostring(retries),
    "last_requeue_reason", reason_code,
    "last_requeue_at_ms", now_ms,
    "heartbeat_owner", "",
    "heartbeat_at_ms", "0"
)
redis.call("ZREM", KEYS[4], task_id)
redis.call("ZADD", KEYS[3], "NX", ready_score, task_id)
redis.call("XADD", KEYS[5], "MAXLEN", "~", stream_maxlen, "*",
    "event_type", "TaskRequeued",
    "task_id", task_id,
    "tenant_id", tenant_id,
    "reason_code", reason_code,
    "requeue_count", tostring(retries),
    "at_ms", now_ms
)

return {"TASK_REQUEUED", tostring(retries)}
