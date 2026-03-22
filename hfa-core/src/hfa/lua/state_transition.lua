--[[
hfa-core/src/hfa/lua/state_transition.lua
IRONCLAD Sprint 22 — Atomic Compare-and-Set state transition

Provides true Redis-atomic CAS for run lifecycle state transitions.
Eliminates the GET→validate→SET race window in Python-level code.

Algorithm
---------
1. Read current state
2. If mode="initial" and key exists → reject (-1)
3. If mode="cas" and expected ≠ current → reject (0)
4. If current is terminal → reject (-3)
5. If transition is illegal → reject (-2)
6. Atomically SET the new state with TTL
7. Return [code, prior_state] tuple for observability

KEYS
----
KEYS[1]  run state STRING key    hfa:run:state:{run_id}

ARGV
----
ARGV[1]  run_id          (string)
ARGV[2]  to_state        (string)
ARGV[3]  expected_state  (string or "" for initial-write)
ARGV[4]  state_ttl       (integer, seconds)
ARGV[5]  mode            ("initial" | "cas")

Returns
-------
Table {code, prior_state}:
  code:
    1   = success
    0   = CAS miss
   -1   = initial-write blocked (key already exists)
   -2   = illegal transition
   -3   = terminal state blocked
  prior_state:
    previous value of the key, or "" if absent
--]]

local state_key = KEYS[1]
local run_id    = ARGV[1]
local to_state  = ARGV[2]
local expected  = ARGV[3]
local ttl       = tonumber(ARGV[4]) or 86400
local mode      = ARGV[5] or "cas"

-- Terminal states: no outgoing transitions allowed
local terminal = {done=true, failed=true, rejected=true, dead_lettered=true}

-- Valid transition map (mirrors Python VALID_TRANSITIONS)
local valid = {
    admitted     = {queued=true, scheduled=true, rejected=true, failed=true},
    queued       = {scheduled=true, failed=true, rejected=true},
    scheduled    = {running=true, failed=true},
    running      = {done=true, failed=true, dead_lettered=true, rescheduled=true},
    rescheduled  = {admitted=true, failed=true},
}

-- Read current state
local current = redis.call('GET', state_key)
if current == false then current = nil end
local prior = current or ""

-- Mode: initial-write (only allowed if key absent)
if mode == "initial" then
    if current ~= nil then
        return {-1, prior}  -- key exists, initial-write blocked
    end
    redis.call('SET', state_key, to_state, 'EX', ttl)
    return {1, prior}
end

-- Mode: CAS (expected must match current)
if expected ~= nil and expected ~= "" then
    if current ~= expected then
        return {0, prior}  -- CAS miss
    end
end

-- Terminal guard
if current ~= nil and terminal[current] then
    return {-3, prior}
end

-- Transition validity
if current ~= nil then
    local allowed = valid[current]
    if allowed == nil then
        return {-2, prior}  -- unknown from_state, fail-closed
    end
    if not allowed[to_state] then
        return {-2, prior}  -- illegal transition
    end
end

-- Commit
redis.call('SET', state_key, to_state, 'EX', ttl)
return {1, prior}
