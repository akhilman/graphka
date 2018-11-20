fun = require('fun')
fiber = require 'fiber'
rx = require 'rx'

--- @class FiberScheduler
-- @description A scheduler that uses tarantool's fiber to schedule events.
local FiberScheduler = {}
FiberScheduler.__index = FiberScheduler
FiberScheduler.__tostring = rx.util.constant('FiberScheduler')

--- Creates a new FiberScheduler.
-- @returns {FiberScheduler}
function FiberScheduler.create()
  return setmetatable({}, FiberScheduler)
end

--- Schedules an action to run at a future point in time.
-- @arg {function} action - The action to run.
-- @arg {number=0} delay - The delay, in milliseconds.
-- @returns {Subscription}
function FiberScheduler:schedule(action, delay, ...)

  local args = rx.util.pack(...)

  local task = fiber.create(function()
    fiber.sleep(delay / 1000)
    return action(rx.util.unpack(args))
  end)

  return rx.Subscription.create(function()
    task:cancel()
  end)

end

--- Converts observable to table filled with first arguments of onNext calls.
-- @returns table
function rx.Observable:toTable()

  local cond = fiber.cond()
  local stopped = false
  local err = nil
  local ret = {}

  local function onNext(val)
    -- print('onNext', val)
    table.insert(ret, val)
  end

  local function onError(msg)
    -- print('onError', msg)
    err = rx.util.pack(msg)
    stopped = true
    cond:signal()
  end

  local function onCompleted()
    -- print('onCompleted')
    stopped = true
    cond:signal()
  end

  self:subscribe(onNext, onError, onCompleted)

  if not stopped then
    cond:wait()
  end
  if err then
    error(rx.util.unpack(err), 0)
  end

  return ret

end

--- Module
return {
  FiberScheduler = FiberScheduler,
}
