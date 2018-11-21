fun = require 'fun'
log = require 'log'
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

--- @class ObservableTrigger
-- @description Observable for tarantool triggers.

local ObservableTrigger = setmetatable({}, rx.Observable)
ObservableTrigger.__index = ObservableTrigger
ObservableTrigger.__tostring = rx.util.constant('ObservableTrigger')

--- Creates a new ObservableTrigger.
-- @returns {ObservableTrigger}

function ObservableTrigger.create(trigger)
  assert(trigger)
  local self = {
    trigger = trigger,
    stopped = false,
    observers = {},
  }
  return setmetatable(self, ObservableTrigger)
end

--- Creates a new Observer and attaches it to the Observable
-- @arg {function|table} onNext|observer - A function called when the Observable produces a value or
--                                         an existing Observer to attach to the Observable.
-- @arg {function} onError - Called when the Observable terminates due to an error.
-- @arg {function} onCompleted - Called when the Observable completes normally.
function ObservableTrigger:subscribe(onNext, onError, onCompleted)

  assert(self.trigger)

  if not self.setopped and not self.handler then
    local function handler(...)
      local args = {...}
      for i = 1, #self.observers do
        xpcall(
          function() self.observers[i]:onNext(rx.util.unpack(args)) end,
          log.error
        )
      end
    end
    self.trigger(handler)
    self.handler = handler
    print('attaching handler')
  end

  local observer

  if rx.util.isa(onNext, rx.Observer) then
    observer = onNext
  else
    observer = rx.Observer.create(onNext, onError, onCompleted)
  end

  table.insert(self.observers, observer)

  return rx.Subscription.create(function()
    for i = 1, #self.observers do
      if self.observers[i] == observer then
        table.remove(self.observers, i)
        break
      end
    end
    if #self.observers == 0 and self.handler then
      self.trigger(nil, self.handler)
      self.handler = nil
      -- print('dettaching handler')
    end
  end)
end

--- Stops ObservableTrigger
function ObservableTrigger:stop()
    for i = 1, #self.observers do
      xpcall(
        function() self.observers[i]:onCompleted() end,
        log.error
      )
    end
    if self.handler then
      self.trigger(nil, self.handler)
      self.handler = nil
      -- print('dettaching handler')
    end
    self.stopped = true
end

--- Converts observable to table filled with first arguments of onNext calls.
-- @returns table
-- TODO remove unused
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
  ObservableTrigger = ObservableTrigger,
}
