local tap = require 'tap'
local tnt = require 't.tnt'

local fun = require 'fun'
local fiber = require 'fiber'
local rx = require 'rx'
local rxtnt = require 'rxtnt'

tnt.cfg{}

local test = tap.test("rxtnt")
test:plan(5)


--[
-- Observable to table
--]
--
local err
local ret

ret = rx.Observable.fromRange(5):toTable()
test:is_deeply(ret, {1, 2, 3, 4, 5}, "Observable to table")


err, ret = pcall(function()
  return rx.Observable.throw('Good error'):toTable()
end)
test:is(err, false, 'Observable to table error')
test:like(ret, 'Good error', 'Observable to table error message')


--[
-- FiberScheduler test
--]

local scheduler = rxtnt.FiberScheduler.create()

ret = rx.Observable.fromTable({'hello'})
  :delay(500, scheduler)
  :toTable()
test:is_deeply(ret, {'hello'}, "FiberScheduler delay")

local justA = rx.Observable.of('A'):delay(600, scheduler)
local justB = rx.Observable.of('B'):delay(500, scheduler)
ret = rx.Observable.amb(justA, justB):toTable()
test:is_deeply(ret, {'B'}, "FiberScheduler delay and amb")


tnt.finish()
test:check()
os.exit(0)
