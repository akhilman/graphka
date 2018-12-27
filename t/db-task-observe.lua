local app = require 't.app'
local clock = require 'clock'
local db = require 'db'
local fiber = require 'fiber'
local record = require 'record'
local rx = require 'rx'
local tap = require 'tap'
local tnt = require 't.tnt'

tnt.cfg{}
app.init{}

local test = tap.test("db.task: Observe")
test:plan(17)

local success, ret
local task
local node

local events = {}

db.task.observe(source):subscribe(function(evt)
  table.insert(events, evt)
end)

test:is(#events, 0, 'Events is empty')

--[[
-- Task
--]]

-- Add node

node = db.node.add(record.Node.from_map({
    name = 'Node',
    enabled = false,
    priority = 0,
    start_offset = 123,
    history_size = -1,
  }))

-- Add task

task = db.task.add(record.Task.from_map({
    node_id = node.id,
    session_id = box.session.id(),
    message_id = 123,
    offset = 321,
    expires = clock.time() + 60,
  }))
test:is(#events, 1, '1 event')

-- Alter task

db.task.set_expires(task.id, clock.time() + 120)
test:is(#events, 2, '2 events')

-- Remove task

db.task.remove(task.id)
test:is(#events, 3, '3 events')

for _, n, topic in fun.enumerate({
  'task_added',
  'task_altered',
  'task_removed'
}) do
  test:is(events[n].topic, topic,
    string.format('Event %s topic', topic))
  test:is(events[n].task_id, task.id,
    string.format('Event %s task_id', topic))
  test:is(events[n].node_id, node.id,
    string.format('Event %s node_id', topic))
end

events = {}

--[[
-- Node state
--]]

-- Set state

db.task.set_node_state(node.id, { atime = clock.time() })
test:is(#events, 0, 'Events is empty')

db.task.set_node_state(node.id, { atime = clock.time(), outdated = true })
test:is(#events, 1, '1 event')

for _, n, topic in fun.enumerate({
  'node_outdated'
}) do
  test:is(events[n].topic, topic,
    string.format('Event %s topic', topic))
  test:is(events[n].node_id, node.id,
    string.format('Event %s node_id', topic))
end

app.destroy()
tnt.finish()
test:check()
os.exit()

