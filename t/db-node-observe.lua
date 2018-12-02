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

local test = tap.test("db.node: create remove")
test:plan(26)

local success, ret
local nodes = {}

local events = {}

db.node.observe(source):subscribe(function(evt)
  table.insert(events, evt)
end)

test:is(#events, 0, 'Events is empty')

for _, name in ipairs({'A', 'B'}) do
  nodes[name] = db.node.add(record('node').from_map({
    name = name,
    enabled = false,
    priority = 2,
    start_offset = 0,
    history_size = 100,
    tmp_session_id = box.session.id()
  }))
end

test:is(#events, 2, '2 event')

db.node.alter(nodes.A.id, {enabled = true})
test:is(#events, 3, '3 event')

db.node.alter(nodes.A.id, {enabled = false})
test:is(#events, 4, '4 event')

db.node.alter(nodes.A.id, {priority = 10})
test:is(#events, 5, '5 event')

db.node.remove(nodes.A.id)
test:is(#events, 6, '6 event')


for _, n, topic in fun.enumerate({
  'node_added',
  'node_added',
  'node_enabled',
  'node_disabled',
  'node_altered',
  'node_removed'
}) do
  test:is(events[n].topic, topic,
    string.format('Event %s topic', topic))
  if n == 2 then
    test:is(events[n].node_id, nodes.B.id,
      string.format('Event %s node_id for node B', topic))
  else
    test:is(events[n].node_id, nodes.A.id,
      string.format('Event %s node_id for node A', topic))
  end
end

-- Wire

while #events > 0 do table.remove(events) end

db.node.connect(nodes.A.id, nodes.B.id, false, false)
test:is(#events, 1, '1 wire event')

db.node.disconnect(nodes.A.id, nodes.B.id)
test:is(#events, 2, '2 wire event')

for _, n, topic in fun.enumerate({
  'node_connected',
  'node_disconnected'
}) do
  test:is(events[n].topic, topic,
    string.format('Event %s topic', topic))
  test:is(events[n].input_id, nodes.A.id,
    string.format('Event %s input_id', topic))
  test:is(events[n].output_id, nodes.B.id,
    string.format('Event %s output_id', topic))
end

app.destroy()
tnt.finish()
test:check()
os.exit()

