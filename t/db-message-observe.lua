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
test:plan(11)

local success, ret

local events = {}

db.message.observe(source):subscribe(function(evt)
  table.insert(events, evt)
end)

test:is(#events, 0, 'Events is empty')

local node = db.node.add(record('node').from_map({
    name = 'Node',
    enabled = false,
    priority = 2,
    start_offset = 0,
    history_size = 100,
    tmp_session_id = box.session.id()
  }))

local message = db.message.add(
  record.Message.from_map({
    node_id = node.id,
    offset = clock.time(),
    content = 'hello'
  })
)

test:is(#events, 1, '1 event')

db.message.remove(node.id, 1)

test:is(#events, 2, '2 event')

for _, n, topic in fun.enumerate({
  'message_added',
  'message_removed'
}) do
  test:is(events[n].topic, topic,
    string.format('Event %s topic is ok', topic))
  test:is(events[n].node_id, node.id,
    string.format('Event %s node id is ok', topic))
  test:is(events[n].message_id, message.id,
    string.format('Event %s message id is ok', topic))
  test:is(events[n].offset, message.offset,
    string.format('Event %s message offset is ok', topic))
end

app.destroy()
tnt.finish()
test:check()
os.exit()

