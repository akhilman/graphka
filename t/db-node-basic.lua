local db = require 'db'
local fiber = require 'fiber'
local record = require 'record'
local rx = require 'rx'
local tap = require 'tap'
local tnt = require 't.tnt'

tnt.cfg{}

local test = tap.test("db.node: Basic")
test:plan(19)

local success, ret
local node

-- Space is ready

ret = db.session.is_ready()
test:ok(ret, 'Session space is ready')

-- Empty list

ret = db.node.iter():totable()
test:is(#ret, 0, 'Empty node list')

-- Add node

node = db.node.add(record('node').from_map({
  name = 'test_node',
  enabled = false,
  priority = 2,
  start_offset = 0,
  history_size = 100,
  tmp_session_id = box.session.id()
}))
test:isnumber(node.id, 'New node id')
test:is(node.name, 'test_node', 'New node name')
test:is(node.enabled, false, 'New node enabled')
test:is(node.priority, 2, 'New node priority')
test:is(node.history_size, 100, 'New node history_size')
test:is(node.tmp_session_id, box.session.id(), 'New node tmp_session_id')

-- List nodes

ret = db.node.iter():totable()
test:is(#ret, 1, 'Listed one node')
test:is_deeply(ret[1], node, 'Node in list equals new node')

-- Get node

ret = db.node.get(node.id)
test:is_deeply(ret, node, 'Get node')

-- Get node by name

ret = db.node.get_by_name(node.name)
test:is_deeply(ret, node, 'Get node by name')

-- Alter node

node = db.node.alter(node.id, {priority=0, enabled=true})
test:is(node.enabled, true, 'Altered node enabled')
test:is(node.priority, 0, 'Altered node priority')

ret = db.node.iter():totable()
test:is_deeply(ret[1], node, 'Node in list equals altered node')

-- Remove node

ret = db.node.remove(node.id)
test:is_deeply(ret[1], node, 'Get node')

ret = db.node.iter():totable()
test:is(#ret, 0, 'Empty node list')

-- Remove by session id

node = db.node.add(record('node').from_map({
  name = 'test_node',
  enabled = false,
  priority = 2,
  start_offset = 0,
  history_size = 100,
  tmp_session_id = box.session.id()
}))

ret = db.node.remove_tmp(box.session.id())
test:is(#ret, 1, 'Removed tmp count')
test:is_deeply(ret[1], node, 'Removed tmp node')

tnt.finish()
test:check()
os.exit()

