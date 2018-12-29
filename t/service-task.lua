local app = require 't.app'
local clock = require 'clock'
local db = require 'db'
local msgpack = require 'msgpack'
local record = require 'record'
local rx = require 'rx'
local tap = require 'tap'
local tnt = require 't.tnt'
local util = require 'util'

local NULL = msgpack.NULL

local TEST_NAME = 'Task service'

-- Add messages
local last_offset = clock.time()
local function add_message(node_name)
  local offset = last_offset + math.random() * 10
  local ok, task = app.take_last(node_name)
  assert(ok)
  assert(task)
  local ok, message = app.add_message(
      task.id,
      offset,
      task.node.name .. ' message at ' .. offset
    )
  assert(ok)
  last_offset = offset
  app.release_task(task.id)
  return message
end

-- Tests

local T = {}

function T.test_no_tasks(test)
  test:plan(2)
  local success, result = app.take_task('*', 100, 0.2)
  test:ok(success, 'take_task called successfully')
  test:isnil(result, 'Nodes have no tasks')
end

function T.test_not_enabled(test)
  test:plan(2)
  -- Disable third node
  app.disable_node('Sink')
  -- Add messages to first two nodes
  fun.iter({'SourceA', 'SourceB'})
    :each(add_message)
  -- Take task
  local success, result = app.take_task('*', 100, 0.2)
  test:ok(success, 'take_task called successfully')
  test:isnil(result, 'Nodes have no tasks')
end

function T.test_trigger_by_enabeling_node(test)
  test:plan(11)
  -- Disable third node
  app.disable_node('Sink')
  -- Add messages to first two nodes
  fun.iter({'SourceA', 'SourceB'})
    :each(add_message)
  -- Enable third node
  local start_time = clock.time()
  fiber.create(function()
    db.session.add(record.Session.create(
        box.session.id(), 'fiber', '', clock.time()))
    fiber.sleep(0.3)
    app.enable_node('Sink')
    db.session.remove(box.session.id())
  end)
  -- Take task
  local success, task = app.take_task('*', 100, 1)
  test:ok(clock.time() - start_time > 0.3, 'Taking took more then 0.3 second')
  test:ok(clock.time() - start_time < 1, 'Taking took less then 1 second')
  test:ok(success, 'take_task called successfully')
  test:istable(task, 'Enabling node produces a task')
  test:is(task.node.name, 'Sink', 'Task\'s node')
  test:is(task.offset, 0, 'Task\'s offset')
  test:is(task.last_message, NULL, 'Task\'s last_message')
  test:is(#task.input_messages, 2, 'Task\'s input_messages has two messages')
  test:is(task.input_messages[1].node_name, 'SourceA',
          'Task\'s first input_messages is by SourceA')
  test:is(task.input_messages[2].node_name, 'SourceB',
          'Task\'s second input_messages is by SourceB')
  -- Release task
  local success, result = app.release_task(task.id)
  test:ok(success, 'release_task called successfully')
end

function T.test_trigger_by_connecting_node(test)
  test:plan(10)
  -- Disconnect nodes
  app.disconnect_nodes('SourceA', 'Sink')
  app.disconnect_nodes('SourceB', 'Sink')
  -- Enabling sink node
  app.enable_node('Sink')
  -- Add messages to first two nodes
  fun.iter({'SourceA', 'SourceB'})
    :each(add_message)
  -- Connect
  local start_time = clock.time()
  fiber.create(function()
    db.session.add(record.Session.create(
        box.session.id(), 'fiber', '', clock.time()))
    fiber.sleep(0.3)
    app.connect_nodes('SourceA', 'Sink')
    db.session.remove(box.session.id())
  end)
  -- Take task
  local success, task = app.take_task('*', 100, 1)
  test:ok(clock.time() - start_time > 0.3, 'Taking took more then 0.3 second')
  test:ok(clock.time() - start_time < 1, 'Taking took less then 1 second')
  test:ok(success, 'take_task called successfully')
  test:istable(task, 'Connecting node produces a task')
  test:is(task.node.name, 'Sink', 'Task\'s node')
  test:is(task.offset, 0, 'Task\'s offset')
  test:is(task.last_message, NULL, 'Task\'s last_message')
  test:is(#task.input_messages, 1, 'Task\'s input_messages has two messages')
  test:is(task.input_messages[1].node_name, 'SourceA',
          'Task\'s first input_messages is by SourceA')
  -- Release task
  local success, result = app.release_task(task.id)
  test:ok(success, 'release_task called successfully')
end

function T.test_trigger_by_adding_message(test)
  test:plan(10)
  -- Enabling sink node
  app.enable_node('Sink')
  -- Add message to SourceA
  local start_time = clock.time()
  fiber.create(function()
    db.session.add(record.Session.create(
        box.session.id(), 'fiber', '', clock.time()))
    fiber.sleep(0.3)
    add_message('SourceA')
    db.session.remove(box.session.id())
  end)
  -- Take task
  local success, task = app.take_task('*', 100, 1)
  test:ok(clock.time() - start_time > 0.3, 'Taking took more then 0.3 second')
  test:ok(clock.time() - start_time < 1, 'Taking took less then 1 second')
  test:ok(success, 'take_task called successfully')
  test:istable(task, 'Adding message produces a task')
  test:is(task.node.name, 'Sink', 'Task\'s node')
  test:is(task.offset, 0, 'Task\'s offset')
  test:is(task.last_message, NULL, 'Task\'s last_message')
  test:is(#task.input_messages, 1, 'Task\'s input_messages has two messages')
  test:is(task.input_messages[1].node_name, 'SourceA',
          'Task\'s first input_messages is by SourceA')
  -- Release task
  local success, result = app.release_task(task.id)
  test:ok(success, 'release_task called successfully')
end

function T.test_trigger_by_task_release(test)
  test:plan(10)
  -- Take SourceA
  local success, task = app.take_last('SourceA')
  test:ok(success, 'take_last called successfully')
  test:istable(task, 'Return value is table')
  -- Take it again
  local success, result = app.take_last('SourceA', 100, 0.3)
  test:ok(success, 'take_last called successfully')
  test:isnil(result, 'Return value is nil')
  local start_time = clock.time()
  fiber.create(function()
    db.session.add(record.Session.create(
        box.session.id(), 'fiber', '', clock.time()))
    fiber.sleep(0.3)
    app.release_task(task.id)
    db.session.remove(box.session.id())
  end)
  -- Take task
  local success, result = app.take_last('SourceA', 100, 1)
  test:ok(clock.time() - start_time > 0.3, 'Taking took more then 0.3 second')
  test:ok(clock.time() - start_time < 1, 'Taking took less then 1 second')
  test:ok(success, 'take_result called successfully')
  test:istable(result, 'Return value is table')
  test:is(result.node.name, 'SourceA', 'Task\'s node')
  -- Release result
  local success, result = app.release_task(result.id)
  test:ok(success, 'release_result called successfully')
end

function T.test_remark_outdated(test)
  test:plan(10)
  -- Enabling sink node
  app.enable_node('Sink')
  -- Add message to SourceA
  local source_msg = add_message('SourceA')

  -- Take task
  local success, task = app.take_task('*', 100, 0.1)
  test:ok(success, 'take_task called successfully')
  test:istable(task, 'Return value is table')
  test:is(task.node.name, 'Sink', 'Task\'s node')
  -- Add message to Sink
  app.add_message(
      task.id,
      source_msg.offset - 1,
      task.node.name .. ' message at ' .. source_msg.offset - 1
    )
  -- Release task
  local success, result = app.release_task(task.id)
  test:ok(success, 'release_task called successfully')

  -- Take task
  local success, task = app.take_task('*', 100, 0.1)
  test:ok(success, 'take_task called successfully')
  test:istable(task, 'Return value is table')
  test:is(task.node.name, 'Sink', 'Task\'s node')
  -- Release task
  local success, result = app.release_task(task.id)
  test:ok(success, 'release_task called successfully')

  -- Take task
  local success, task = app.take_task('*', 100, 0.1)
  test:ok(success, 'take_task called successfully')
  test:isnil(task, 'Return value is nil')
end

function T.test_rotation(test)
  test:plan(15)
  local names = {}

  local success, task = app.take_last('*')
  test:ok(success, 'take_last called successfully')
  test:istable(task, 'Return value is table')
  names[1] = task.node.name
  local success, result = app.release_task(task.id)
  test:ok(success, 'release_task called successfully')

  local success, task = app.take_last('*')
  test:ok(success, 'take_last called successfully')
  test:istable(task, 'Return value is table')
  test:isnt(task.node.name, names[1], 'Task\'s node is changed')
  names[2] = task.node.name
  local success, result = app.release_task(task.id)
  test:ok(success, 'release_task called successfully')

  local success, task = app.take_last('*')
  test:ok(success, 'take_last called successfully')
  test:istable(task, 'Return value is table')
  test:is(task.node.name, names[1], 'Task\'s node is changed again')
  local success, result = app.release_task(task.id)
  test:ok(success, 'release_task called successfully')

  local success, task = app.take_last('*')
  test:ok(success, 'take_last called successfully')
  test:istable(task, 'Return value is table')
  test:is(task.node.name, names[2], 'Task\'s node is changed again')
  local success, result = app.release_task(task.id)
  test:ok(success, 'release_task called successfully')
end

-- Run tests

local function runtest(name, func)
  print(name)
  local test = tap.test(string.format('%s: %s', TEST_NAME, name))
  -- Add nodes
  fun.iter({'SourceA', 'SourceB', 'Sink'})
    :each(app.add_node)
  -- Enable source nodes
  fun.iter({'SourceA', 'SourceB'})
    :each(util.partial(app.enable_node))
  -- Connect first two nodes to third one
  fun.iter({'SourceA', 'SourceB'})
    :each(util.revpartial(app.connect_nodes, 'Sink'))
  -- Run test
  func(test)
  -- Remove nodes
  fun.iter(rx.util.pack(app.list_nodes())[2])
    :map(util.itemgetter('name'))
    :each(app.remove_node)
  test:check()
end


tnt.cfg{
    -- log_level = 7,
}
app.init{}
fun.iter(T)
  :filter(function(name, func) return string.match(name, '^test_.*$') end)
  :each(runtest)
app.destroy()
tnt.finish()
os.exit()
