local app = require 't.app'
local clock = require 'clock'
local db = require 'db'
local fiber = require 'fiber'
local fun = require 'fun'
local msgpack = require 'msgpack'
local record = require 'record'
local rx = require 'rx'
local tap = require 'tap'
local tnt = require 't.tnt'
local util = require 'util'

local NULL = msgpack.NULL

tnt.cfg{}
app.init{}

local test = tap.test("db.task: Basic")
test:plan(25)

local success, result
local nodes

-- Space is ready

ret = db.message.is_ready()
test:ok(ret, 'Task space is ready')

-- Add nodes

local nodes = fun.range(2)
  :map(tostring)
  :map(util.partial(fun.operator.concat, 'Node'))
  :map(function(name)
      return db.node.add(record('node').from_map({
        name = name,
        enabled = true,
        priority = 0,
        start_offset = 123,
        history_size = -1,
      }))
    end)
  :totable()

-- iter_node_states should yield nothing for now
result = db.task.iter_node_states():totable()
print(require'yaml'.encode(result))
test:is_deeply(result, {}, 'iter_node_states yields nothing')

-- Connect nodes
db.node.connect(nodes[1].id, nodes[2].id)

-- Create task for first node

local task = db.task.add(record.Task.from_map({
  node_id = nodes[1].id,
  session_id = box.session.id(),
  message_id = 123,
  offset = 321,
  expires = clock.time() + 60,
}))

-- Try to create task for same node

local success, result = pcall(
  db.task.add,
  record.Task.from_map({
    node_id = nodes[1].id,
    session_id = box.session.id(),
    message_id = NULL,
    offset = nodes[1].start_offset,
    expires = clock.time() + 60,
  })
)
test:ok(not success, 'Can not create Task for same node second time')

-- Test exist_for_node(node_id)

result = db.task.exist_for_node(nodes[1].id)
test:ok(result, 'Task exist for First node')
result = db.task.exist_for_node(nodes[2].id)
test:ok(not result, 'Task not exist for Second node')

-- Test get(id)

result = db.task.get(task.id)
test:is_deeply(result, task, 'Task returned by get()')
result = db.task.get(255)
test:isnil(result, 'nil returned by get() with wrong id')

-- Test get_by_node(node_id)

result = db.task.get_by_node(nodes[1].id)
test:is_deeply(result, task, 'Task returned by get_by_node() for first node')
result = db.task.get_by_node(nodes[2].id)
test:isnil(result, 'nil returned by get_by_node() for second node')

-- Create task for second node

local second_task = db.task.add(record.Task.from_map({
  node_id = nodes[2].id,
  session_id = box.session.id(),
  message_id = NULL,
  offset = nodes[1].start_offset,
  expires = clock.time() + 60,
}))

-- Test iter()

result = db.task.iter():totable()
test:is_deeply(result, {task, second_task}, 'iter() yields both tasks')

-- Test iter_by_session_id(session_id)

result = db.task.iter_by_session_id(box.session.id()):totable()
test:is_deeply(result, {task, second_task},
               'iter_by_session_id() yields both tasks for right session')
result = db.task.iter_by_session_id(255):totable()
test:is_deeply(result, {},
               'iter_by_session_id() yields nothing for wrong session')

-- Test iter_expired() and set_expires(task_id, timestamp)

result = db.task.iter_expired(clock.time()):totable()
test:is_deeply(result, {}, 'No tasks expired')

second_task.expires = clock.time() - 1
result = db.task.set_expires(second_task.id, second_task.expires)
test:is_deeply(result, second_task, 'set_expires returns updated task')

result = db.task.get(second_task.id)
test:is_deeply(result, second_task, 'Task is updated in db')

result = db.task.iter_expired(clock.time()):totable()
test:is_deeply(result, {second_task}, 'iter_expired() returns expired task')

-- Test remove(task_id)

result = db.task.remove(second_task.id)
test:is_deeply(result, second_task, 'remove() returns task itself.')

ok, result = pcall(db.task.remove, second_task.id)
test:ok(not ok, 'remove() raises error if no such task.')

-- Test get_node_state(node_id)

local default_state = {
  node_id = nodes[1].id,
  outdated = false,
  atime = 0
}
result = db.task.get_node_state(nodes[1].id)
test:is_deeply(result, default_state, "Default node state")

-- Test set_node_state()
local update = {
  outdated = true,
  atime = clock.time()
}
result = db.task.set_node_state(nodes[1].id, update)
test:is_deeply(result, util.merge_tables(default_state, update),
               'set_node_state returns updated state')
result = db.task.get_node_state(nodes[1].id)
test:is_deeply(result, util.merge_tables(default_state, update),
               'State is updated in database')

-- Test iter_node_states()
result = db.task.iter_node_states():totable()
test:is_deeply(
    fun.iter(result):filter(util.itemeq('node_id', nodes[1].id)):nth(1),
    util.merge_tables(default_state, update),
    'iter_node_states yields first node\'s state'
  )

-- Test clear_node_state()
result = db.task.clear_node_state(nodes[1].id)
test:is_deeply(result, util.merge_tables(default_state, update),
               'First time clear_node_state returns updated state')
result = db.task.clear_node_state(nodes[1].id)
test:isnil(result, 'Second time clear_node_state returns nil')

result = db.task.get_node_state(nodes[1].id)
test:is_deeply(result, default_state, "Cleared state is default")

app.destroy()
tnt.finish()
test:check()
os.exit()

