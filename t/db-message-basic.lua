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

local test = tap.test("db.message: Basic")
test:plan(25)

local success, result
local node
local start_offset = clock.time()

-- Space is ready

ret = db.message.is_ready()
test:ok(ret, 'Message space is ready')

-- Add message node not exist

success, result = pcall(
  db.message.add,
  record('message').from_map({
    node_id = 255,
    offset = start_offset,
    content = 'hello'
  })
)
test:ok(not success, 'Can not add message if node not exist')

-- Add nodes

local nodes = fun.range(4)
  :map(tostring)
  :map(util.partial(fun.operator.concat, 'Node'))
  :map(function(name)
      return db.node.add(record('node').from_map({
        name = name,
        enabled = true,
        priority = 0,
        start_offset = 0,
        history_size = -1,
      }))
    end)
  :totable()

-- Add messages

fun.iter(nodes)
  :take_n(3)
  :each(function(node)
    local offset = start_offset
    fun.range(10)
      :each(function(n)
        offset = offset + math.random() * 10
        db.message.add(record('message').from_map({
          node_id = node.id,
          offset = offset,
          content = string.format("Node: %s, message %d", node.name, n)
        }))
      end)
  end)

-- Add message with offset smaller then last one

success, result = pcall(
  db.message.add,
  record('message').from_map({
    node_id = nodes[1].id,
    offset = start_offset,
    content = 'hello'
  })
)
test:ok(not success, 'Can not add message with offset less then last one')

-- Check summary

result = db.message.summary(nodes[1].id)
test:is(result.count, 10, 'Node1 message count is 10')
test:isnumber(result.last_offset, 'Node1 last offset is number')

result = db.message.summary(nodes[4].id)
test:is(result.count, 0, 'Node3 message count is 0')
test:is(result.last_offset, NULL, 'Node1 last offset is NULL')

local last_offset = fun.iter(nodes)
  :map(util.itemgetter('id'))
  :map(db.message.summary)
  :map(util.itemgetter('last_offset'))
  :filter(function(v) return type(v) == 'number' end)
  :max()
test:ok(last_offset > start_offset, 'Last offset greater then start offset')

-- Get middle index

result = db.message.iter_by_id(nodes[1].id):nth(5)
local middle_offset = result.offset

test:ok(start_offset < middle_offset,
        'Middle message\'s offset greater then start offset')

-- Get all messages without "get_prev"

local messages = db.message.iter(
  fun.iter(nodes):map(util.itemgetter('id')):totable(),
  middle_offset
):totable()

result = fun.iter(messages)
  :map(util.itemgetter('offset'))
  :all(util.partial(fun.operator.le, middle_offset))
test:ok(result, 'All message\'s offsets greater or equal requested offset')
test:is(messages[1].offset, middle_offset,
        'First message\'s offset is requested offset')
test:is(messages[#messages].offset, last_offset,
        'Check lest message\'s offset')

result = fun.zip(
    fun.iter(messages):map(util.itemgetter('offset')),
    fun.iter(messages):map(util.itemgetter('offset')):tail()
  ):all(fun.operator.le)
test:ok(result, 'All message\'s sorted by offset')

-- Get all messages with "get_prev"

local messages = db.message.iter(
  fun.iter(nodes):map(util.itemgetter('id')):totable(),
  middle_offset,
  true -- get_prev
):totable()

result = fun.iter(messages)
  :map(util.itemgetter('offset'))
  :any(util.partial(fun.operator.gt, middle_offset))
test:ok(result, 'Some message\'s offsets less then requested offset')
test:ok(messages[1].offset < middle_offset,
        'First message\'s offset less then requested offset')
test:is(messages[#messages].offset, last_offset,
        'Check lest message\'s offset')

result = fun.zip(
    fun.iter(messages):map(util.itemgetter('offset')),
    fun.iter(messages):map(util.itemgetter('offset')):tail()
  ):all(fun.operator.le)
test:ok(result, 'All message\'s sorted by offset')

-- Removing

local node_id = nodes[1].id
local pre_count = db.message.summary(node_id).count
local n_removed = db.message.remove(node_id, math.floor(pre_count / 2))
local post_count = db.message.summary(node_id).count
test:is(n_removed + post_count, pre_count,
        string.format('%d messages removed, %d messages kept',
                      n_removed, post_count))
test:is(db.message.iter_by_id(node_id):length(), post_count,
        string.format('Node1 contains %d messages', post_count))

local n_removed = db.message.remove(node_id, math.ceil(pre_count / 2))
local summary = db.message.summary(node_id)
test:is(summary.count, 0, 'Count in summary is 0')
test:is(summary.first_id, NULL, 'First id is NULL')
test:is(summary.first_offset, NULL, 'First offset is NULL')
test:is(summary.last_id, NULL, 'Last id is NULL')
test:is(summary.last_offset, NULL, 'Last offset is NULL')
test:is(db.message.iter_by_id(node_id):length(), 0, 'Node1 have no messages')

app.destroy()
tnt.finish()
test:check()
os.exit()

