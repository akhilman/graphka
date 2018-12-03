local app = require 't.app'
local clock = require 'clock'
local db = require 'db'
local fiber = require 'fiber'
local rx = require 'rx'
local tap = require 'tap'
local tnt = require 't.tnt'

tnt.cfg{}
app.init{}

local test = tap.test("Message serivce")
test:plan(10)

local nodes
local success, result

-- Add nodes

local _, node_a = app.add_node('A')
local _, node_b = app.add_node('B', {history_size=3})

-- Add messages

local start_offset = clock.time()
fun.iter({node_a, node_b})
  :each(function(node)
    local offset = start_offset
    fun.range(10)
      :each(function(n)
        offset = offset + math.random() * 10
        app.add_message(
          node.name,
          offset,
          string.format("Node: %s, message %d", node.name, n)
        )
      end)
  end)

-- check summary

local _, summary = app.message_summary()
test:is(summary.A.count, 10, 'Node A have 10 messages')
test:is(summary.B.count, 10, 'Node A have 10 messages')

-- check get by id

success, result = app.get_messages_by_id(
  'A', summary.A.first_id + 1, summary.A.last_id - 1)
test:is(#result, 8, 'Got 8 messages by id')

success, result = app.get_messages_by_id(
  'A', summary.A.first_id + 1, summary.A.last_id - 1, 3)
test:is(#result, 3, 'Got 3 messages by id with limit')

-- get by offset

local first_offset = math.max(summary.A.first_offset, summary.B.first_offset)
local last_offset = math.max(summary.A.last_offset, summary.B.last_offset)
local middle_offset = (first_offset + last_offset) / 2

success, result = app.get_messages({'A', 'B'}, middle_offset, 20, false)
test:ok(result[1].offset >= middle_offset,
        'First message\'s offset is requested offset')
test:is(result[#result].offset, last_offset,
        'Check lest message\'s offset')

success, result = app.get_messages({'A', 'B'}, middle_offset, 20, true)
test:ok(result[1].offset < middle_offset,
        'First message\'s offset less then requested offset')
test:is(result[#result].offset, last_offset,
        'Check lest message\'s offset')

-- Purge

app.remove_node('A')
app.purge()
fiber.sleep(1)

local msg_count = box.space.message:count()
local summary_count = box.space.message_summary:count()
test:is(msg_count, 3, '3 messages left')
test:is(summary_count, 1, '1 summary left')

app.destroy()
tnt.finish()
test:check()
os.exit()
