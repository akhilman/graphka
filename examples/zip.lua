local clock = require 'clock'
local fiber = require 'fiber'
local fun = require 'fun'
local json = require 'json'
local log = require 'log'
local msgpack = require 'msgpack'
local net_box = require 'net.box'

local CLIENT_NAME = 'zip'
local URL = os.getenv('GRAPHKA_URL')
URL = URL or 'localhost:3311'

local NULL = msgpack.NULL

-- Connect to server
log.info(CLIENT_NAME .. ': Connecting to ' .. URL)
local conn = net_box.connect(URL)
local ok = conn:wait_connected()
assert(ok, 'Can not connect to ' .. URL)
local ok, result = conn:call('app.rename_session', {CLIENT_NAME}, {})
assert(ok, result)
local ok, session = conn:call('app.current_session', {}, {})
assert(ok, session)
CLIENT_NAME = CLIENT_NAME .. '#' .. session.id

-- Produce messages
while true do

  local name = 'example_zip'

  -- Get task
  log.info(string.format('%s: Getting task for node %s', CLIENT_NAME, name))
  local ok, task = conn:call('app.take_task', {name})
  assert(ok, task)

  if task ~= NULL then
    local content = {}
    -- TODO add node to task itself
    local ok, node = conn:call('app.get_node', {task.node})
    assert(ok, node)
    if task.last_message ~= NULL then
      content.n = task.last_message.content.n + 1
    else
      content.n = 1
    end

    fiber.sleep(0.3)  -- simulate hard work

    -- Create queue for input message
    content.inputs = {}
    for _, input in ipairs(node.inputs) do
      content.inputs[input] = {}
    end

    -- Process input messages
    local offset = 0
    for _, message in fun.iter(task.input_messages)
        :filter(function(msg) return msg.offset > task.offset end) do

      -- Add input message to queue
      table.insert(content.inputs[message.node], message.content.n)
      offset = math.max(offset, message.offset)

      -- Process messages from queue when all inputs have messages
      if fun.iter(node.inputs)
          :all(function(n) return #content.inputs[n] > 0 end) then

        log.info(string.format(
            '%s: Adding message #%d with inputs %s to %s with offset %f',
            CLIENT_NAME, content.n, json.encode(content.inputs), name, offset
          ))
        local ok, result = conn:call(
          'app.add_message', {task.id, offset, content})
        assert(ok, result)

        -- Clear queue
        for _, input in ipairs(node.inputs) do
          content.inputs[input] = {}
        end

      end
    end

    -- Release task
    local ok, result = conn:call('app.release_task', {task.id})
    assert(ok, result)
  end
end