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
  log.info(string.format('%f %s: Getting task for node %s',
                         clock.time(), CLIENT_NAME, name))
  local ok, task = conn:call('app.take_task', {name})
  assert(ok, task)

  if task ~= NULL then
    local content = {}
    -- TODO add node to task itself
    local ok, node = conn:call('app.get_node', {task.node_name})
    assert(ok, node)
    if task.last_message ~= NULL then
      content.n = task.last_message.content.n + 1
    else
      content.n = 1
    end

    -- fiber.sleep(0.3)  -- simulate hard work

    content.inputs = {}

    -- Process input messages
    local offset = 0
    for _, message in ipairs(task.input_messages) do

      -- Add input message to queue
      content.inputs[message.node_name] = message.content.n
      offset = math.max(offset, message.offset)

      -- Process messages from queue when all inputs have messages
      if offset > task.offset and fun.iter(node.inputs) then
        log.info(string.format(
            '%f %s: Adding message #%d with inputs %s to %s with offset %f',
            clock.time(), CLIENT_NAME, content.n,
            json.encode(content.inputs), name, offset
          ))
        local ok, result = conn:call(
          'app.add_message', {task.id, offset, content})
        assert(ok, result)
      end
    end

    -- Release task
    local ok, result = conn:call('app.release_task', {task.id})
    assert(ok, result)
  end
end
