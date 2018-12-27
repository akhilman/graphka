local clock = require 'clock'
local fiber = require 'fiber'
local fun = require 'fun'
local json = require 'json'
local log = require 'log'
local msgpack = require 'msgpack'
local net_box = require 'net.box'

local CLIENT_NAME = 'sink'
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

  local name = 'example_sink'

  -- Get task
  log.info(string.format('%f %s: Getting task for node %s',
                         clock.time(), CLIENT_NAME, name))
  local ok, task = conn:call('app.take_task', {name})
  assert(ok, task)

  if task ~= NULL then

    -- Process input messages
    local offset = 0
    for _, message in fun.iter(task.input_messages)
        :filter(function(msg) return msg.offset > task.offset end) do

      log.info(string.format(
          '%f %s: Got message #%d with from %s with offset %f',
          clock.time(), CLIENT_NAME,
          message.content.n, message.node, message.offset
        ))
      offset = math.max(offset, message.offset)
    end

    -- Add empty message to cutoff all input messages before offset
    local ok, result = conn:call(
      'app.add_message', {task.id, offset, NULL})
    assert(ok, result)

    -- Release task
    local ok, result = conn:call('app.release_task', {task.id})
    assert(ok, result)
  end
end
