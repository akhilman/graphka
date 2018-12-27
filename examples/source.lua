local clock = require 'clock'
local fiber = require 'fiber'
local fun = require 'fun'
local log = require 'log'
local msgpack = require 'msgpack'
local net_box = require 'net.box'

local CLIENT_NAME = 'source'
local URL = os.getenv('GRAPHKA_URL')
URL = URL or 'localhost:3311'

local NODE_NAMES = {
  'example_source_a',
  'example_source_b'
}

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

-- Procuce messages
while true do

  local name = NODE_NAMES[math.random(1, #NODE_NAMES)]

  log.info(string.format('%f %s: Acquiring node %s',
                         clock.time(), CLIENT_NAME, name))
  local ok, task = conn:call('app.take_last', {name})
  assert(ok, task)

  if task == NULL then
    log.warn(string.format('%f %s: Node "%s" not ready',
                           clock.time(), CLIENT_NAME, name))

  else
    local content = {}
    local offset = clock.time()
    if task.last_message ~= NULL then
      content.n = task.last_message.content.n + 1
    else
      content.n = 1
    end

    -- fiber.sleep(0.3)  -- simulate hard work

    log.info(string.format(
        '%f %s: Adding message #%d to %s with offset %f',
        clock.time(), CLIENT_NAME, content.n, name, offset
      ))
    local ok, result = conn:call('app.add_message', {task.id, offset, content})
    assert(ok, result)

    local ok, result = conn:call('app.release_task', {task.id})
    assert(ok, result)
  end
  fiber.sleep(math.random())
end
