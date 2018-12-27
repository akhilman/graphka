local clock = require 'clock'
local fun = require 'fun'
local log = require 'log'
local net_box = require 'net.box'

local CLIENT_NAME = 'make_graph'
local URL = os.getenv('GRAPHKA_URL')
URL = URL or 'localhost:3311'

local NODE_NAMES = {
  'example_source_a',
  'example_source_b',
  'example_zip',
  'example_sink'
}

-- Connect to server
log.info(string.format('%f %s: Connecting to %s',
                       clock.time(), CLIENT_NAME, URL))
local conn = net_box.connect(URL)
local ok = conn:wait_connected()
assert(ok, 'Can not connect to ' .. URL)
local ok, result = conn:call('app.rename_session', {CLIENT_NAME})
assert(ok, result)
local ok, session = conn:call('app.current_session')
assert(ok, session)
CLIENT_NAME = CLIENT_NAME .. '#' .. session.id

-- Remove nodes if exists
local ok, node_list = conn:call('app.list_nodes')
assert(ok)
node_list = fun.iter(node_list)
  :map(function(node) return node.name end)
  :filter(function(name) return fun.index(name, NODE_NAMES) end)
  :each(function(name)
      log.info(string.format('%f %s: Removing node %s',
                             clock.time(), CLIENT_NAME, name))
      local ok, result = conn:call('app.remove_node', {name})
      assert(ok, result)
    end)

-- Add nodes
for _, name in ipairs(NODE_NAMES) do
  log.info(string.format('%f %s: Adding node %s',
                         clock.time(), CLIENT_NAME, name))
  local ok, result = conn:call('app.add_node', {name})
  assert(ok, result)
end

-- Connect nodes
for _, names in ipairs({
      { 'example_source_a', 'example_zip' },
      { 'example_source_b', 'example_zip' },
      { 'example_zip', 'example_sink' },
    }) do
  log.info(string.format('%f %s: Connecting nodes %s -> %s',
                         clock.time(), CLIENT_NAME, names[1], names[2]))
  local ok, result = conn:call('app.connect_nodes', names)
  assert(ok, result)
end

-- Enable nodes
for _, name in ipairs(NODE_NAMES) do
  log.info(string.format('%f %s: Enabling node %s',
                         clock.time(), CLIENT_NAME, name))
  local ok, result = conn:call('app.enable_node', {name})
  assert(ok, result)
end

conn:close()
