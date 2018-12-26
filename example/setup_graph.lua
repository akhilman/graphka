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
log.info(CLIENT_NAME .. ': Connecting to ' .. URL)
local conn = net_box.connect(URL)
local ok = conn:wait_connected()
assert(ok, 'Can not connect to ' .. URL)
local ok, result = conn:call('app.rename_session', {CLIENT_NAME}, {})
assert(ok, result)

-- Remove nodes if exists
local ok, node_list = conn:call('app.list_nodes', {}, {})
assert(ok)
node_list = fun.iter(node_list)
  :map(function(n) return n.name end)
  :filter(function(n) return fun.index(n, NODE_NAMES) end)
  :each(function(n)
      log.info(CLIENT_NAME .. ': Removing node ' .. n)
      local ok, result = conn:call('app.remove_node', {n}, {})
      assert(ok, result)
    end)

-- Adding nodes
for _, name in ipairs(NODE_NAMES) do
  log.info(CLIENT_NAME .. ': Adding node ' .. name)
  local ok, result = conn:call('app.add_node', {name}, {})
  assert(ok, result)
end

-- Connect nodes
for _, names in ipairs({
      { 'example_source_a', 'example_zip' },
      { 'example_source_b', 'example_zip' },
      { 'example_zip', 'example_sink' },
    }) do
  log.info(CLIENT_NAME .. ': Connecting nodes: ' .. names[1] .. ' -> ' .. names[2])
  local ok, result = conn:call('app.connect_nodes', names, {})
  assert(ok, result)
end

-- Enabling nodes
for _, name in ipairs(NODE_NAMES) do
  log.info(CLIENT_NAME .. ': Enabling node ' .. name)
  local ok, result = conn:call('app.enable_node', {name}, {})
  assert(ok, result)
end

conn:close()
