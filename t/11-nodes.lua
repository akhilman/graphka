local tap = require 'tap'
local tnt = require 't.tnt'
local rx = require 'rx'
local fiber = require 'fiber'

tnt.cfg{}
local app = require 'app'
app.init{automigrate=true}

local test = tap.test("Nodes module")
test:plan(2)

local success, rep

for n=1, 10, 1 do
  graphka.add_node('node' .. n)
end
for n=1, 9, 1 do
  graphka.connect_nodes(
    'node' .. n, 'node' .. n + 1,
    {sink_required = true, source_required = true}
  )
end

success, rep = graphka.list_nodes()
test:is(#rep, 10, 'Test add node responce')

graphka.remove_node('node6')
fiber.sleep(0.2)

success, rep = graphka.list_nodes()
test:is_deeply(rep, {}, 'Test remove by required responce')

tnt.finish()
test:check()
os.exit()
