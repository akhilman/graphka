local app = require 't.app'
local tap = require 'tap'
local tnt = require 't.tnt'
local rx = require 'rx'

tnt.cfg{}
app.init{}

local test = tap.test("Nodes module")
test:plan(2)

local success, ret

for n=1, 10, 1 do
  app.add_node('node' .. n)
end
for n=1, 9, 1 do
  app.connect_nodes(
    'node' .. n, 'node' .. n + 1,
    {input_required = true, output_required = true}
  )
end

success, ret = app.list_nodes()
test:is(#ret, 10, '10 nodes in list')

app.remove_node('node6')

success, ret = app.list_nodes()
test:is(#ret, 0, 'No nodes in list')

app.destroy()
tnt.finish()
test:check()
os.exit()
