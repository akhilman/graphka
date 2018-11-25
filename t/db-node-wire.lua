local tap = require 'tap'
local tnt = require 't.tnt'
local rx = require 'rx'
local fiber = require 'fiber'
local db = require 'db'
local util = require 'util'
local Record = require 'record'


--[[
           ,------------+------> E
          \/            |
 A -r---> B -r--------> D -r---> F
          |             ^
           ---r> C -r---'        G

  -r----> - input required
  -----r> - output required

--]]

tnt.cfg{}

local test = tap.test("db.node: Node dependences and connections")
test:plan(29)

local success, ret
local nodes = {}

ret = db.node.iter():totable()
test:is(#ret, 0, 'Empty node list')

-- Add nodes

for _, name in ipairs({'A', 'B', 'C', 'D', 'E', 'F', 'G'}) do
  nodes[name] = db.node.add(Record.from_map('node', {
    name = name,
    enabled = false,
    priority = 0,
    history_size = 100,
    tmp_session_id = box.session.id()
  }))
end

ret = db.node.iter():totable()
test:is(#ret, 7, '7 nodes in list')

-- Connect nodes

db.node.connect(nodes.A.id, nodes.B.id, true, false)
db.node.connect(nodes.B.id, nodes.C.id, false, true)
db.node.connect(nodes.B.id, nodes.D.id, true, false)
db.node.connect(nodes.C.id, nodes.D.id, true, false)
db.node.connect(nodes.D.id, nodes.B.id, false, false)
db.node.connect(nodes.D.id, nodes.E.id, false, false)
db.node.connect(nodes.D.id, nodes.F.id, true, false)

-- Check inputs

ret = db.node.iter_inputs(nodes.B.id)
  :map(function(n) return n.name end)
  :totable()
test:is(#ret, 2, 'Node B have 2 inputs')
test:ok(fun.index('A', ret), 'Node A in node B inputs')
test:ok(fun.index('D', ret), 'Node D in node B inputs')

-- Check required inputs

ret = db.node.iter_inputs(nodes.B.id, true)
  :map(function(n) return n.name end)
  :totable()
test:is(#ret, 1, 'Node B have 1 required inputs')
test:ok(fun.index('A', ret), 'Node A in node B required inputs')

-- Check outputs

ret = db.node.iter_outputs(nodes.B.id)
  :map(function(n) return n.name end)
  :totable()
test:is(#ret, 2, 'Node B have 2 outputs')
test:ok(fun.index('C', ret), 'Node C in node B outputs')
test:ok(fun.index('D', ret), 'Node D in node B outputs')

-- Check required outputs

ret = db.node.iter_outputs(nodes.B.id, true)
  :map(function(n) return n.name end)
  :totable()
test:is(#ret, 1, 'Node B have 1 required outputs')
test:ok(fun.index('C', ret), 'Node C in node B required outputs')

-- Check recursive

ret = db.node.iter_recursive(nodes.C.id)
  :take_n(10)
  :map(util.itemgetter('name'))
  :totable()
test:is(#ret, 6, '6 nodes is wired together')
test:isnil(fun.index('G', ret), 'Node G not wired with C, C included')

-- Check required recursive

ret = db.node.iter_recursive(nodes.D.id, true)
  :map(util.itemgetter('name'))
  :totable()
test:is(#ret, 4, '4 nodes is requird to D, D inclued')
test:isnil(fun.index('F', ret), 'Node F not required to D')
test:isnil(fun.index('E', ret), 'Node E not required to D')
test:isnil(fun.index('G', ret), 'Node G not required to D')

-- Check remove

ret = fun.iter(db.node.remove(nodes.D.id))
  :map(util.itemgetter('name'))
  :totable()
test:is(#ret, 4, '4 nodes is removed by D')

ret = db.node.iter():map(util.itemgetter('name')):totable()
test:is(#ret, 3, '3 nodes in list')
test:ok(fun.index('F', ret), 'Node F not removed to D')
test:ok(fun.index('E', ret), 'Node E not removed to D')
test:ok(fun.index('G', ret), 'Node G not removed to D')

-- Connect and disconnect E ----> F

ret = db.node.iter_outputs(nodes.E.id):map(util.itemgetter('name')):totable()
test:is(#ret, 0, '0 outputs for E before connect')
ret = db.node.iter_inputs(nodes.F.id):map(util.itemgetter('name')):totable()
test:is(#ret, 0, '0 inputs for F before connect')

ret = db.node.connect(nodes.E.id, nodes.F.id, false, false)

ret = db.node.iter_outputs(nodes.E.id):map(util.itemgetter('name')):totable()
test:is(#ret, 1, '1 outputs for E after connect')
ret = db.node.iter_inputs(nodes.F.id):map(util.itemgetter('name')):totable()
test:is(#ret, 1, '1 inputs for F after connect')

db.node.disconnect(nodes.E.id, nodes.F.id)

ret = db.node.iter_outputs(nodes.E.id):map(util.itemgetter('name')):totable()
test:is(#ret, 0, '0 outputs for E after disconnect')
ret = db.node.iter_inputs(nodes.F.id):map(util.itemgetter('name')):totable()
test:is(#ret, 0, '0 inputs for F after disconnect')

tnt.finish()
test:check()
os.exit()

