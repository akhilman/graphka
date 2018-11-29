api = require 'api'
db = require 'db'
fiber = require 'fiber'
record = require 'record'
rx = require 'rx'
util = require 'util'

--- API

local methods = {}

local function format_node(node)
  assert(node._schema == 'node', 'node must be node record')
  local ret = node:to_map()
  ret.inputs = db.node.iter_inputs(node.id)
    :map(util.itemgetter('name'))
    :totable()
  ret.outputs = db.node.iter_outputs(node.id)
    :map(util.itemgetter('name'))
    :totable()
  ret.requires = fun.chain(
    db.node.iter_inputs(node.id, true),
    db.node.iter_outputs(node.id, true)
  ):map(util.itemgetter('name')):totable()
  ret.temporary = fun.operator.truth(ret.tmp_session_id)
  ret.tmp_session_id = nil
  return ret
end

function methods.add_node(name, params)
  assert(type(name) == 'string', 'name must be string')
  assert(not params or type(params) == 'table', 'params must be table')
  params = util.merge_tables(
    {
      enabled = false,
      priority = 0,
      start_offset = 0,
      history_size = -1,
    },
    params
  )
  if params.temporary then
    params.tmp_session_id = box.session.id()
    params.temporary = nil
  end
  local node = record('node').from_map(params)
  node.id = nil
  node.name = name
  node = db.node.add(node)
  return format_node(node)
end

function methods.enable_node(name)
  assert(type(name) == 'string', 'name must be string')
  local node = db.node.get_by_name(name)
  db.node.alter(node.id, {enabled = true})
end

function methods.disable_node(name)
  assert(type(name) == 'string', 'name must be string')
  local node = db.node.get_by_name(name)
  db.node.alter(node.id, {enabled = false})
end

function methods.remove_node(name)
  assert(type(name) == 'string', 'name must be string')
  local node = db.node.get_by_name(name)
  local removed = db.node.remove(node.id)
  return #removed
end

function methods.list_nodes()
  return db.node.iter():map(format_node):totable()
end

function methods.connect_nodes(input, output, params)
  assert(type(input) == 'string', 'name must be string')
  assert(type(output) == 'string', 'name must be string')
  assert(not params or type(params) == 'table', 'params must be table')

  local input_node = db.node.get_by_name(input)
  local output_node = db.node.get_by_name(output)
  params = params or {}
  local input_required = fun.operator.truth(params.input_required)
  local output_required = fun.operator.truth(params.output_required)

  db.node.connect(
    input_node.id, output_node.id,
    input_required, output_required
  )
end

function methods.disconnect_nodes(input, output)
  assert(type(input) == 'string', 'name must be string')
  assert(type(output) == 'string', 'name must be string')

  local input_node = db.node.get_by_name(input)
  local output_node = db.node.get_by_name(output)

  db.node.disconnect(input_node.id, output_node.id)
end

--- Service

local services = {}

function services.node(config, source, scheduler)

  if not db.node.is_ready() then
    log.warn('Node database not ready.')
    return
  end

  local sink = rx.Subject.create()

  local events = db.node.observe()
  source:subscribe(rx.util.noop, events.stop, events.stop)
  events:delay(0.01, scheduler):subscribe(sink)

  api.publish(methods, 'node', 'api', source):subscribe(sink)

  source
    :filter(function(msg) return msg.topic == 'session_removed' end)
    :subscribe(function(msg)
      return db.node.remove_tmp(msg.session_id)
    end)

  return sink

end

return {
  services = services
}
