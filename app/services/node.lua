local api = require 'api'
local db = require 'db'
local fun = require 'fun'
local record = require 'record'
local rx = require 'rx'
local util = require 'util'

local M = {}

--- API

local methods = {}

function methods.add_node(call, name, params)
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
    params.tmp_session_id = call.session_id
    params.temporary = nil
  end
  local node = record.Node.from_map(params)
  node.id = nil
  node.name = name
  node = db.node.add(node)
  return db.node.format_node(node)
end

function methods.enable_node(call, name)
  assert(type(name) == 'string', 'name must be string')
  local node = db.node.get_by_name(name)
  assert(node, string.format('No such node "%s"', name))
  db.node.alter(node.id, {enabled = true})
end

function methods.disable_node(call, name)
  assert(type(name) == 'string', 'name must be string')
  local node = db.node.get_by_name(name)
  assert(node, string.format('No such node "%s"', name))
  db.node.alter(node.id, {enabled = false})
end

function methods.remove_node(call, name)
  assert(type(name) == 'string', 'name must be string')
  local node = db.node.get_by_name(name)
  assert(node, string.format('No such node "%s"', name))
  local removed = db.node.remove(node.id)
  return #removed
end

function methods.get_node(call, name)
  assert(type(name) == 'string', 'name must be string')
  local node = db.node.get_by_name(name)
  return db.node.format_node(node)
end

function methods.list_nodes()
  return db.node.iter():map(db.node.format_node):totable()
end

function methods.connect_nodes(call, input, output, params)
  assert(type(input) == 'string', 'name must be string')
  assert(type(output) == 'string', 'name must be string')
  assert(not params or type(params) == 'table', 'params must be table')

  local input_node = db.node.get_by_name(input)
  local output_node = db.node.get_by_name(output)
  assert(input_node, string.format('No such node "%s"', input))
  assert(output_node, string.format('No such node "%s"', output))

  params = params or {}
  local input_required = fun.operator.truth(params.input_required)
  local output_required = fun.operator.truth(params.output_required)

  db.node.connect(
    input_node.id, output_node.id,
    input_required, output_required
  )
end

function methods.disconnect_nodes(call, input, output)
  assert(type(input) == 'string', 'name must be string')
  assert(type(output) == 'string', 'name must be string')

  local input_node = db.node.get_by_name(input)
  local output_node = db.node.get_by_name(output)
  assert(input_node, string.format('No such node "%s"', input))
  assert(output_node, string.format('No such node "%s"', output))

  db.node.disconnect(input_node.id, output_node.id)
end

--- Service

function M.service(config, source, scheduler)

  if not db.node.is_ready() then
    log.warn('Node database not ready.')
    return
  end

  local sink = rx.Subject.create()

  local events = db.node.observe()
  source:filter(util.itemeq('topic', 'stop')):subscribe(events.stop)
  events:delay(0, scheduler):subscribe(sink)

  api.publish(methods, 'node', 'app', source, true):subscribe(sink)

  source
    :filter(function(msg) return msg.topic == 'session_removed' end)
    :subscribe(function(msg)
      return db.node.remove_tmp(msg.session_id)
    end)

  return sink

end


return M
