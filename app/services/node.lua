fiber = require 'fiber'
reqrep = require 'reqrep'
util = require 'util'
rx = require 'rx'

--- API

local methods = {}

local function format_node(node)
  assert(session._schema == 'node', 'node must be node record')
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
  ):totable()
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
      temporary = false,
    },
    params
  )
  local node = Record.from_map(params)
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

  local sink = rx.Subject.create()

  reqrep.dispatch(source, 'node:req', methods):subscribe(sink)

  source
    :filter(function(msg) return msg.topic == 'session:disconnected' end)
    :map(function(msg)
      return box.space['node'].index['tmp_session_id']:select(msg.session_id)
    end)
    :map(rx.Observable.fromTable)
    :flatMap()
    :map(function(row) return row[F.node.id] end)
    :subscribe(function(id) box.space['node']:delete(id) end)

  --- Trigger handlers

  local on_node_replace = util.partial(fiber.create, function(old, new)
    fiber.sleep(0.02)
    if not new and not old then
      return
    end
    local old_enabled = false
    local enabled = new and new[F.node.enabled] or false
    local node_id = new and new[F.node.id] or old[F.node.id]
    if not old then
      sink:onNext({topic = 'node:added', node_id = node_id})
    else
      old_enabled = old[F.node.enabled]
    end
    if old_enabled ~= enabled then
      sink:onNext({topic = enabled and 'node:enabled' or 'node:disabled',
                   node_id = node_id})
    end
    if not new then
      sink:onNext({topic = 'node:removed', node_id = node_id})
      local node_to_remove = fun.totable(fun.chain(
        -- remove node by required sink
        fun.iter(box.space['wire'].index['sink_id']:pairs(node_id))
          :filter(function(link) return link[F.wire.sink_required] end)
          :map(function(link) return link[F.wire.source_id] end),
        -- remove node by required source
        fun.iter(box.space['wire'].index['source_id']:pairs(node_id))
          :filter(function(link) return link[F.wire.source_required] end)
          :map(function(link) return link[F.wire.sink_id] end)
      ):filter(util.partial(fun.operator.ne, node_id)))
      -- remove dead links
      fun.iter(box.space['wire'].index['sink_id']:pairs(node_id))
        :map(function(link) return link[F.wire.id] end)
        :each(function(id) box.space['wire']:delete(id) end)
      fun.iter(box.space['wire'].index['source_id']:pairs(node_id))
        :map(function(link) return link[F.wire.id] end)
        :each(function(id) box.space['wire']:delete(id) end)
      -- remove node
      fun.iter(node_to_remove)
        :each(function(id) box.space['node']:delete(id) end)
    end
  end)

  local on_link_replace = util.partial(fiber.create, function(old, new)
    fiber.sleep(0.02)
    if not old then
      sink:onNext({
        topic = 'node:connected',
        node_id = new[F.wire.sink_id],
        source_id = new[F.wire.source_id]
      })
    elseif not new then
      sink:onNext({
        topic = 'node:disconnected',
        node_id = old[F.wire.sink_id],
        source_id = old[F.wire.source_id]
      })
    end
  end)

  local function remove_handlers()
    box.space['node']:on_replace(nil, on_node_replace)
    box.space['wire']:on_replace(nil, on_link_replace)
  end

  if box.space['node'] and box.space['wire'] then
    box.space['node']:on_replace(on_node_replace)
    box.space['wire']:on_replace(on_link_replace)
    source:subscribe(rx.util.noop, remove_handlers, remove_handlers)
  end

  return sink

end

return {
  services = services
}
