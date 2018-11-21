fiber = require 'fiber'
reqrep = require 'reqrep'
util = require 'util'
rx = require 'rx'

local services = {}

function services.node(config, source)

  local sink = rx.Subject.create()

  local methods = {}

  local function format_node(row)
    local sources = fun.totable(
      fun.iter(box.space['node_links'].index['sink_id']
               :pairs(row[F.node.id]))
      :map(function(link) return link[F.node_links.source_id] end)
      :map(function(id) return box.space['node']:get(id) end)
      :filter(fun.operator.truth)
      :map(function(node) return node[F.node.name] end)
    )
    local sinks = fun.totable(
      fun.iter(box.space['node_links'].index['source_id']
               :pairs(row[F.node.id]))
      :map(function(link) return link[F.node_links.sink_id] end)
      :map(function(id) return box.space['node']:get(id) end)
      :filter(fun.operator.truth)
      :map(function(node) return node[F.node.name] end)
    )
    return {
      -- id = row[F.node.id],
      name = row[F.node.name],
      enabled = row[F.node.enabled],
      nice = row[F.node.nice],
      start_offset = row[F.node.start_offset],
      history_size = row[F.node.history_size],
      temporary = row[F.node.tmp_session_id] and true or false,
      sources = sources,
      sinks = sinks,
    }
  end

  function methods.add_node(name, params)
    params = util.merge_tables(
      {
        enabled = false,
        nice = 0,
        start_offset = 0,
        history_size = -1,
        temporary = false,
      },
      params or {}
    )
    local row = box.space['node']:insert{
      nil, name,
      params.enabled, params.nice, params.start_offset, params.history_size,
      params.temporary and box.session.id() or nil
    }
    return format_node(row)
  end

  function methods.enable_node(name)
    local row = box.space['node'].index.name:update(
      name, {{'=', F.node.enabled, true}}
    )
  end

  function methods.disable_node(name)
    local row = box.space['node'].index.name:update(
      name, {{'=', F.node.enabled, false}}
    )
  end

  function methods.remove_node(name)
    local row = box.space['node'].index.name:delete(name)
    if not row then
      error('No such node "' .. name .. '"')
    end
  end

  function methods.list_nodes()
    return fun.totable(
      fun.iter(box.space['node']:pairs()):map(format_node)
    )
  end

  function methods.connect_nodes(source, sink, params)

    params = util.merge_tables(
      {
        source_required = false,
        sink_required = false,
      },
      params or {}
    )

    local source_row = box.space['node'].index.name:get(source)
    if not source_row then
      error('No such node "' .. source .. '"')
    end
    local source_id = source_row[F.node.id]

    local sink_row = box.space['node'].index.name:get(sink)
    if not sink_row then
      error('No such node "' .. sink .. '"')
    end
    local sink_id = sink_row[F.node.id]

    box.space['node_links']:insert{
      nil, source_id, sink_id,
      params.source_required, params.sink_required
    }

  end

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
        fun.iter(box.space['node_links'].index['sink_id']:pairs(node_id))
          :filter(function(link) return link[F.node_links.sink_required] end)
          :map(function(link) return link[F.node_links.source_id] end),
        -- remove node by required source
        fun.iter(box.space['node_links'].index['source_id']:pairs(node_id))
          :filter(function(link) return link[F.node_links.source_required] end)
          :map(function(link) return link[F.node_links.sink_id] end)
      ):filter(util.partial(fun.operator.ne, node_id)))
      -- remove dead links
      fun.iter(box.space['node_links'].index['sink_id']:pairs(node_id))
        :map(function(link) return link[F.node_links.id] end)
        :each(function(id) box.space['node_links']:delete(id) end)
      fun.iter(box.space['node_links'].index['source_id']:pairs(node_id))
        :map(function(link) return link[F.node_links.id] end)
        :each(function(id) box.space['node_links']:delete(id) end)
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
        node_id = new[F.node_links.sink_id],
        source_id = new[F.node_links.source_id]
      })
    elseif not new then
      sink:onNext({
        topic = 'node:disconnected',
        node_id = old[F.node_links.sink_id],
        source_id = old[F.node_links.source_id]
      })
    end
  end)

  local function remove_handlers()
    box.space['node']:on_replace(nil, on_node_replace)
    box.space['node_links']:on_replace(nil, on_link_replace)
  end

  if box.space['node'] and box.space['node_links'] then
    box.space['node']:on_replace(on_node_replace)
    box.space['node_links']:on_replace(on_link_replace)
    source:subscribe(rx.util.noop, remove_handlers, remove_handlers)
  end

  return sink

end

return {
  services = services
}
