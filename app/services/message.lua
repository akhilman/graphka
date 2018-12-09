local db = require 'db'
local record = require 'record'
local api = require 'api'
local rx = require 'rx'
local util = require 'util'

local assertup = util.assertup

local M = {}

--- Methods

local methods = {}

function methods.add_message(node_name, offset, content)
  local node = db.node.get_by_name(node_name)
  assertup(node, string.format('No such node "%s"', node_name))
  local message = record.Message.from_map{
    node_id = node.id,
    offset = util.truth(offset) and offset or 0,
    content = content
  }
  return db.message.add(message)
end

function methods.get_messages_by_id(node_name, min_id, max_id, limit)
  limit = util.truth(limit) and limit or 10000
  local node = db.node.get_by_name(node_name)
  assert(node, string.format('No sucn node %s', node_name))
  return db.message.iter_by_id(node.id, min_id, max_id)
    :take_n(limit)
    :map(db.node.make_name_resolver())
    :totable()
end

function methods.get_messages(node_names, offset, limit, get_prev)
  if type(node_names) ~= 'table' then
    node_names = { node_names }
  end
  limit = util.truth(limit) and limit or 10000
  assertup(type(limit) == 'number', 'limit should be integer or nil')

  local node_ids = fun.iter(node_names)
    :map(function(name)
        local node = db.node.get_by_name(name)
        if node then return node
        else error(string.format('No such node %s', name))
        end
      end)
    :map(util.itemgetter('id'))
  return db.message.iter(node_ids, offset, get_prev)
    :take_n(limit)
    :map(db.node.make_name_resolver())
    :totable()
end

function methods.message_summary()
  local nodes = db.node.iter():totable()
  return fun.zip(
    fun.iter(nodes)
      :map(util.itemgetter('name')),
    fun.iter(nodes)
      :map(util.itemgetter('id'))
      :map(db.message.summary)
      :map(db.node.make_name_resolver())
  ):tomap()
end

--- Purge messages

local function purge_loop(config, control_chan, pending_nodes, forced)

  local interval = config.purge_interval
  local limit = config.purge_message_limit

  if not forced then
    local cmd = control_chan:get(interval)
    if cmd == 'stop' then
      return
    elseif cmd == 'purge' then
      forced = true
      pending_nodes = db.message.iter_summary():totable()
    end
    if not pending_nodes or #pending_nodes == 0 then
      pending_nodes = db.message.iter_summary():totable()
    end
  else
    fiber.sleep(0.1)
  end

  local summary = table.remove(pending_nodes)
  if not summary then
    return purge_loop(config, control_chan)
  end

  log.debug(string.format('Purging messages form node #%d',
                          summary.node_id))

  local to_remove = 0
  local node = db.node.get(summary.node_id)
  if not node then
    to_remove = summary.count
  elseif node and node.history_size > 0 then
    to_remove = math.max(0, summary.count - node.history_size)
  end

  if to_remove > 0 then
    local n_removed = db.message.remove(
      summary.node_id,
      math.min(to_remove, limit)
    )
    log.verbose(string.format("%d messages removed from node #%s",
                              n_removed, summary.node_id))
    if n_removed < to_remove then
      table.insert(pending_nodes, db.message.summary(summary.node_id))
    end
  end

  return purge_loop(config, control_chan, pending_nodes, forced)
end

--- Service

function M.service(config, source, scheduler)

  if not db.message.is_ready() then
    log.warn('Message database not ready.')
    return
  end

  local sink = rx.Subject.create()

  local events = db.message.observe()
  source:filter(util.itemeq('topic', 'stop')):subscribe(events.stop)
  events:delay(0, scheduler):subscribe(sink)

  local purge_ctrl = fiber.channel()
  fiber.create(purge_loop, config, purge_ctrl)
  source:filter(function(msg)
      return fun.index(msg.topic, {'purge', 'stop'})
    end)
    :subscribe(function(msg) purge_ctrl:put(msg.topic) end)

  api.publish(methods, 'message', 'app', source):subscribe(sink)

  return sink
end

return M
