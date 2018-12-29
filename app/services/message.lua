local db = require 'db'
local record = require 'record'
local api = require 'api'
local rx = require 'rx'
local util = require 'util'

local assertup = util.assertup

local M = {}

--- Methods

local methods = {}

function methods.add_message(node_name_or_task_id, offset, content)
  assert(type(node_name_or_task_id) == 'string'
         or type(node_name_or_task_id) == 'number',
         'First argument should be node name or task id')
  local node
  if type(node_name_or_task_id) == 'string' then
    node = db.node.get_by_name(node_name_or_task_id)
  else
    local task = db.task.get(node_name_or_task_id)
    assert(task, string.format('No such task #%d', node_name_or_task_id))
    node = db.node.get(task.node_id)
  end
  assert(node, string.format('No such node "%s"', node_name_or_task_id))
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
  assert(type(limit) == 'number', 'limit should be integer or nil')

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

local function purge_messages(config, force)

  log.debug('Purging messages')

  local limit = config.purge_message_limit

  for _, summary in db.message.iter_summary() do
    if not force and limit <= 0 then
      break
    end

    local to_remove = 0
    local node = db.node.get(summary.node_id)
    if not node then
      to_remove = summary.count
    elseif node and node.history_size > 0 then
      to_remove = math.max(0, summary.count - node.history_size)
    end

    if not force then
      to_remove = math.min(to_remove, limit)
    end

    if to_remove > 0 then
      local n_removed = db.message.remove(summary.node_id, to_remove)
      log.verbose(string.format("%d messages removed from node #%d",
                                n_removed, summary.node_id))
      limit = limit - n_removed
    end
  end
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

  source
    :filter(util.itemeq('topic', 'purge'))
    :map(util.itemgetter('force'))
    :subscribe(util.partial(purge_messages, config))

  api.publish(methods, 'message', 'app', source):subscribe(sink)

  return sink
end

return M
