local db = require 'db'
local record = require 'record'
local api = require 'api'
local rx = require 'rx'
local util = require 'util'

local assertup = util.assertup

local M = {}

--- Methods

local methods = {}

function methods.add_message(node_name, schema, offset, data)
  local node = db.node.get_by_name(node_name)
  local message = record.Message.from_map{
    node_id = node.id,
    schema = schema,
    offset = util.truth(offset) or 0,
    data = data
  }
  return db.message.add(message)
end

function methods.get_messages_by_id(node_name, min_id, max_id, limit)
  limit = util.truth(limit) and limit or 10000
  local node = db.node.get_by_name(node_name)
  return db.message.iter_by_id(node.id, min_id, max_id)
    :take_n(limit)
    :totable()
end

function methods.get_messages(node_names, offset, limit, get_prev)
  if type(node_names) ~= 'table' then
    node_names = { node_names }
  end
  limit = util.truth(limit) and limit or 10000
  assertup(type(limit) == 'number', 'limit should be integer or nil')

  local node_ids = fun.iter(node_names)
    :map(db.node.get_by_name)
    :map(util.itemgetter('id'))
  return db.message.iter(node_ids, offset, get_prev)
    :take_n(limit)
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
  ):tomap()
end


--- Service

function M.service(config, source, scheduler)

  if not db.message.is_ready() then
    log.warn('Message database not ready.')
    return
  end

  local sink = rx.Subject.create()

  api.publish(methods, 'message', 'app', source):subscribe(sink)

  return sink
end

return M
