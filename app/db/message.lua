local db_node = require 'db.node'
local fun = require 'fun'
local record = require 'record'
local rx = require 'rx'
local rxtnt = require 'rxtnt'
local util = require 'util'

local ITERATORS = {'EQ', 'REQ', 'GT', 'GE', 'LT', 'LE', 'ALL'}
local INDEXES = {'id', 'offset'}

local assertup = util.assertup

local M = {}

function M.is_ready()
  return fun.iter({
      'message',
      'message_index',
      'message_summary',
    })
    :map(function(k) return box.space[k] end)
    :all(fun.operator.truth)
end

function M.add(message)

  assertup(message._schema == 'message', "message should be message record")

  local message_index
  local row

  row = box.space.message_summary:get(message.node_id)
  if row then
    local summary = record('message_summary').from_tuple(row)
    assertup(
      summary.last_offset <= message.offset,
      'Message offset should be greater or equal to previous offset'
    )
  else
    row = box.space.node:get(message.node_id)
    assertup(row, 'No node with id ' .. message.node_id)
  end

  row = box.space.message:insert(message:to_tuple())
  message = record('message_index').from_tuple(row)

  row = box.space.message_index:insert(T.message_index.tuple(message))
  message_index = record('message_index').from_tuple(row)

  box.space.message_summary:upsert(
    {
      message.node_id,
      message.id,
      message.id,
      message.offset,
      message.offset,
      1
    }, {
      { '=', F.message_summary.last_id, message.id },
      { '=', F.message_summary.last_offset, message.offset },
      { '+', F.message_summary.count, 1 }
    }
  )

  return message_index
end

function M.summary(node_id)
  assertup(type(node_id) == 'number', 'node_id must be integer')
  local row
  local summary
  row = box.space.message_summary:get(node_id)
  if row then
    summary = record('message_summary').from_tuple(row)
  else
    row = box.space.node:get(node_id)
    assertup(row, 'No node with id ' .. node_id)
    summary = record('message_summary').from_map({node_id=node_id, count=0})
  end
  return summary
end

function M.get(id)
  assertup(type(id) == 'number', 'id must be integer')
  local row = box.space.message:get(id)
  assertup(row, 'No such message')
  return record('message').from_tuple(row)
end

function M.iter_index(index, iterator, start, node_id)

  assertup(type(iterator) == 'string', 'iterator must be stirng')
  assertup(type(node_id) == 'number', 'node_id must be integer')
  assertup(type(start) == 'number', 'offset must be integer')

  iterator = string.upper(iterator)
  index = string.lower(index)

  assertup(fun.index(index, INDEXES),
           'index must be one of: ' .. table.concat(INDEXES, ', '))
  assertup(fun.index(iterator, ITERATORS),
           'iterator must be one of: ' .. table.concat(ITERATORS, ', '))

  return fun.iter(box.space.message_index.index['node_id_and_' .. index]
      :pairs({node_id, start}, iterator))
    :map(record('message_index').from_tuple)
    :take_while(function(m) return m.node_id == node_id end)
end

function M.iter_by_id(node_id, min_id, max_id)

  local summary = M.summary(node_id) -- raises error if no such node
  if summary.count == 0 then
    return fun.iter({})
  end

  min_id = min_id and math.max(min_id, summary.first_id) or summary.first_id
  max_id = max_id and math.min(max_id, summary.last_id) or summary.last_id

  return M.iter_index('id', 'GE', min_id, node_id)
    :take_while(function(m) return m.id <= max_id end)
    :map(util.itemgetter('id'))
    :map(M.get)
end

function M.iter(node_ids, offset, get_prev)

  if type(node_ids) ~= 'table' then node_ids = {node_ids} end
  assertup(
    fun.iter(node_ids):all(function(id) return type(id) == 'number' end),
    'node_ids should be integer of list of integers'
  )
  offset = offset or 0
  assertup(type(offset) == 'number', 'offset should be number of nil')

  local function generator(param, state)

    local str_node_id
    local next_index
    local new_index

    if not next(state) then
      return nil, nil
    end

    str_node_id = fun.iter(state)
      :min_by(function(a, b)
          return state[a].offset < state[b].offset and a or b
        end)

    next_index = state[str_node_id]
    new_index = M.iter_index(
        'id', 'GT', next_index.id, next_index.node_id)
      :take_while(function(ix)
          return ix.id <= param[str_node_id].last_id
        end)
      :nth(1)
    state[str_node_id] = new_index

    return state, next_index

  end

  local start_offsets = fun.zip(
    fun.iter(node_ids):map(tostring),
    fun.duplicate(offset)
  ):tomap()

  if get_prev then
    start_offsets = util.merge_tables(
      start_offsets,
      fun.zip(
        fun.iter(node_ids):map(tostring),
        fun.iter(node_ids)
          :map(util.partial(M.iter_index, 'offset', 'LE', offset))
          :map(util.partial(fun.nth, 1))
          :filter(fun.operator.truth)
          :map(util.itemgetter('offset'))
      ):tomap()
    )
  end

  -- summary is param
  local param = fun.zip(
    fun.iter(node_ids):map(tostring),
    fun.iter(node_ids):map(M.summary)
  ):tomap()
  -- queue is state
  local state = fun.iter(start_offsets)
    :map(function(node_id, offset)
        local buf = M.iter_index('offset', 'GE', offset, tonumber(node_id))
          :nth(1)
        return node_id, buf
      end)
    :tomap()

  return fun.iter(generator, param, state)
    :map(util.itemgetter('id'))
    :map(M.get)
end

function M.remove(node_id, count)
  -- TODO implement db.message.remove()
  error('Not implemented')
end

return {
  message = M
}
