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

function M.summary(node_id)
  assertup(type(node_id) == 'number', 'node_id must be integer')
  local row
  local summary
  row = box.space.message_summary:get(node_id)
  if row then
    summary = record.MessageSummary.from_tuple(row)
  else
    row = box.space.node:get(node_id)
    if not row then
      return nil
    end
    summary = record.MessageSummary.from_map({node_id=node_id, count=0})
  end
  return summary
end

function M.iter_summary()
  return fun.iter(box.space.message_summary:pairs())
    :map(record.MessageSummary.from_tuple)
end

function M.add(message)

  assertup(message._schema == 'message', "message should be message record")

  local message_index
  local row

  row = box.space.message_summary:get(message.node_id)
  if row then
    local summary = record.MessageSummary.from_tuple(row)
    assertup(
      summary.last_offset <= message.offset,
      'Message offset should be greater or equal to previous offset'
    )
  else
    row = box.space.node:get(message.node_id)
    assertup(row, 'No node with id ' .. message.node_id)
  end

  row = box.space.message:insert(message:to_tuple())
  message = record.MessageIndex.from_tuple(row)

  row = box.space.message_index:insert(T.message_index.tuple(message))
  message_index = record.MessageIndex.from_tuple(row)

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

function M.get(id)
  assertup(type(id) == 'number', 'id must be integer')
  local row = box.space.message:get(id)
  if not row then
    return nil
  end
  return record.Message.from_tuple(row)
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
    :map(record.MessageIndex.from_tuple)
    :take_while(function(m) return m.node_id == node_id end)
end

function M.iter_by_id(node_id, min_id, max_id)

  local summary = M.summary(node_id) -- raises error if no such node
  if summary.count == 0 then
    return fun.iter({})
  end

  min_id = util.truth(min_id) and math.max(min_id, summary.first_id)
    or summary.first_id
  max_id = util.truth(max_id) and math.min(max_id, summary.last_id)
    or summary.last_id

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
  offset = util.truth(offset) and offset or 0
  assertup(type(offset) == 'number', 'offset should be number of nil')
  get_prev = util.truth(get_prev)

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

function M.remove(node_id, limit)
  local n_removed = 0
  local prev
  fun.chain(M.iter_index('id', 'GT', 0, node_id), {'drop_summary'})
    :take_n(limit + 1)
    :each(function(index)
      if prev then
        if index == 'drop_summary' then
          box.space.message_summary:delete(node_id)
        else
          box.space.message_summary:update(
            node_id,
            {
              { '=', F.message_summary.first_id, index.id },
              { '=', F.message_summary.first_offset, index.offset },
              { '-', F.message_summary.count, 1 }
            }
          )
        end
        box.space.message_index:delete(prev.id)
        box.space.message:delete(prev.id)
        n_removed = n_removed + 1
      end
      prev = index
    end)

    return n_removed
end

-- Observe

function M.observe()

  local message_index_trigger = rxtnt.ObservableTrigger.create(function(...)
    box.space.message_index:on_replace(...)
  end)

  local events = message_index_trigger:map(function(old, new)
    old = old and record.MessageIndex.from_tuple(old) or nil
    new = new and record.MessageIndex.from_tuple(new) or nil
    if not old then
      return {
        topic = 'message_added',
        node_id = new.node_id,
        message_id = new.id,
        offset = new.offset
      }
    elseif not new then
      return {
        topic = 'message_removed',
        node_id = old.node_id,
        message_id = old.id,
        offset = old.offset
      }
    else
      return {
        topic = 'message_altered',
        node_id = new.node_id,
        message_id = new.id
      }
    end
  end)

  events.stop = function ()
    message_index_trigger:stop()
  end

  return events
end

return {
  message = M
}
