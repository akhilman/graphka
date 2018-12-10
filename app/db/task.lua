local clock = require 'clock'
local db_node = require 'db.node'
local db_session = require 'db.session'
local fun = require 'fun'
local record = require 'record'
local rx = require 'rx'
local rxtnt = require 'rxtnt'
local util = require 'util'

local assertup = util.assertup

local M = {}

function M.is_ready()
  return fun.iter({
      'task',
      'node_state',
    })
    :map(function(k) return box.space[k] end)
    :all(fun.operator.truth)
end

function M.exist_for_node(node_id)
  assertup(type(node_id) == 'number', 'node_id must be integer')
  return fun.operator.ne(box.space.task.index.node_id:count(node_id), 0)
end

function M.get(id)
  assertup(type(id) == 'number', 'id must be integer')
  local row = box.space.task:get(id)
  if not row then
    return nil
  end
  return record.Task.from_tuple(row)
end

function M.get_by_node(node_id)
  assertup(type(node_id) == 'number', 'node_id must be integer')
  local row = box.space.task.index.node_id:get(node_id)
  if not row then
    return nil
  end
  return record.Task.from_tuple(row)
end

function M.iter()
  return fun.iter(box.space.task:pairs())
    :map(record.Task.from_tuple)
end

function M.iter_by_session_id(session_id)
  assertup(type(session_id) == 'number', 'session_id must be number')
  return fun.iter(box.space.task.index.session_id:pairs(session_id))
    :map(record.Task.from_tuple)
end

function M.iter_expired(time)
  assertup(type(time) == 'number', 'time must be number')
  return fun.iter(
      box.space.task.index.expires:pairs(time, 'LT'))
    :map(record.Task.from_tuple)
end

function M.add(task)
  assertup(task._schema == 'task', 'task must be task record')
  assertup(fun.operator.ne(box.space.node:count(task.node_id), 0),
           string.format('No such node #%d', task.node_id))
  assertup(fun.operator.ne(box.space.session:count(task.session_id), 0),
           string.format('No such session #%d', task.session_id))
  assertup(type(task.expires) == 'number', 'task.expires must be number')
  local row = box.space.task:insert(task:to_tuple())
  return record.Task.from_tuple(row)
end

function M.set_offset(id, offset)
  assertup(type(id) == 'number', 'id must be integer')
  local row = box.space.task:update(id, {
    { '=', F.task.offset, offset }
  })
  if not row then
    return nil
  end
  return record.Task.from_tuple(row)
end

function M.set_expires(id, expires)
  assertup(type(id) == 'number', 'id must be integer')
  local row = box.space.task:update(id, {
    { '=', F.task.expires, expires }
  })
  assertup(row, string.format('No task #%s', id))
  return record.Task.from_tuple(row)
end

function M.remove(id)
  assertup(type(id) == 'number', 'id must be integer')
  local row = box.space.task:delete(id)
  assertup(row, string.format('No task #%s', id))
  return record.Task.from_tuple(row)
end

-- Node offset

function M.get_node_state(node_id)
  assertup(type(node_id) == 'number', 'node_id must be integer')
  local row = box.space.node_state:get(node_id)
  if not row then
    local node_row = box.space.node:get(node_id)
    if not node_row then
      return nil
    end
    local node = record.Node.from_tuple(node_row)
    return record.NodeState.from_map({
      node_id = node.id,
      offset = node.start_offset,
      atime = 0,
    })
  end
  return record.NodeState.from_tuple(row)
end

function M.set_node_state(node_id, offset)
  local old_offset = M.get_node_state(node_id).offset
  local atime = clock.time()
  assertup(offset >= old_offset,
           'New offset should be greater or equal to old one')
  if offset > old_offset then
    box.space.node_state:upsert({
      node_id,
      offset,
      atime
    }, {
      { '=', F.node_state.offset, offset },
      { '=', F.node_state.atime, atime }
    })
  end
  return record.NodeState.create(node_id, offset, atime)
end

function M.touch_node_state(node_id)
  local old_offset = M.get_node_state(node_id).offset
  local atime = clock.time()
  box.space.node_state:upsert({
    node_id,
    old_offset,
    atime
  }, {
    { '=', F.node_state.atime, atime }
  })
  return record.NodeState.create(node_id, old_offset, atime)
end

function M.clear_node_state(node_id)
  local row = box.space.node_state:delete(node_id)
  if not row then
    return nil
  end
  return record.NodeState.from_map(row)
end

function M.iter_node_states()
  return fun.iter(box.space.node_state:pairs())
    :map(record.NodeState.from_tuple)
end

-- Observe

function M.observe()

  local task_trigger = rxtnt.ObservableTrigger.create(function(...)
    box.space.task:on_replace(...)
  end)

  local offset_trigger = rxtnt.ObservableTrigger.create(function(...)
    box.space.node_state:on_replace(...)
  end)

  local task_events = task_trigger:map(function(old, new)
      old = old and record.Task.from_tuple(old) or nil
      new = new and record.Task.from_tuple(new) or nil
      if not old then
        local msg =  {
          topic = 'task_added',
          node_id = new.node_id,
          task_id = new.id,
          session_id = new.session_id,
          offset = new.offset,
          expires = new.expires
        }
        return msg
      elseif not new then
        return {
          topic = 'task_removed',
          node_id = old.node_id,
          task_id = old.id,
          session_id = old.session_id,
          offset = old.offset
        }
      else
        return {
          topic = 'task_altered',
          node_id = new.node_id,
          task_id = new.id,
          session_id = new.session_id,
          offset = new.offset,
          expires = new.expires
        }
      end
    end)

  local offset_events = offset_trigger:map(function(old, new)
      old = old and record.NodeState.from_tuple(old) or nil
      new = new and record.NodeState.from_tuple(new) or nil
      if new and (not old or new.offset > old.offset) then
        local msg =  {
          topic = 'offset_reached',
          node_id = new.node_id,
          offset = new.offset
        }
        return msg
      end
    end)
    :filter(fun.operator.truth)

  local events = task_events:merge(offset_events)

  events.stop = function ()
    task_trigger:stop()
  end

  return events
end

return {
  task = M
}
