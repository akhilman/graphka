local record = require 'record'
local rx = require 'rx'
local rxtnt = require 'rxtnt'
local util = require 'util'

local assertup = util.assertup

local M = {}

function M.is_ready()
  return (fun.operator.truth(box.space.node)
          and fun.operator.truth(box.space.wire))
end

-- Node

function M.add(node)
  assertup(node._schema == 'node', 'node must be node record')
  local row = box.space.node:insert(node:to_tuple())
  return record.Node.from_tuple(row)
end

function M.remove(id)
  local function remove_one(id)
    fun.chain(fun.iter(box.space.wire.index['input_id']:pairs(id)),
              fun.iter(box.space.wire.index['output_id']:pairs(id)))
      :map(record.Wire.from_tuple)
      :each(function(w)
        box.space.wire:delete({w.input_id, w.output_id})
      end)
    local row = box.space.node:delete(id)
    return record.Node.from_tuple(row)
  end
  return fun.iter(
    M.iter_recursive(id, true)
    :map(util.itemgetter('id'))
    :totable()
  ):map(remove_one):totable()
end

function M.remove_tmp(session_id)
  local removed = {}
  for _, n in fun.iter(
        box.space.node.index['tmp_session_id']:select(session_id))
      :map(record.Node.from_tuple) do
    if not fun.index(n.id, removed) then
      removed = util.concatenate(removed, M.remove(n.id))
    end
  end
  return removed
end

function M.alter(id, params)
  assertup(type(id) == 'number', 'id must be integer')
  assertup(type(params) == 'table', 'params must be table')
  assertup(fun.length(params) > 0, 'params must be non empty table')
  assertup(not params.id, 'Can not alter node\'s "id" attribute')
  local unknown = fun.iter(params)
    :filter(function(k, v) return not F.node[k] end)
    :totable()
  assertup(fun.length(unknown) == 0,
         'Node has no params: ' .. table.concat(unknown, ', '))
  local row = box.space.node:update(
    id,
    fun.iter(params)
      :map(function(k, v) return {'=', F.node[k], v} end)
      :totable()
  )
  return record.Node.from_tuple(row)
end

function M.get(id)
  assertup(type(id) == 'number', 'id must be integer')
  local row = box.space.node:get(id)
  assertup(row, 'No such node')
  return record.Node.from_tuple(row)
end

function M.get_by_name(name)
  assertup(type(name) == 'string', 'name must be string')
  local row = box.space.node.index['name']:get(name)
  assertup(row, string.format('No such node "%s".', name))
  return record.Node.from_tuple(row)
end

function M.iter()
  return fun.iter(box.space.node:pairs())
    :map(record.Node.from_tuple)
end

-- Wire

function M.iter_inputs(id, required)
  assertup(type(id) == 'number', 'id must be integer')
  return fun.iter(box.space.wire.index['output_id']:pairs(id))
    :map(record.Wire.from_tuple)
    :filter(function(wire) return not required or wire.input_required end)
    :map(function(wire) return M.get(wire.input_id) end)
end

function M.iter_outputs(id, required)
  assertup(type(id) == 'number', 'id must be integer')
  return fun.iter(box.space.wire.index['input_id']:pairs(id))
    :map(record.Wire.from_tuple)
    :filter(function(wire) return not required or wire.output_required end)
    :map(function(wire) return M.get(wire.output_id) end)
end

function M.iter_recursive(id, required)
  -- Iterates over all connected nodes recursively including current node

  local function generator(param, state)

    local node
    local state = table.deepcopy(state)
    local inner = state.inner

    if not inner or not inner.state then

      if state.current == #state.queue then
        return nil, nil  -- stop iteration
      end

      state.current = state.current + 1

      inner = {}
      inner.gen, inner.param, inner.state = fun.chain(
        M.iter_inputs(state.queue[state.current], param.required),
        M.iter_outputs(state.queue[state.current], param.required)
      )
      state.inner = inner
    end

    inner.state, node = inner.gen(inner.param, inner.state)
    if inner.state and not fun.index(node.id, state.queue) then
      table.insert(state.queue, node.id)
      return state, node
    end

    return generator(param, state)  -- tail recursion
  end

  local param = { required = required }
  local state = { current = 0, queue = {id}, inner = nil }

  return fun.chain({M.get(id)}, fun.iter(generator, param, state))
end

function M.connect(input_id, output_id, input_required, output_required)
  assertup(type(input_id) == 'number', 'input_id must be integer')
  assertup(type(output_id) == 'number', 'output_id must be integer')
  local wire = record.Wire.create()
  wire.input_id = input_id
  wire.output_id = output_id
  wire.input_required = util.truth(input_required)
  wire.output_required = util.truth(output_required)
  local row = box.space.wire:insert(wire:to_tuple())
  return record.Wire.from_tuple(row)
end

function M.disconnect(input_id, output_id)
  assertup(type(input_id) == 'number', 'id must be integer')
  assertup(type(output_id) == 'number', 'id must be integer')
  local row = box.space.wire:delete({input_id, output_id})
  assertup(row, "No such node connection")
  return record.Wire.from_tuple(row)
end

-- Observe

function M.observe()

  local node_trigger = rxtnt.ObservableTrigger.create(function(...)
    box.space['node']:on_replace(...)
  end)
  local wire_trigger = rxtnt.ObservableTrigger.create(function(...)
    box.space['wire']:on_replace(...)
  end)

  local node_events = node_trigger:map(function(old, new)
    old = old and record.Node.from_tuple(old) or nil
    new = new and record.Node.from_tuple(new) or nil
    if not old then
      return {
        topic = 'node_added',
        node_id = new.id
      }
    elseif not new then
      return {
        topic = 'node_removed',
        node_id = old.id
      }
    elseif new.enabled ~= old.enabled then
      return {
        topic = new.enabled and 'node_enabled' or 'node_disabled',
        node_id = new.id,
      }
    else
      return {
        topic = 'node_altered',
        node_id = new.id,
      }
    end
  end)

  local wire_events = wire_trigger:map(function(old, new)
    old = old and record.Wire.from_tuple(old) or nil
    new = new and record.Wire.from_tuple(new) or nil
    if not old then
      return {
        topic = 'node_connected',
        input_id = new.input_id,
        output_id = new.output_id
      }
    elseif not new then
      return {
        topic = 'node_disconnected',
        input_id = old.input_id,
        output_id = old.output_id
      }
    end
  end)

  local events = node_events:merge(wire_events)

  events.stop = function ()
    node_trigger:stop()
    wire_trigger:stop()
  end

  return events
end

return {
  node = M
}
