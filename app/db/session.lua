local fun = require 'fun'
local record = require 'record'
local rx = require 'rx'
local rxtnt = require 'rxtnt'
local util = require 'util'

local assertup = util.assertup

local M = {}
M = {}

function M.is_ready()
  return fun.operator.truth(box.space.session)
end

function M.add(session)
  assertup(session._schema == 'session', 'session must be session record')
  local row = box.space.session:insert(session:to_tuple())
  return record.Session.from_tuple(row)
end

function M.rename(id, name)
  assertup(type(id) == 'number', 'id must be integer')
  assertup(type(name) == 'string', 'name must be string')
  local row = box.space.session:update(id, {
    {'=', F.session.name, name}
  })
  if not row then
    return nil
  end
  return record.Session.from_tuple(row)
end

function M.remove(id)
  assertup(type(id) == 'number', 'id must be integer')
  local row = box.space.session:delete(id)
  if not row then
    return nil
  end
  return record.Session.from_tuple(row)
end

function M.get(id)
  assertup(type(id) == 'number', 'id must be integer')
  local row = box.space.session:get(id)
  if not row then
    return nil
  end
  return record.Session.from_tuple(row)
end

function M.iter()
  return fun.iter(box.space.session:pairs())
    :map(record.Session.from_tuple)
end

function M.observe_connections()

  local conn_trigger = rxtnt.ObservableTrigger.create(
    box.session.on_connect)
  local disconn_trigger = rxtnt.ObservableTrigger.create(
    box.session.on_disconnect)

  local events = rx.Observable.merge(
    conn_trigger:map(function()
      return 'connected', box.session.id(), box.session.peer() end),
    disconn_trigger:map(function()
      return 'disconnected', box.session.id(), box.session.peer() end)
  )

  events.stop = function ()
    conn_trigger:stop()
    disconn_trigger:stop()
  end

  return events
end

function M.observe()

  local trigger = rxtnt.ObservableTrigger.create(function(...)
    box.space['session']:on_replace(...)
  end)

  local events = trigger:map(function(old, new)
    old = old and record.Session.from_tuple(old) or nil
    new = new and record.Session.from_tuple(new) or nil
    if not old then
      return {
        topic = 'session_added',
        session_id = new.id
      }
    elseif not new then
      return {
        topic = 'session_removed',
        session_id = old.id
      }
    elseif new.name ~= old.name then
      return {
        topic = 'session_renamed',
        session_id = new.id,
        new_name = new.name
      }
    end
  end)

  events.stop = function()
    trigger:stop()
  end

  return events
end

return {
  session = M
}
