local Record = require 'record'
local fun = require 'fun'
local rx = require 'rx'
local rxtnt = require 'rxtnt'
local util = require 'util'

local M = {}
local session = {}
M.session = session

function session.is_ready()
  return fun.operator.truth(box.space.session)
end

function session.add(session)
  assert(session._schema == 'session', 'session must be session record')
  local row = box.space.session:insert(session:to_tuple())
  return Record.from_tuple('session', row)
end

function session.rename(id, name)
  assert(type(id) == 'number', 'id must be integer')
  assert(type(name) == 'string', 'name must be string')
  local row = box.space.session:update(box.session.id(), {
    {'=', F.session.name, name}
  })
  assert(row, "No such session")
  return Record.from_tuple('session', row)
end

function session.remove(id)
  assert(type(id) == 'number', 'id must be integer')
  local row = box.space.session:delete(id)
  assert(row, "No such session")
  return Record.from_tuple('session', row)
end

function session.get(id)
  assert(type(id) == 'number', 'id must be integer')
  local row = box.space.session:get(id)
  assert(row, "No such session")
  return Record.from_tuple('session', row)
end

function session.get_current()
  return session.get(box.session.id())
end

function session.iter()
  return fun.iter(box.space.session:pairs())
    :map(util.partial(Record.from_tuple, 'session'))
end

function session.observe_connections(source)

  local conn = rxtnt.ObservableTrigger.create(box.session.on_connect)
  local disconn = rxtnt.ObservableTrigger.create(box.session.on_disconnect)

  if source then
    local function stop()
      conn:stop()
      disconn:stop()
    end
    source:subscribe(rx.util.noop, stop, stop)
  end

  return rx.Observable.merge(
    conn:map(function()
      return 'connected', box.session.id(), box.session.peer() end),
    disconn:map(function()
      return 'disconnected', box.session.id(), box.session.peer() end)
  )
end

function session.observe(source)

  local db_events = rxtnt.ObservableTrigger.create(function(...)
    box.space['session']:on_replace(...)
  end)

  if source then
    --- stop observable on source's onComplete
    local function stop()
      db_events:stop()
    end
    source:subscribe(rx.util.noop, stop, stop)
  end

  local events = db_events:map(function(old, new)
    old = old and Record.from_tuple('session', old) or nil
    new = new and Record.from_tuple('session', new) or nil
    if not old then
      return {
        topic = 'session:added',
        session_id = new.id
      }
    elseif not new then
      return {
        topic = 'session:removed',
        session_id = old.id
      }
    elseif new.name ~= old.name then
      return {
        topic = 'session:renamed',
        session_id = new.id,
        new_name = new.name
      }
    end
  end)

  return events
end

return M
