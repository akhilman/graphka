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
  box.space.session:insert(session:to_tuple())
end

function session.rename(id, name)
  assert(type(id) == 'number', 'id must be integer')
  assert(type(name) == 'string', 'name must be string')
  local row = box.space.session:update(box.session.id(), {
    {'=', F.session.name, name}
  })
  assert(row, "No such session")
end

function session.delete(id)
  assert(type(id) == 'number', 'id must be integer')
  row = box.space.session:delete(id)
  assert(row, "No such session")
end

function session.get(id)
  assert(type(id) == 'number', 'id must be integer')
  local row = box.space.session:get(id)
  assert(row, "No such session")
  local session Record.from_tuple('session', row)
  return session
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

return M
