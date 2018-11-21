local Record = require 'record'
local fun = require 'fun'
local rx = require 'rx'
local rxtnt = require 'rxtnt'
local utils = require 'utils'

local M = {}
local session = {}
M.session = session

function session.is_ready()
  return fun.operator.truth(box.space.sessions)
end

function session.add(session)
  assert(session._schema == 'sessions')
  box.space.sessions:insert(session:to_tuple())
end

function session.rename(id, name)
  name = name or 'unnamed'
  local row = box.space.sessions:update(box.session.id(), {
    {'=', F.sessions.name, name}
  })
  assert(row, "No such session")
end

function session.delete(id)
  row = box.space.sessions:delete(id)
  assert(row, "No such session")
end

function session.get(id)
  assert(type(id) == 'number')
  local row = box.space.sessions:get(id)
  assert(row, "No such session")
  local session Record.from_tuple('sessions', row)
  return session
end

function session.get_current()
  return session.get(box.session.id())
end

function session.iter()
  return fun.iter(box.space.sessions:pairs())
    :map(utils.partial(Record.from_tuple, 'sessions'))
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
