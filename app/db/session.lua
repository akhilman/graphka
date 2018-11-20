local Record = require 'record'
local fun = require 'fun'
local rx = require 'rx'
local rxtnt = require 'rxtnt'
local utils = require 'utils'

local M = {}

function M.is_sessions_db_ready()
  return fun.operator.truth(box.space.sessions)
end

function M.add_session(session)
  assert(session._schema == 'sessions')
  box.space.sessions:insert(session:to_tuple())
end

function M.rename_session(id, name)
  name = name or 'unnamed'
  local row = box.space.sessions:update(box.session.id(), {
    {'=', F.sessions.name, name}
  })
  assert(row, "No such session")
end

function M.delete_session(id)
  row = box.space.sessions:delete(id)
  assert(row, "No such session")
end

function M.get_session(id)
  assert(type(id) == 'number')
  local row = box.space.sessions:get(id)
  assert(row, "No such session")
  local session Record.from_tuple('sessions', row)
  return session
end

function M.get_current_session()
  return M.get_session(box.session.id())
end

function M.iter_sessions()
  return fun.iter(box.space.sessions:pairs())
    :map(utils.partial(Record.from_tuple, 'sessions'))
end

function M.observe_connections(source)
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
