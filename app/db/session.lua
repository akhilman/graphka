local Record = require 'record' .Record
local fun = require 'fun'
local rx = require 'rx'
local utils = require 'utils'

local M = {}

function M.add_session(session)
  assert(session._scheme == 'session')
  box.space.sessions:insert(session:to_tuple())
end

function M.rename_session(id, name)
  name = name or 'unnamed'
  box.space.sessions:update(box.session.id(), {
    {'=', F.sessions.name, name}
  })
end

function M.delete_session(id)
  box.space.sessions:delete(id)
end

function M.get_session(id)
  assert(type(id) == 'number')
  local row = box.space.sessions:get(id)
  local session Record.from_tuple('sessions', row)
  return session
end

function M.iter_sessions()
  return fun.iter(box.space.sessions:pairs())
    :map(utils.partial(Record.from_tuple, 'sessions'))
end

return M
