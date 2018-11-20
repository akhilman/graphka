local Record = require 'record'
local clock = require 'clock'
local db = require 'db'
local fun = require 'fun'
local log = require 'log'
local reqrep = require 'reqrep'
local rx = require 'rx'
local utils = require 'utils'

--- API

local methods = {}

function methods.list_sessions()
  local sessions
  sessions = fun.totable(
    db.iter_sessions():map(Record.to_table)
  )
  return sessions
end

function methods.rename_session(sink, name)
  db.rename_session(box.session.id(), name)
  sink:onNext({
    topic = 'session:renamed',
    session_id = box.session.id(),
    name = name,
  })
end

--- Service

local services = {}

function services.session(config, source)

  if not db.is_sessions_db_ready() then
    log.warn('Sessions database not ready.')
    return
  end

  local sink = rx.Subject.create()
  local events = db.observe_connections(source)

  local partial_methods = fun.iter(methods)
    :map(function(k, v) return k, utils.partial(v, sink) end)
    :tomap()
  reqrep.dispatch(source, 'session:req', partial_methods):subscribe(sink)

  local success, session = pcall(db.get_current_session)
  if not success then
    xpcall(function() db.add_session(
      Record.create(
        'sessions', box.session.id(), 'server', '', clock.time()))
      end, log.error)
  end

  events
    :filter(function(evt, id, peer) return evt == 'connected' end)
    :subscribe(function(evt, id, peer)
      db.add_session(
        Record.create('sessions', id, 'unnamed', peer, clock.time())) end)

  events
    :filter(function(evt, id, peer) return evt == 'disconnected' end)
    :subscribe(function(evt, id, peer) db.delete_session(id) end)

  events
    :map(function(evt, id, peer) return {
      topic = 'session:' .. evt,
      session_id = id,
      peer = peer,
    } end)
    :subscribe(sink)

  return sink

end


return {
  services = services
}
