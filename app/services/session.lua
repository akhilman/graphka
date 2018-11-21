local Record = require 'record'
local clock = require 'clock'
local db = require 'db'
local fun = require 'fun'
local log = require 'log'
local reqrep = require 'reqrep'
local rx = require 'rx'
local rxtnt = require 'rxtnt'
local util = require 'util'

--- API

local methods = {}

function methods.list_sessions()
  local session
  session = fun.totable(
    db.session.iter():map(Record.to_table)
  )
  return session
end

function methods.rename_session(sink, name)
  db.session.rename(box.session.id(), name)
  sink:onNext({
    topic = 'session:renamed',
    session_id = box.session.id(),
    name = name,
  })
end

--- Service

local services = {}

function services.session(config, source, scheduler)

  if not db.session.is_ready() then
    log.warn('Sessions database not ready.')
    return
  end

  local sink = rx.Subject.create()
  local conn_events = db.session.observe_connections(source)

  local partial_methods = fun.iter(methods)
    :map(function(k, v) return k, util.partial(v, sink) end)
    :tomap()
  reqrep.dispatch(source, 'session:req', partial_methods):subscribe(sink)

  local success, session = pcall(db.session.get_current)
  if not success then
    xpcall(function() db.session.add(
      Record.create(
        'session', box.session.id(), 'server', '', clock.time()))
      end, log.error)
  end

  conn_events
    :filter(function(evt, id, peer) return evt == 'connected' end)
    :subscribe(function(evt, id, peer)
      db.session.add(
        Record.create('session', id, 'unnamed', peer, clock.time())) end)

  conn_events
    :filter(function(evt, id, peer) return evt == 'disconnected' end)
    :subscribe(function(evt, id, peer) db.session.delete(id) end)

  db.session.observe(source):delay(0.01, scheduler):subscribe(sink)

  return sink

end


return {
  services = services
}
