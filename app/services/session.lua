local api = require 'api'
local clock = require 'clock'
local db = require 'db'
local fun = require 'fun'
local log = require 'log'
local record = require 'record'
local rx = require 'rx'

--- API

local methods = {}

function methods.list_sessions()
  return db.session.iter():map(record('session').to_map):totable()
end

function methods.rename_session(name)
  db.session.rename(box.session.id(), name)
end

--- Service

local services = {}

function services.session(config, source, scheduler)

  if not db.session.is_ready() then
    log.warn('Sessions database not ready.')
    return
  end

  local sink = rx.Subject.create()

  local conn_events = db.session.observe_connections()
  source:subscribe(rx.util.noop, conn_events.stop, conn_events.stop)

  local events = db.session.observe(source)
  source:subscribe(rx.util.noop, events.stop, events.stop)
  events:delay(0.01, scheduler):subscribe(sink)

  api.publish(methods, 'session', 'api', source):subscribe(sink)

  local success, session = pcall(db.session.get_current)
  if not success then
    xpcall(function() db.session.add(
      record('session').create(box.session.id(), 'server', '', clock.time()))
    end, log.error)
  end

  conn_events
    :filter(function(evt, id, peer) return evt == 'connected' end)
    :subscribe(function(evt, id, peer)
      db.session.add(
        record('session').create(id, 'unnamed', peer, clock.time())) end)

  conn_events
    :filter(function(evt, id, peer) return evt == 'disconnected' end)
    :subscribe(function(evt, id, peer) db.session.remove(id) end)

  return sink

end


return {
  services = services
}
