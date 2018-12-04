local api = require 'api'
local clock = require 'clock'
local db = require 'db'
local fun = require 'fun'
local log = require 'log'
local record = require 'record'
local rx = require 'rx'

local M = {}


--- API

local methods = {}

function methods.list_sessions(call)
  return db.session.iter():map(record('session').to_map):totable()
end

function methods.rename_session(call, name)
  db.session.rename(call.session_id, name)
end

--- Service

function M.service(config, source, scheduler)

  if not db.session.is_ready() then
    log.warn('Sessions database not ready.')
    return
  end

  local sink = rx.Subject.create()

  local conn_events = db.session.observe_connections()
  source:subscribe(rx.util.noop, conn_events.stop, conn_events.stop)

  local events = db.session.observe(source)
  source:subscribe(rx.util.noop, events.stop, events.stop)
  events:delay(0, scheduler):subscribe(sink)

  api.publish(methods, 'session', 'app', source, true):subscribe(sink)

  local success, session = pcall(db.session.get, box.session.id())
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


return M
