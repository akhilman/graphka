local clock = require 'clock'
local db = require 'db'
local fiber = require 'fiber'
local fun = require 'fun'
local log = require 'log'
local reqrep = require 'reqrep'
local rx = require 'rx'
local rxtnt = require 'rxtnt'
local utils = require 'utils'

local services = {}

function services.session(config, source)

  local sink = rx.Subject.create()

  local methods = {}

  function methods.list_sessions()
    local sessions
    sessions = fun.totable(
      db.iter_sessions()
        :map(function(r) return r:to_table() end)
    )
    return sessions
  end

  function methods.rename_session(name)
    db.rename_session(box.session.id(), name)
    sink:onNext({
      topic = 'session:renamed',
      session_id = box.session.id(),
      name = name,
    })
  end

  reqrep.dispatch(source, 'session:req', methods):subscribe(sink)

  if box.space['sessions']
          and not box.space['sessions']:get(box.session.id()) then
    box.space['sessions']:insert{
      box.session.id(), 'server', 'server', clock.time()
    }
  end

  local function on_connect()
    box.space['sessions']:insert{
      box.session.id(),
      'unnamed',
      box.session.peer(),
      clock.time(),
    }
    sink:onNext({
      topic = 'session:connected',
      session_id = box.session.id(),
    })
  end

  local function on_disconnect()
    db.delete_session(box.session.id())
    sink:onNext({
      topic = 'session:disconnected',
      session_id = box.session.id(),
    })
  end

  local function remove_handlers()
    box.session.on_connect(nil, on_connect)
    box.session.on_disconnect(nil, on_disconnect)
  end

  box.session.on_connect(on_connect)
  box.session.on_disconnect(on_disconnect)
  source:subscribe(rx.util.noop, remove_handlers, remove_handlers)

  return sink

end


return {
  services = services
}
