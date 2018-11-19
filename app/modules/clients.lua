local clock = require 'clock'
local fiber = require 'fiber'
local fun = require 'fun'
local log = require 'log'
local reqrep = require 'reqrep'
local rx = require 'rx'
local rxtnt = require 'rxtnt'
local utils = require 'utils'

local modules = {}

local function init_client(name)
  name = name or 'unnamed'
  local row = box.space.clients:insert{nil, name, clock.time()}
  return row[F.clients.client_id]
end

local function list_clients()
  local clients
  clients = fun.totable(
    fun.iter(box.space['clients']:pairs())
    :map(function(row)
      return {row[F.clients.client_id], row[F.clients.name]}
    end)
  )
  return clients
end

local function remove_client(client_id)
  local row = box.space.clients:delete(client_id)
  if not row then
    error('No such client')
  end
  return row[F.clients.client_id]
end

function modules.clients(config, source)

  local sink = rx.Subject.create()

  reqrep.dispatch(source, {
    init_client = init_client,
    remove_client = remove_client,
    list_clients = list_clients,
  }):subscribe(sink)

  return sink

end


return {
  modules = modules
}
