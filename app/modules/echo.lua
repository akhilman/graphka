
local modules = {}

function modules.echo(config, source)

  local function reply(msg)
    return {
      to = 'api',
      req_id = msg.req_id,
      success = true,
      result = msg.data,
    }
  end

  local sink = source:map(reply)

  return sink

end

return modules
