local utils = require 'utils'

local S = {}
local M = {}

for _, mod in ipairs{
  require 'services.echo',
  require 'services.session',
  require 'services.node',
  require 'services.api'
} do
  for k, v in pairs(mod) do
    M[k] = v
  end
  for k, v in pairs(mod.services) do
    S[k] = v
  end
end

M.services = S

return M
