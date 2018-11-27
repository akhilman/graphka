local util = require 'util'

local S = {}
local M = {}

for _, mod in ipairs{
  require 'services.api',
  require 'services.node',
  require 'services.session',
  require 'services.test'
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
