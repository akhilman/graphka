local M = {}

function M.fmask_to_regexp(fmask)
  local expr = fmask
  expr = string.gsub(expr, '([%%%$%^%(%)%.%-%+%*%?])', '%%%1')
  expr = string.gsub(expr, '%[(.*)%%%-(.*)%]', '[%1%-%2]')
  expr = string.gsub(expr, '%[(.*)%%([%*%?])(.*)%]', '[%1%2%3]')
  expr = string.gsub(expr, '%[!', '[^')
  expr = string.gsub(expr, '%%%*', '.*')
  expr = string.gsub(expr, '%%%?', '.')
  expr = '^' .. expr .. '$'
  return expr
end

function M.fnmatch(fname, fmask)
  if string.match(fname, M.fmask_to_regexp(fmask)) then
    return fname
  end
end

return M
