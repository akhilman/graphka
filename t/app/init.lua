local app = require 'app'
local fio = require 'fio'
local util = require 'util'

local dir = os.getenv('TNT_FOLDER')
local cleanup = false

if dir == nil then
  dir = fio.tempdir()
  cleanup = true
end

local migrations = fio.pathjoin(dir, 'migrations')

local test_app = {}

test_app.__index = app

function test_app.init(cfg)
  cfg = cfg or {}
  cfg = util.merge_tables(
    cfg,
    { migrations = migrations }
  )
  app.init(cfg)
  box.spacer:makemigration('init')
  box.spacer:migrate_up()
  app.destroy()

  app.init(cfg)
end

function test_app.destroy()
  app.destroy()
  if cleanup then
    local files = util.concatenate(
      fio.glob(fio.pathjoin(migrations, '*')),
      fio.glob(fio.pathjoin(dir, '*'))
    )
    for _, file in pairs(files) do
      fio.unlink(file)
    end
    log.info("rmtree %s", dir)
    local ret = fio.rmtree(dir)
  end
end

return setmetatable(test_app, app)
