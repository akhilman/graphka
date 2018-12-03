local app = require 'app'
local fiber = require 'fiber'
local fio = require 'fio'
local util = require 'util'

local dir = os.getenv('TNT_FOLDER')
local cleanup = false

if dir == nil then
  dir = fio.tempdir()
  cleanup = true
end

local migrations = fio.pathjoin(dir, 'migrations')

local real_init = app.init
local real_destroy = app.destroy

function app.init(cfg)
  cfg = cfg or {}
  cfg = util.merge_tables(
    cfg,
    { migrations = migrations }
  )

  real_init(cfg)
  box.spacer:makemigration('init')
  box.spacer:migrate_up()
  real_destroy()

  real_init(cfg)
end

function app.destroy()
  real_destroy()
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

return app
