local bg
local logger
if os.getenv('FG') then
    bg = false
    logger = '| tee'
end

box = {
    listen = os.getenv("LISTEN") or "127.0.0.1:3311",
    memtx_memory = 100 * 1024 * 1024, -- 100 MB
    background = bg,
    log_level = 7,
    log = logger,
}

app = {
    migrations = './migrations',
    automigrate = false,
}
