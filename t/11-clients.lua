local tap = require 'tap'
local tnt = require 't.tnt'
local rx = require 'rx'

tnt.cfg{}
local app = require 'app'
app.init{}
box.spacer:migrate_up()

local test = tap.test("Clients module")
test:plan(11)

local success, rep

success, responce = graphka.list_clients()
test:is(success, true, 'Test list_clients API method success')
test:is_deeply(responce, {}, 'Test list_clients API method responce')

success, responce = graphka.init_client('name')
test:is(success, true, 'Test init_client API method success')
test:is_deeply(responce, 1, 'Test init_client API method responce')

success, responce = graphka.list_clients()
test:is(success, true, 'Test list_clients API method success')
test:is_deeply(responce, {{1, 'name'}}, 'Test list_clients API method responce')

success, responce = graphka.remove_client(1)
test:is(success, true, 'Test remove_client API method success')
test:is_deeply(responce, 1, 'Test remove_client API method responce')

success, responce = graphka.list_clients()
test:is(success, true, 'Test list_clients API method success')
test:is_deeply(responce, {}, 'Test list_clients API method responce')

success, responce = graphka.remove_client(1)
test:is(success, false, 'Test remove_client API method success')

tnt.finish()
test:check()
os.exit()
