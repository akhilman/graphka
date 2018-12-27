local app = require 't.app'
local fiber = require 'fiber'
local rx = require 'rx'
local tap = require 'tap'
local tnt = require 't.tnt'

tnt.cfg{}
app.init{ timeout = 1, enable_test_service = true}

local test = tap.test("Echo module")
test:plan(4)

local success, rep

success, responce = app.echo('Hello world!')
test:is(success, true, 'Test echo API method success')
test:is_deeply(responce, {'Hello world!'}, 'Test echo API method responce')

success, responce = app.error('Hello world!')
test:is(success, false, 'Test error API method success')

success, responce = app.delay(10)
test:is(success, false, 'Test API timeout error')

app.destroy()
tnt.finish()
test:check()
os.exit()
