local app = require 't.app'
local fiber = require 'fiber'
local rx = require 'rx'
local tap = require 'tap'
local tnt = require 't.tnt'

tnt.cfg{}
app.init{}

local test = tap.test("Echo module")
test:plan(4)

local success, rep

success, responce = graphka.echo('Hello world!')
test:is(success, true, 'Test echo API method success')
test:is_deeply(responce, {'Hello world!'}, 'Test echo API method responce')

success, responce = graphka.error('Hello world!')
test:is(success, false, 'Test error API method success')

graphka.unprotected_error('Error')
fiber.sleep(1.5)
success, responce = graphka.echo('Hello world!')
test:is(success, true, 'Service restarted after error')

app.destroy()
tnt.finish()
test:check()
os.exit()
