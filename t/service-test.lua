local tap = require 'tap'
local tnt = require 't.tnt'
local rx = require 'rx'

tnt.cfg{}

local test = tap.test("Echo module")
test:plan(4)

local success, rep

success, responce = graphka.echo('Hello world!')
test:is(success, true, 'Test echo API method success')
test:is_deeply(responce, {'Hello world!'}, 'Test echo API method responce')

success, responce = graphka.error('Hello world!')
test:is(success, false, 'Test error API method success')
test:is_deeply(responce, {'Hello world!'}, 'Test echo API method responce')

tnt.finish()
test:check()
os.exit()
