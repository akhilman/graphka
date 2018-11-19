local tap = require 'tap'
local tnt = require 't.tnt'

tnt.cfg{}
local app = require 'app'
app.init{}

local test = tap.test("Echo module")
test:plan(2)

local success, rep
success, responce = echo('Hello world!')
test:is(success, true, 'Test echo API method success')
test:is(responce, 'Hello world!', 'Test echo API method responce')

tnt.finish()
test:check()
os.exit()
