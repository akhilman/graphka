local clock = require 'clock'
local db = require 'db'
local fiber = require 'fiber'
local record = require 'record'
local rx = require 'rx'
local tap = require 'tap'
local tnt = require 't.tnt'

tnt.cfg{}

local test = tap.test("db.session: Basic")
test:plan(17)

local success, ret
local session

-- Space is ready

ret = db.session.is_ready()
test:ok(ret, 'Session space is ready')

-- List server session

ret = db.session.iter():totable()
test:is(#ret, 1, 'Server session in list')
test:is(ret[1].id, box.session.id(), 'Current session id')
test:is(ret[1].name, 'server', 'Current session name')

-- Add session

session = db.session.add(record('session').from_map({
  id = 255,
  name = 'test_session',
  peer = 'test_peer',
  ctime = clock.time()
}))
test:is(session.id, 255, 'New session id')
test:is(session.name, 'test_session', 'New session name')
test:is(session.peer, 'test_peer', 'New session peer')
test:isnumber(session.ctime, 'New session ctime')

-- List sessions

ret = db.session.iter():totable()
test:is(#ret, 2, 'Listed two session')
test:is_deeply(ret[2], session, 'Test session in list equals new session')

-- Get session

ret = db.session.get(session.id)
test:is_deeply(ret, session, 'Get session by id')

-- Get current session

ret = db.session.get_current()
test:is(ret.id, box.session.id(), 'Current session id')
test:is(ret.name, 'server', 'Current session name')

-- Rename session

session = db.session.rename(session.id, 'new_name')
test:is(session.name, 'new_name', 'Session new name')

ret = db.session.iter():totable()
test:is(ret[2].name, 'new_name', 'New name in session list')

-- Remove session

ret = db.session.remove(session.id)
test:is_deeply(ret, session, 'Remove session')

ret = db.session.iter():totable()
test:is(#ret, 1, 'Empty session list')

tnt.finish()
test:check()
os.exit()

