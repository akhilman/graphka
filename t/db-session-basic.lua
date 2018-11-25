local clock = require 'clock'
local tap = require 'tap'
local tnt = require 't.tnt'
local rx = require 'rx'
local fiber = require 'fiber'
local db = require 'db'
local Record = require 'record'

tnt.cfg{}

local test = tap.test("db.session: Basic")
test:plan(14)

local success, ret
local session

-- Space is ready

ret = db.session.is_ready()
test:ok(ret, 'Session space is ready')

-- List empty

ret = db.session.iter():totable()
test:is(#ret, 0, 'Empty session list')

-- Add session

session = db.session.add(Record.from_map('session', {
  id = box.session.id(),
  name = 'test_session',
  peer = 'test_peer',
  ctime = clock.time()
}))
test:is(session.id, box.session.id(), 'New session id')
test:is(session.name, 'test_session', 'New session name')
test:is(session.peer, 'test_peer', 'New session peer')
test:isnumber(session.ctime, 'New session ctime')

-- List sessions

ret = db.session.iter():totable()
test:is(#ret, 1, 'Listed one session')
test:is_deeply(ret[1], session, 'Session in list equals new session')

-- Get session

ret = db.session.get(session.id)
test:is_deeply(ret, session, 'Get session by id')

-- Get current session

ret = db.session.get_current()
test:is_deeply(ret, session, 'Get current session')

-- Rename session

session = db.session.rename(session.id, 'new_name')
test:is(session.name, 'new_name', 'Session new name')

ret = db.session.iter():totable()
test:is(ret[1].name, 'new_name', 'New name in session list')

-- Remove session

ret = db.session.remove(session.id)
test:is_deeply(ret, session, 'Remove session')

ret = db.session.iter():totable()
test:is(#ret, 0, 'Empty session list')

tnt.finish()
test:check()
os.exit()

