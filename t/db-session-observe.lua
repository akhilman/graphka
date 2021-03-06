local app = require 't.app'
local clock = require 'clock'
local db = require 'db'
local fiber = require 'fiber'
local record = require 'record'
local rx = require 'rx'
local tap = require 'tap'
local tnt = require 't.tnt'

tnt.cfg{}
app.init{}

local test = tap.test("db.session: Observe")
test:plan(10)

local success, ret
local session

local events = {}

db.session.observe(source):subscribe(function(evt)
  table.insert(events, evt)
end)

test:is(#events, 0, 'Events is empty')

session = db.session.add(record('session').from_map({
  id = 255,
  name = 'test_session',
  peer = 'test_peer',
  ctime = clock.time()
}))
test:is(#events, 1, '1 event')

db.session.rename(session.id, 'new_name')
test:is(#events, 2, '2 event')

db.session.remove(session.id)
test:is(#events, 3, '3 event')

for _, n, topic in fun.enumerate({
  'session_added',
  'session_renamed',
  'session_removed'
}) do
  test:is(events[n].topic, topic,
    string.format('Event %s topic', topic))
  test:is(events[n].session_id, session.id,
    string.format('Event %s session_id', topic))
end

app.destroy()
tnt.finish()
test:check()
os.exit()

