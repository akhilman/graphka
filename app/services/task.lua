local api = require 'api'
local clock = require 'clock'
local db = require 'db'
local fiber = require 'fiber'
local fnmatch = require 'fnmatch'
local fun = require 'fun'
local msgpack = require 'msgpack'
local record = require 'record'
local util = require 'util'

local NULL = msgpack.NULL
local assertup = util.assertup

local M = {}

-- Adds messages to task
local function fill_task(task, limit)

  local resolve_node = db.node.make_name_resolver()
  local filled_task = resolve_node(task)

  filled_task.message_id = nil

  if util.truth(task.message_id) then
    filled_task.last_message = resolve_node(
      db.message.get(task.message_id))
  else
    filled_task.last_message = NULL
  end

  filled_task.input_messages = db.message.iter(
      db.node.iter_inputs(task.node_id)
        :map(util.itemgetter('id'))
        :totable(),
      filled_task.offset,
      true
    )
    :map(resolve_node)
    :take_n(limit)
    :totable()

  return filled_task
end

-- Adds fields to task table
local function format_task(task)
  local node = db.node.get(task.node_id)
  local summary = db.message.summary(task.node_id)
  local offset = summary and summary.last_offset or node.start_offset
  local formatted_task = task:to_map()
  formatted_task.node = node.name
  formatted_task.offset = offset
  formatted_task.session = db.session.get(task.session_id):to_map()
  formatted_task.session_id = nil
  formatted_task.exprired = clock.time() > task.expires
  return formatted_task
end

-- Returns true if node is realy outdated
local function is_node_outdated(node_id)
  local offset
  local summary = db.message.summary(node_id)
  if summary and util.truth(summary.last_offset) then
    offset = summary.last_offset
  else
    offset = 0
  end
  return db.node.iter_inputs(node_id)
    :map(util.itemgetter('id'))
    :map(db.message.summary)
    :map(util.itemgetter('last_offset'))
    :filter(util.truth)
    :any(util.partial(fun.operator.lt, offset))
end

-- Sorts nodes by node_state atime
local function sort_by_atime(nodes)
  return table.sort(nodes, function(a, b)
    return db.task.get_node_state(a.id).atime
           < db.task.get_node_state(b.id).atime
  end)
end

-- Methods

local function make_methods(config, node_cond, task_cond)

  -- Selects nodes by masks with deadline
  local function select_nodes(node_masks, outdated, deadline)
    node_masks = type(node_masks) == 'table' and node_masks or { node_masks }
    assertup(
      #node_masks > 0
      and fun.iter(node_masks)
        :map(type)
        :all(util.partial(fun.operator.eq, 'string')),
      'node_masks should be string or list of strings'
    )
    local nodes
    repeat
      nodes = db.node.iter()
        :filter(util.itemgetter('enabled'))
        :filter(function(node)
          return fun.iter(node_masks)
            :any(util.partial(fnmatch.fnmatch, node.name))
          end)
      if outdated then
        nodes = nodes:filter(function(node)
            return db.task.get_node_state(node.id).outdated
          end)
      end
      nodes = nodes:totable()
    until #nodes ~= 0
        or not node_cond:wait(math.max(0, deadline - clock.time()))
    return nodes
  end

  -- Removes task from database
  local function release(task)
    assertup(task._schema == 'task', 'task should be task record')
    local res = db.task.remove(task.id)
    assertup(res, string.format(
      'Task #%d not registered in database', task.id))
  end

  -- Adds task to database for any provided node
  local function acquire(nodes, session_id, task_lifetime, deadline)
    local task
    local state
    local summary
    local message_id
    local offset
    local ok, result
    repeat
      for _, node in ipairs(nodes) do
        if db.task.exist_for_node(node.id) then
          ok = false
        else
          state = db.task.get_node_state(node.id)
          summary = db.message.summary(node.id)
          if summary and util.truth(summary.last_id) then
            message_id = summary.last_id
            offset = summary.last_offset
          else
            message_id = NULL
            offset = node.start_offset
          end
          ok, result = pcall(
            db.task.add,
            record.Task.from_map({
              node_id = node.id,
              session_id = session_id,
              message_id = message_id,
              offset = offset,
              expires = clock.time() + task_lifetime,
            })
          )
        end
        if ok then
          log.debug(string.format('Node "%s" acquired', node.name))
          task = result
          break
        end
      end
    until task or not task_cond:wait(math.max(0, deadline - clock.time()))
    if task then
      db.task.set_node_state(task.node_id, {
          atime = clock.time(),
          outdated = false
        })
    end
    return task
  end

  -- API methods
  local methods = {}

  local function take_task(last, call, node_masks, limit, timeout)
    local nodes
    local task
    local filled_task
    local deadline
    local task_lifetime = config.task_lifetime
    limit = limit or config.messages_per_task
    limit = math.min(limit, config.messages_per_task)
    timeout = timeout or config.timeout
    timeout = math.min(timeout, config.timeout)
    deadline = clock.time() + timeout
    nodes = select_nodes(node_masks, not last, deadline)
    if #nodes == 0 then
      return nil
    end
    sort_by_atime(nodes)
    task = acquire(nodes, call.session_id, task_lifetime, deadline)
    if not task then
      return nil
    end
    filled_task = fill_task(task, limit)
    return filled_task
  end

  methods.take_last = util.partial(take_task, true)
  methods.take_task = util.partial(take_task, false)

  function methods.touch_task(call, task_id, timeout)
    timeout = util.truth(timeout) and timeout or config.timeout
    assert(type(timeout) == 'number', 'timeout should be number or nil')
    db.task.set_expires(task_id, clock.time() + timeout)
  end

  function methods.release_task(call, task_id)
    local task = db.task.get(task_id)
    assert(task, string.format('No such task #%d', task_id))
    local summary = db.message.summary(task.node_id)
    if task.message_id ~= summary.last_id
        and is_node_outdated(task.node_id) then
      db.task.set_node_state(task.node_id, {
        atime = clock.time(),
        outdated = true
      })
    end
    release(task)
  end

  function methods.list_tasks(call)
    return db.task.iter():map(format_task):totable()
  end

  function methods.task_summary(call)
    local nodes = db.node.iter():totable()
    return fun.zip(
      fun.iter(nodes)
        :map(util.itemgetter('name')),
      fun.iter(nodes)
        :map(function(node)
            local task = db.task.get_by_node(node.id)
            local state = db.task.get_node_state(node.id)
            return {
              task = task and format_task(task) or NULL,
              atime = state and state.atime or NULL,
              outdated = state and state.outdated or false
            }
          end)
    ):tomap()
  end

  return methods
end

--- Purge tasks

local function purge_loop(config, control_chan)

  local interval = config.purge_interval

  local cmd = control_chan:get(interval)
  if cmd == 'stop' then
    return
  end

  local n_removed = db.task.iter_expired(clock.time())
    :map(util.itemgetter('id'))
    :map(db.task.remove)
    :length()
  log.verbose(string.format("%d expired tasks removed", n_removed))

  local n_removed = db.task.iter_node_states()
    :map(util.itemgetter('node_id'))
    :filter(function(id) return not db.node.get(id) end)
    :map(db.task.clear_node_state)
    :length()
  log.verbose(string.format("%d orphaned node states removed", n_removed))

  return purge_loop(config, control_chan)
end

function M.service(config, source, scheduler)

  if not db.task.is_ready() then
    log.warn('Task database not ready.')
    return
  end

  local sink = rx.Subject.create()

  local node_cond = fiber.cond()
  local task_cond = fiber.cond()

  -- Events
  local events = db.task.observe()
  source:filter(util.itemeq('topic', 'stop')):subscribe(events.stop)
  events:delay(0, scheduler):subscribe(sink)

  -- Remove task on session end
  source
    :filter(util.itemeq('topic', 'session_removed'))
    :subscribe(function(msg)
        db.task.iter_by_session_id(msg.session_id)
          :map(util.itemgetter('id'))
          :map(db.task.remove)
          :map(util.itemgetter('node_id'))
          :filter(is_node_outdated)
          :each(util.revpartial(db.task.set_node_state, { outdated = true }))
      end)

  -- Wakeup on new node
  source
    :filter(util.itemeq('topic', 'node_added'))
    :subscribe(function() node_cond:broadcast() end)

  -- Wakeup on node enabled
  source
    :filter(util.itemeq('topic', 'node_enabled'))
    :subscribe(function() node_cond:broadcast() end)

  -- Wakeup on node outdated
  source
    :filter(util.itemeq('topic', 'node_outdated'))
    :subscribe(function() node_cond:broadcast() end)

  -- Wakeup on task removed
  source
    :filter(util.itemeq('topic', 'task_removed'))
    :subscribe(function() task_cond:broadcast() end)

  -- Mark outdated on new message
  source
    :filter(util.itemeq('topic', 'message_added'))
    :subscribe(function(msg)
        local count = db.node.iter_outputs(msg.node_id)
          :map(util.itemgetter('id'))
          :map(util.revpartial(db.task.set_node_state, { outdated = true }))
          :length()
        log.debug(string.format(
            '%d nodes marketd outdated by new message in node %d',
            count, msg.node_id
          ))
      end)

  -- Mark outdated on new wire
  source
    :filter(util.itemeq('topic', 'node_connected'))
    :map(util.itemgetter('output_id'))
    :filter(is_node_outdated)
    :subscribe(function(node_id)
        local state = db.task.set_node_state(node_id, {outdated = true})
        if state then
          log.debug(string.format(
              'Node %d marketd outdated by new input connection',
              node_id
            ))
        end
      end)

  -- Expired tasks cleaner
  local purge_ctrl = fiber.channel()
  fiber.create(purge_loop, config, purge_ctrl)
  source:filter(function(msg)
      return fun.index(msg.topic, {'purge', 'stop'})
    end)
    :subscribe(function(msg) purge_ctrl:put(msg.topic) end)

  -- API
  api.publish(
      make_methods(config, node_cond, task_cond),
      'task', 'app', source, true
    ):subscribe(sink)

  return sink
end


return M
