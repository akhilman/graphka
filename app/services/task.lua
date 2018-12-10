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

-- Methods

local function make_methods(config, node_cond, offset_cond, task_cond)

  -- Returns true if any node's input have greater offset
  local function is_node_outdated(node)
    local offset = db.task.get_node_state(node.id).offset
    return db.node.iter_inputs(node.id)
      :map(util.itemgetter('id'))
      :map(db.message.summary)
      :map(util.itemgetter('last_offset'))
      :any(util.partial(fun.operator.lt, offset))
  end

  -- Sorts nodes by node_state atime
  local function sort_by_atime(nodes)
    return table.sort(nodes, function(a, b)
      return db.task.get_node_state(a.id).atime
             < db.task.get_node_state(b.id).atime
    end)
  end

  -- Selects nodes by masks with deadline
  local function select_nodes(node_masks, deadline)
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
      nodes = db.node.iter():filter(function(node)
          return fun.iter(node_masks)
            :any(util.partial(fnmatch.fnmatch, node.name))
          end)
        :totable()
    until #nodes ~= 0
        or not node_cond:wait(math.max(0, deadline - clock.time()))
    return nodes
  end

  -- Adds task to databse for this node with deadline
  local function acquire(nodes, session_id, timeout, deadline)
    local task
    local ok, result
    repeat
      for _, node in ipairs(nodes) do
        if db.task.exist_for_node(node.id) then
          ok = false
        else
          ok, result = pcall(
            db.task.add,
            record.Task.from_map({
              node_id = node.id,
              session_id = session_id,
              offset = 0,
              expires = clock.time() + timeout,
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
      db.task.touch_node_state(task.node_id)
    end
    return task
  end

  -- Save as acquire but only when node outdated
  local function acquire_outdated(nodes, session_id, timeout, deadline)
    local task
    local outdated
    repeat
      outdated = fun.iter(nodes):filter(is_node_outdated):totable()
      if #outdated ~= 0 then
        task = acquire(outdated, session_id, timeout, deadline)
        if task and not is_node_outdated(
            fun.iter(outdated)
              :filter(util.itemeq('id', task.node_id))
              :nth(1)
            ) then
            task = nil
        end
      end
    until task or not offset_cond:wait(math.max(0, deadline - clock.time()))
    return task
  end

  -- Adds fields to task table
  local function format_task(task)
    local formatted_task = task:to_map()
    formatted_task.node = db.node.get(task.node_id).name
    formatted_task.offset = db.task.get_node_state(task.node_id).offset
    formatted_task.session = db.session.get(task.session_id):to_map()
    formatted_task.session_id = nil
    formatted_task.exprired = clock.time() > task.expires
    return formatted_task
  end

  -- Adds messages to task
  local function fill_task(task, limit)

    local offset = db.task.get_node_state(task.node_id).offset
    local summary = db.message.summary(task.node_id)

    local resolve_node = db.node.make_name_resolver()

    local filled_task = resolve_node(task)

    filled_task.last_message = (
      util.truth(summary.last_id)
      and resolve_node(db.message.get(summary.last_id))
      or NULL
    )
    filled_task.input_messages = db.message.iter(
        db.node.iter_inputs(task.node_id)
          :map(util.itemgetter('id'))
          :totable(),
        offset,
        true
      )
      :map(resolve_node)
      :take_n(limit)
      :totable()

    if #filled_task.input_messages ~= 0 then
      offset = math.max(
        offset,
        filled_task.input_messages[#filled_task.input_messages].offset
      )
    end

    if util.truth(filled_task.last_message) then
      offset = math.max(offset, filled_task.last_message.offset)
    end


    filled_task.offset = offset

    return filled_task
  end

  -- API methods
  local methods = {}

  function methods.take_last(call, node_masks, limit, timeout)
    local nodes
    local task
    local filled_task
    local deadline
    limit = limit or config.messages_per_task
    limit = math.min(limit, config.messages_per_task)
    timeout = timeout or config.timeout
    timeout = math.min(timeout, config.timeout)
    deadline = clock.time() + timeout
    nodes = select_nodes(node_masks, deadline)
    if #nodes == 0 then
      return nil
    end
    sort_by_atime(nodes)
    task = acquire(nodes, call.session_id, timeout, deadline)
    if not task then
      return nil
    end
    filled_task = fill_task(task, limit)
    db.task.set_offset(task.id, filled_task.offset)
    return filled_task
  end

  function methods.take_task(call, node_masks, limit, timeout)
    local nodes
    local task
    local filled_task
    local deadline
    limit = limit or config.messages_per_task
    limit = math.min(limit, config.messages_per_task)
    timeout = timeout or config.timeout
    timeout = math.min(timeout, config.timeout)
    deadline = clock.time() + timeout
    nodes = select_nodes(node_masks, deadline)
    sort_by_atime(nodes)
    task = acquire_outdated(
        nodes, call.session_id, timeout, deadline)
    if not task then
      return nil
    end
    filled_task = fill_task(task, limit)
    db.task.set_offset(task.id, filled_task.offset)
    return filled_task
  end

  function methods.touch_task(call, task_id, timeout)
    timeout = util.truth(timeout) and timeout or config.timeout
    assert(type(timeout) == 'number', 'timeout should be number or nil')
    db.task.set_expires(task_id, clock.time() + timeout)
  end

  function methods.ack_task(call, task_id, offset)
    local task = db.task.get(task_id)
    assert(task, string.format('No such task #%d', task_id))
    offset = util.truth(offset) and offset or task.offset
    db.task.set_node_state(task.node_id, offset)
    db.task.remove(task.id)
  end

  function methods.release_task(call, task_id)
    local task = db.task.get(task_id)
    assert(task, string.format('No such task #%d', task_id))
    db.task.touch_node_state(task.node_id)
    db.task.remove(task.id)
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
            local outdated = is_node_outdated(node)
            return {
              task = task and format_task(task) or NULL,
              offset = state and state.offset or NULL,
              atime = state and state.atime or NULL,
              outdated = outdated,
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
  log.verbose(string.format("%d orphaned offset removed", n_removed))

  return purge_loop(config, control_chan)
end

function M.service(config, source, scheduler)

  if not db.task.is_ready() then
    log.warn('Task database not ready.')
    return
  end

  local sink = rx.Subject.create()

  local node_cond = fiber.cond()
  local offset_cond = fiber.cond()
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
          :each(db.task.remove)
      end)

  -- Wakeup on new node
  source
    :filter(util.itemeq('topic', 'node_added'))
    :subscribe(function() node_cond:broadcast() end)

  -- Wakeup on new offset
  source
    :filter(util.itemeq('topic', 'offset_reached'))
    :subscribe(function() offset_cond:broadcast() end)

  -- Wakeup on task removed
  source
    :filter(util.itemeq('topic', 'task_removed'))
    :subscribe(function() task_cond:broadcast() end)

  -- Expired tasks cleaner
  local purge_ctrl = fiber.channel()
  fiber.create(purge_loop, config, purge_ctrl)
  source:filter(function(msg)
      return fun.index(msg.topic, {'purge', 'stop'})
    end)
    :subscribe(function(msg) purge_ctrl:put(msg.topic) end)

  -- API
  api.publish(
      make_methods(config, node_cond, offset_cond, task_cond),
      'task', 'app', source, true
    ):subscribe(sink)

  return sink
end


return M
