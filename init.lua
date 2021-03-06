-- 框架入口
require "luarocks.loader"
require "resty.core"
local constants = require "kong.constants"

do
  -- let's ensure the required shared dictionaries are
  -- declared via lua_shared_dict in the Nginx conf

  for _, dict in ipairs(constants.DICTS) do
    if not ngx.shared[dict] then
      return error("missing shared dict '" .. dict .. "' in Nginx "          ..
                   "configuration, are you using a custom template? "        ..
                   "Make sure the 'lua_shared_dict " .. dict .. " [SIZE];' " ..
                   "directive is defined.")
    end
  end

  -- if we're running `nginx -t` then don't initialize
  if os.getenv("KONG_NGINX_CONF_CHECK") then
    return {
      init = function() end,
    }
  end
end

require("kong.globalpatches")()


local kong_global = require "kong.global"
local PHASES = kong_global.phases


_G.kong = kong_global.new() -- no versioned PDK for plugins for now


local DB = require "kong.db"
local dns = require "kong.tools.dns"
local utils = require "kong.tools.utils"
local lapis = require "lapis"
local runloop = require "kong.runloop.handler"
local responses = require "kong.tools.responses"
local singletons = require "kong.singletons"
local DAOFactory = require "kong.dao.factory"
local kong_cache = require "kong.cache"
local ngx_balancer = require "ngx.balancer"
local kong_resty_ctx = require "kong.resty.ctx"
local plugins_iterator = require "kong.runloop.plugins_iterator"
local balancer_execute = require("kong.runloop.balancer").execute
local kong_cluster_events = require "kong.cluster_events"
local kong_error_handlers = require "kong.error_handlers"

local ngx              = ngx
local header           = ngx.header
local ngx_log          = ngx.log
local ngx_ERR          = ngx.ERR
local ngx_CRIT         = ngx.CRIT
local ngx_DEBUG        = ngx.DEBUG
local ipairs           = ipairs
local assert           = assert
local tostring         = tostring
local coroutine        = coroutine
local get_last_failure = ngx_balancer.get_last_failure
local set_current_peer = ngx_balancer.set_current_peer
local set_timeouts     = ngx_balancer.set_timeouts
local set_more_tries   = ngx_balancer.set_more_tries

local loaded_plugins

local function load_plugins(kong_conf, dao)
  local in_db_plugins, sorted_plugins = {}, {}

  ngx_log(ngx_DEBUG, "Discovering used plugins")

  local rows, err_t = dao.plugins:find_all()
  if not rows then
    return nil, tostring(err_t)
  end

  for _, row in ipairs(rows) do in_db_plugins[row.name] = true end

  -- check all plugins in DB are enabled/installed
  for plugin in pairs(in_db_plugins) do
    if not kong_conf.loaded_plugins[plugin] then
      return nil, plugin .. " plugin is in use but not enabled"
    end
  end

  -- load installed plugins
  for plugin in pairs(kong_conf.loaded_plugins) do
    if constants.DEPRECATED_PLUGINS[plugin] then
      ngx.log(ngx.WARN, "plugin '", plugin, "' has been deprecated")
    end

    -- NOTE: no version _G.kong (nor PDK) in plugins main chunk

    local ok, handler = utils.load_module_if_exists("kong.plugins." .. plugin .. ".handler")
    if not ok then
      return nil, plugin .. " plugin is enabled but not installed;\n" .. handler
    end

    local ok, schema = utils.load_module_if_exists("kong.plugins." .. plugin .. ".schema")
    if not ok then
      return nil, "no configuration schema found for plugin: " .. plugin
    end

    ngx_log(ngx_DEBUG, "Loading plugin: " .. plugin)

    sorted_plugins[#sorted_plugins+1] = {
      name = plugin,
      handler = handler(),
      schema = schema
    }
  end

  -- sort plugins by order of execution
  -- 每个插件中都会指定自己的权重
  -- 如限流插件中：RateLimitingHandler.PRIORITY = 901
  -- 这里按权重重新排序
  table.sort(sorted_plugins, function(a, b)
    local priority_a = a.handler.PRIORITY or 0
    local priority_b = b.handler.PRIORITY or 0
    return priority_a > priority_b
  end)

  -- add reports plugin if not disabled
  if kong_conf.anonymous_reports then
    local reports = require "kong.reports"

    local db_infos = dao:infos()
    reports.add_ping_value("database", kong_conf.database)
    reports.add_ping_value("database_version", db_infos.version)

    reports.toggle(true)

    sorted_plugins[#sorted_plugins+1] = {
      name = "reports",
      handler = reports,
      schema = {},
    }
  end

  return sorted_plugins
end


-- Kong public context handlers.
-- @section kong_handlers

local Kong = {}

-- init_by_lua_block
-- 用来完成耗时长的模块加载
-- 或者初始化一些全局常量
function Kong.init()
  local pl_path = require "pl.path"
  local conf_loader = require "kong.conf_loader"
  local ip = require "kong.tools.ip"

  -- check if kong global is the correct one
  if not kong.version then
    error("configuration error: make sure your template is not setting a " ..
          "global named 'kong' (please use 'Kong' instead)")
  end

  -- retrieve kong_config
  local conf_path = pl_path.join(ngx.config.prefix(), ".kong_env")
  local config = assert(conf_loader(conf_path))

  kong_global.init_pdk(kong, config, nil) -- nil: latest PDK

  local db = assert(DB.new(config))
  assert(db:init_connector())

  local dao = assert(DAOFactory.new(config, db)) -- instantiate long-lived DAO
  local ok, err_t = dao:init()
  if not ok then
    error(tostring(err_t))
  end

  assert(dao:are_migrations_uptodate())

  db.old_dao = dao

  loaded_plugins = assert(load_plugins(config, dao))

  assert(runloop.build_router(db, "init"))
  assert(runloop.build_api_router(dao, "init"))

  -- LEGACY
  singletons.ip = ip.init(config)
  singletons.dns = dns(config)
  singletons.dao = dao
  singletons.configuration = config
  singletons.db = db
  -- /LEGACY

  kong.dao = dao
  kong.db = db
  kong.dns = singletons.dns
end

-- init_worker_by_lua_block
-- 用于定时拉取配置/数据（启动一些定时任务）
-- 有几个Nginx工作进程就有几个定时任务
function Kong.init_worker()
  kong_global.set_phase(kong, PHASES.init_worker)

  -- special math.randomseed from kong.globalpatches
  -- not taking any argument. Must be called only once
  -- and in the init_worker phase, to avoid duplicated
  -- seeds.
  math.randomseed()

  -- init DAO


  local ok, err = kong.dao:init_worker()
  if not ok then
    ngx_log(ngx_CRIT, "could not init DB: ", err)
    return
  end


  -- init inter-worker events


  local worker_events = require "resty.worker.events"


  local ok, err = worker_events.configure {
    shm = "kong_process_events", -- defined by "lua_shared_dict"
    timeout = 5,            -- life time of event data in shm
    interval = 1,           -- poll interval (seconds)

    wait_interval = 0.010,  -- wait before retry fetching event data
    wait_max = 0.5,         -- max wait time before discarding event
  }
  if not ok then
    ngx_log(ngx_CRIT, "could not start inter-worker events: ", err)
    return
  end


  -- init cluster_events


  local cluster_events, err = kong_cluster_events.new {
    dao                     = kong.dao,
    -- db_update_frequency ：该配置决定 kong 节点从数据库拉取缓存无效事件，
    -- 同步任务执行的频率。一个更小的值意味着同步任务将会更频繁的执行，
    -- kong 节点的缓存数据将保持和数据库更强的一致性。
    -- 一个更大的值，意味着你的 kong 节点花更少的时间去处理同步任务，
    -- 从而更加将更多资源集中去处理请求。
    poll_interval           = kong.configuration.db_update_frequency,
    -- db_update_propagation ：如果你的数据库也是集群的并且最终一致性的（比如：Cassandra），你必须配置该值。
    -- 它将确保db_update_propagation秒后，数据库节点间的变化在整个数据库集群中所有节点生效。
    -- 当配置了该值，kong 节点从同步任务中接收无效事件，清除本地缓存将会延迟这么多秒。
    poll_offset             = kong.configuration.db_update_propagation,
  }
  if not cluster_events then
    ngx_log(ngx_CRIT, "could not create cluster_events: ", err)
    return
  end


  -- init cache


  local cache, err = kong_cache.new {
    cluster_events    = cluster_events,
    worker_events     = worker_events,
    -- db_update_propagation ：同上
    propagation_delay = kong.configuration.db_update_propagation,
    -- db_cache_ttl ：缓存数据库实体的时间（包括缓存命中或者穿透），
    -- 该存活时间是一种保护措施，以防 kong 节点漏掉处理缓存无效事件，
    -- 避免旧数据长时间没有被清理。当缓存生存时间到了，缓存值将会被清理掉，
    -- 下一次将会从数据库读取数据并再次缓存起来。
    ttl               = kong.configuration.db_cache_ttl,
    neg_ttl           = kong.configuration.db_cache_ttl,
    resurrect_ttl     = kong.configuration.resurrect_ttl,
    resty_lock_opts   = {
      exptime = 10,
      timeout = 5,
    },
  }
  if not cache then
    ngx_log(ngx_CRIT, "could not create kong cache: ", err)
    return
  end

  -- 在缓存中设置 router:version
  local ok, err = cache:get("router:version", { ttl = 0 }, function()
    return "init"
  end)
  if not ok then
    ngx_log(ngx_CRIT, "could not set router version in cache: ", err)
    return
  end

  -- 在缓存中设置 api_router:version
  local ok, err = cache:get("api_router:version", { ttl = 0 }, function()
    return "init"
  end)
  if not ok then
    ngx_log(ngx_CRIT, "could not set API router version in cache: ", err)
    return
  end


  -- LEGACY
  -- 缓存的单例
  singletons.cache          = cache
  singletons.worker_events  = worker_events
  singletons.cluster_events = cluster_events
  -- /LEGACY


  -- 使用缓存的另一种方法
  kong.cache = cache
  kong.worker_events = worker_events
  kong.cluster_events = cluster_events

  kong.db:set_events_handler(worker_events)
  kong.dao:set_events_handler(worker_events)


  runloop.init_worker.before()


  -- run plugins init_worker context
  -- 插件 启动（继承的基类中的方法，部分插件自己重写了）

  for _, plugin in ipairs(loaded_plugins) do
    kong_global.set_namespaced_log(kong, plugin.name)

    plugin.handler:init_worker()
  end
end

-- ssl_certificate_by_lua_block
-- 在Nginx和下游服务开始一个SSL握手时处理
function Kong.ssl_certificate()
  kong_global.set_phase(kong, PHASES.certificate)

  local ctx = ngx.ctx

  runloop.certificate.before(ctx)

  for plugin, plugin_conf in plugins_iterator(loaded_plugins, true) do
    kong_global.set_namespaced_log(kong, plugin.name)
    plugin.handler:certificate(plugin_conf)
    kong_global.reset_log(kong)
  end
end

-- balancer_by_lua_block
-- 上游服务器中的负载均衡器
function Kong.balancer()
  kong_global.set_phase(kong, PHASES.balancer)

  local ctx = ngx.ctx
  local balancer_data = ctx.balancer_data
  local tries = balancer_data.tries
  local current_try = {}
  balancer_data.try_count = balancer_data.try_count + 1
  tries[balancer_data.try_count] = current_try

  runloop.balancer.before()

  if balancer_data.try_count > 1 then
    -- only call balancer on retry, first one is done in `runloop.access.after`
    -- which runs in the ACCESS context and hence has less limitations than
    -- this BALANCER context where the retries are executed

    -- record failure data
    local previous_try = tries[balancer_data.try_count - 1]
    previous_try.state, previous_try.code = get_last_failure()

    -- Report HTTP status for health checks
    local balancer = balancer_data.balancer
    if balancer then
      local ip, port = balancer_data.ip, balancer_data.port

      if previous_try.state == "failed" then
        if previous_try.code == 504 then
          balancer.report_timeout(ip, port)
        else
          balancer.report_tcp_failure(ip, port)
        end

      else
        balancer.report_http_status(ip, port, previous_try.code)
      end
    end

    local ok, err, errcode = balancer_execute(balancer_data)
    if not ok then
      ngx_log(ngx_ERR, "failed to retry the dns/balancer resolver for ",
              tostring(balancer_data.host), "' with: ", tostring(err))
      return ngx.exit(errcode)
    end

  else
    -- first try, so set the max number of retries
    local retries = balancer_data.retries
    if retries > 0 then
      set_more_tries(retries)
    end
  end

  current_try.ip   = balancer_data.ip
  current_try.port = balancer_data.port

  -- set the targets as resolved
  ngx_log(ngx_DEBUG, "setting address (try ", balancer_data.try_count, "): ",
                     balancer_data.ip, ":", balancer_data.port)
  local ok, err = set_current_peer(balancer_data.ip, balancer_data.port)
  if not ok then
    ngx_log(ngx_ERR, "failed to set the current peer (address: ",
            tostring(balancer_data.ip), " port: ", tostring(balancer_data.port),
            "): ", tostring(err))
    return ngx.exit(500)
  end

  ok, err = set_timeouts(balancer_data.connect_timeout / 1000,
                         balancer_data.send_timeout / 1000,
                         balancer_data.read_timeout / 1000)
  if not ok then
    ngx_log(ngx_ERR, "could not set upstream timeouts: ", err)
  end

  runloop.balancer.after()
end

-- rewrite_by_lua_block
-- 内部URL重写或者外部重定向
function Kong.rewrite()
  kong_resty_ctx.stash_ref()
  kong_global.set_phase(kong, PHASES.rewrite)

  local ctx = ngx.ctx

  runloop.rewrite.before(ctx)

  -- we're just using the iterator, as in this rewrite phase no consumer nor
  -- api will have been identified, hence we'll just be executing the global
  -- plugins
  for plugin, plugin_conf in plugins_iterator(loaded_plugins, true) do
    kong_global.set_named_ctx(kong, "plugin", plugin_conf)
    kong_global.set_namespaced_log(kong, plugin.name)

    plugin.handler:rewrite(plugin_conf)

    kong_global.reset_log(kong)
  end

  runloop.rewrite.after(ctx)
end

-- access_by_lua_block
-- 访问控制
function Kong.access()
  kong_global.set_phase(kong, PHASES.access)

  local ctx = ngx.ctx

  runloop.access.before(ctx)

  ctx.delay_response = true

  for plugin, plugin_conf in plugins_iterator(loaded_plugins, true) do
    if not ctx.delayed_response then
      kong_global.set_named_ctx(kong, "plugin", plugin_conf)
      kong_global.set_namespaced_log(kong, plugin.name)

      local err = coroutine.wrap(plugin.handler.access)(plugin.handler, plugin_conf)

      kong_global.reset_log(kong)

      if err then
        ctx.delay_response = false
        return responses.send_HTTP_INTERNAL_SERVER_ERROR(err)
      end
    end
  end

  if ctx.delayed_response then
    return responses.flush_delayed_response(ctx)
  end

  ctx.delay_response = false

  runloop.access.after(ctx)
end

-- header_filter_by_lua_block
-- 设置响应头
function Kong.header_filter()
  kong_global.set_phase(kong, PHASES.header_filter)

  local ctx = ngx.ctx

  runloop.header_filter.before(ctx)

  for plugin, plugin_conf in plugins_iterator(loaded_plugins) do
    kong_global.set_named_ctx(kong, "plugin", plugin_conf)
    kong_global.set_namespaced_log(kong, plugin.name)

    plugin.handler:header_filter(plugin_conf)

    kong_global.reset_log(kong)
  end

  runloop.header_filter.after(ctx)
end

-- body_filter_by_lua_block
-- 对响应数据进行过滤
function Kong.body_filter()
  kong_global.set_phase(kong, PHASES.body_filter)

  for plugin, plugin_conf in plugins_iterator(loaded_plugins) do
    kong_global.set_named_ctx(kong, "plugin", plugin_conf)
    kong_global.set_namespaced_log(kong, plugin.name)

    plugin.handler:body_filter(plugin_conf)

    kong_global.reset_log(kong)
  end

  runloop.body_filter.after(ngx.ctx)
end

-- log_by_lua_block
-- 使用Lua处理日志
function Kong.log()
  kong_global.set_phase(kong, PHASES.log)

  for plugin, plugin_conf in plugins_iterator(loaded_plugins) do
    kong_global.set_named_ctx(kong, "plugin", plugin_conf)
    kong_global.set_namespaced_log(kong, plugin.name)

    -- 此处为每个插件中使用的 conf
    plugin.handler:log(plugin_conf)

    kong_global.reset_log(kong)
  end

  runloop.log.after(ngx.ctx)
end

-- content_by_lua_block
function Kong.handle_error()
  kong_resty_ctx.apply_ref()

  if not ngx.ctx.plugins_for_request then
    for plugin, plugin_conf in plugins_iterator(loaded_plugins, true) do
      -- just build list of plugins
    end
  end

  return kong_error_handlers(ngx)
end

-- 后台管理API
function Kong.serve_admin_api(options)
  options = options or {}

  header["Access-Control-Allow-Origin"] = options.allow_origin or "*"

  if ngx.req.get_method() == "OPTIONS" then
    header["Access-Control-Allow-Methods"] = "GET, HEAD, PUT, PATCH, POST, DELETE"
    header["Access-Control-Allow-Headers"] = "Content-Type"

    return ngx.exit(204)
  end

  -- 见/api/init.lua
  return lapis.serve("kong.api")
end

return Kong
