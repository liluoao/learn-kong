local responses    = require "kong.tools.responses"


local kong         = kong
local setmetatable = setmetatable
local ipairs       = ipairs


-- Loads a plugin config from the datastore.
-- @return plugin config table or an empty sentinel table in case of a db-miss
-- 从数据库查出来加到内存里
local function load_plugin_into_memory(route_id,
                                       service_id,
                                       consumer_id,
                                       plugin_name,
                                       api_id)
  -- 根据名字 路由ID 服务ID 消费者ID APIID查出所有插件配置
  local rows, err = kong.dao.plugins:find_all {
             name = plugin_name,
         route_id = route_id,
       service_id = service_id,
      consumer_id = consumer_id,
           api_id = api_id,
  }
  if err then
    return nil, tostring(err)
  end

  if #rows > 0 then
    for _, row in ipairs(rows) do
      -- 返回 路由ID 服务ID 消费者ID APIID全一致的
      if    route_id == row.route_id    and
          service_id == row.service_id  and
         consumer_id == row.consumer_id and
              api_id == row.api_id      then
        return row
      end
    end
  end
end


--- Load the configuration for a plugin entry in the DB.
-- Given an API, a Consumer and a plugin name, retrieve the plugin's
-- configuration if it exists. Results are cached in ngx.dict
-- @param[type=string] route_id ID of the route being proxied.
-- @param[type=string] service_id ID of the service being proxied.
-- @param[type=string] consumer_id ID of the Consumer making the request (if any).
-- @param[type=stirng] plugin_name Name of the plugin being tested for.
-- @param[type=string] api_id ID of the API being proxied.
-- @treturn table Plugin retrieved from the cache or database.
local function load_plugin_configuration(route_id,
                                         service_id,
                                         consumer_id,
                                         plugin_name,
                                         api_id)
  -- 缓存键格式  名字:路由ID:服务ID:消费者ID:APIID
  local plugin_cache_key = kong.dao.plugins:cache_key(plugin_name,
                                                            route_id,
                                                            service_id,
                                                            consumer_id,
                                                            api_id)

  local plugin, err = kong.cache:get(plugin_cache_key,
                                     nil,
                                     load_plugin_into_memory,
                                     route_id,
                                     service_id,
                                     consumer_id,
                                     plugin_name,
                                     api_id)
  if err then
    ngx.ctx.delay_response = false
    return responses.send_HTTP_INTERNAL_SERVER_ERROR(err)
  end

  if plugin ~= nil and plugin.enabled then
    local cfg       = plugin.config or {}
    cfg.api_id      = plugin.api_id
    cfg.route_id    = plugin.route_id
    cfg.service_id  = plugin.service_id
    cfg.consumer_id = plugin.consumer_id

    return cfg
  end
end


local function get_next(self)
  local i = self.i + 1

  local plugin = self.loaded_plugins[i]
  if not plugin then
    return nil
  end

  self.i = i

  local ctx = self.ctx

  -- load the plugin configuration in early phases
  if self.access_or_cert_ctx then

    local api          = self.api
    local route        = self.route
    local service      = self.service
    local consumer     = ctx.authenticated_consumer

    if consumer then
      local schema = plugin.schema
      if schema and schema.no_consumer then
        consumer = nil
      end
    end

    local      api_id = api      and      api.id or nil
    local    route_id = route    and    route.id or nil
    local  service_id = service  and  service.id or nil
    local consumer_id = consumer and consumer.id or nil

    local plugin_name = plugin.name

    local plugin_configuration

    -- 按照本次请求的API、路由、服务、消费者查找匹配的配置
    -- 优先度降序
    repeat

      -- 同时匹配路由、服务、消费者
      if route_id and service_id and consumer_id then
        plugin_configuration = load_plugin_configuration(route_id, service_id, consumer_id, plugin_name, nil)
        if plugin_configuration then
          break
        end
      end

      -- 路由与消费者
      if route_id and consumer_id then
        plugin_configuration = load_plugin_configuration(route_id, nil, consumer_id, plugin_name, nil)
        if plugin_configuration then
          break
        end
      end

      -- 服务与消费者
      if service_id and consumer_id then
        plugin_configuration = load_plugin_configuration(nil, service_id, consumer_id, plugin_name, nil)
        if plugin_configuration then
          break
        end
      end

      -- 接口与消费者
      if api_id and consumer_id then
        plugin_configuration = load_plugin_configuration(nil, nil, consumer_id, plugin_name, api_id)
        if plugin_configuration then
          break
        end
      end

      -- 路由与服务
      if route_id and service_id then
        plugin_configuration = load_plugin_configuration(route_id, service_id, nil, plugin_name, nil)
        if plugin_configuration then
          break
        end
      end

      -- 消费者
      if consumer_id then
        plugin_configuration = load_plugin_configuration(nil, nil, consumer_id, plugin_name, nil)
        if plugin_configuration then
          break
        end
      end

      --路由
      if route_id then
        plugin_configuration = load_plugin_configuration(route_id, nil, nil, plugin_name, nil)
        if plugin_configuration then
          break
        end
      end

      -- 服务
      if service_id then
        plugin_configuration = load_plugin_configuration(nil, service_id, nil, plugin_name, nil)
        if plugin_configuration then
          break
        end
      end

      -- 接口（DEPRECATED）
      if api_id then
        plugin_configuration = load_plugin_configuration(nil, nil, nil, plugin_name, api_id)
        if plugin_configuration then
          break
        end
      end

      -- 纯插件
      plugin_configuration = load_plugin_configuration(nil, nil, nil, plugin_name, nil)

    until true

    if plugin_configuration then
      -- 放进 ngx.ctx
      ctx.plugins_for_request[plugin.name] = plugin_configuration
    end
  end

  -- return the plugin configuration
  local plugins_for_request = ctx.plugins_for_request
  if plugins_for_request[plugin.name] then
    -- 返回这个插件的本身和配置
    return plugin, plugins_for_request[plugin.name]
  end

  return get_next(self) -- Load next plugin
end


local plugin_iter_mt = { __call = get_next }


--- Plugins for request iterator.
-- Iterate over the plugin loaded for a request, stored in
-- `ngx.ctx.plugins_for_request`.
-- @param[type=boolean] access_or_cert_ctx Tells if the context
-- is access_by_lua_block. We don't use `ngx.get_phase()` simply because we can
-- avoid it.
-- @treturn function iterator
local function iter_plugins_for_req(loaded_plugins, access_or_cert_ctx)
  local ctx = ngx.ctx

  if not ctx.plugins_for_request then
    ctx.plugins_for_request = {}
  end

  local plugin_iter_state = {
    i                     = 0,
    ctx                   = ctx,
    api                   = ctx.api,
    route                 = ctx.route,
    service               = ctx.service,
    loaded_plugins        = loaded_plugins,
    access_or_cert_ctx    = access_or_cert_ctx,
  }

  return setmetatable(plugin_iter_state, plugin_iter_mt)
end


-- 返回迭代器
-- 1号位是插件 2号位是插件的配置
return iter_plugins_for_req
