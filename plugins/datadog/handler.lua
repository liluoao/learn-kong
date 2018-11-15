local BasePlugin       = require "kong.plugins.base_plugin"
local basic_serializer = require "kong.plugins.log-serializers.basic"
local statsd_logger    = require "kong.plugins.datadog.statsd_logger"


local ngx_log       = ngx.log
local ngx_timer_at  = ngx.timer.at
local string_gsub   = string.gsub
local pairs         = pairs
local string_format = string.format
local NGX_ERR       = ngx.ERR


local DatadogHandler    = BasePlugin:extend()
-- 优先级低
DatadogHandler.PRIORITY = 10
DatadogHandler.VERSION = "0.1.0"


-- 消费者的3种标识的链表
local get_consumer_id = {
  consumer_id = function(consumer)
    -- 如果 consumer 存在则返回 id 属性
    return consumer and string_gsub(consumer.id, "-", "_")
  end,
  custom_id   = function(consumer)
    -- 同理
    return consumer and consumer.custom_id
  end,
  username    = function(consumer)
    return consumer and consumer.username
  end
}


-- 定义了4种需要特殊处理的统计
-- 涉及到消费者与状态码
local metrics = {
  -- 状态码统计
  status_count = function (api_name, message, metric_config, logger)
    local fmt = string_format("%s.request.status", api_name,
                       message.response.status)

    -- 调用 statsd_logger 中的 statsd_message
    -- 发送消息给Datadog配置的主机端口
    -- stat_types 是取单位
    -- 数字 1 是这里每次都是一次请求进来
    logger:send_statsd(string_format("%s.%s", fmt, message.response.status),
                       1, logger.stat_types.counter,
                       metric_config.sample_rate, metric_config.tags)

    -- 上面是状态码单个信息 下面是状态码数量
    -- 例子：xxx.request.status.200:1|c|#app:kong
    -- 下面：xxx.request.status.total:1|c|#app:kong
    -- 这里还没有拼上统一前缀
    logger:send_statsd(string_format("%s.%s", fmt, "total"), 1,
                       logger.stat_types.counter,
                       metric_config.sample_rate, metric_config.tags)
  end,
  -- 唯一用户计数
  unique_users = function (api_name, message, metric_config, logger)
    -- 查找在配置中 unique_users 的身份标识是哪个字段 默认为 custom_id
    -- 取上面链表中对应属性
    local get_consumer_id = get_consumer_id[metric_config.consumer_identifier]
    -- 传递参数 获取到标识
    local consumer_id     = get_consumer_id(message.consumer)

    if consumer_id then
      local stat = string_format("%s.user.uniques", api_name)

      -- 例子：xxx.user.uniques:1|s|#app:kong
      logger:send_statsd(stat, consumer_id, logger.stat_types.set,
                         nil, metric_config.tags)
    end
  end,
  -- 每个用户的请求数
  request_per_user = function (api_name, message, metric_config, logger)
    local get_consumer_id = get_consumer_id[metric_config.consumer_identifier]
    local consumer_id     = get_consumer_id(message.consumer)

    if consumer_id then
      local stat = string_format("%s.user.%s.request.count", api_name, consumer_id)

      -- 例子：xxx.user.1.request.count:1|c|#app:kong
      logger:send_statsd(stat, 1, logger.stat_types.counter,
                         metric_config.sample_rate, metric_config.tags)
    end
  end,
  -- 用户与状态码统计
  status_count_per_user = function (api_name, message, metric_config, logger)
    local get_consumer_id = get_consumer_id[metric_config.consumer_identifier]
    local consumer_id     = get_consumer_id(message.consumer)

    if consumer_id then
      local fmt = string_format("%s.user.%s.request.status", api_name, consumer_id)

      -- 例子：xxx.user.1.request.status.200:1|c|#app:kong
      logger:send_statsd(string_format("%s.%s", fmt, message.response.status),
                         1, logger.stat_types.counter,
                         metric_config.sample_rate, metric_config.tags)

      -- 例子：xxx.user.1.request.status.total:1|c|#app:kong
      logger:send_statsd(string_format("%s.%s", fmt,  "total"),
                         1, logger.stat_types.counter,
                         metric_config.sample_rate, metric_config.tags)
    end
  end,
}


local function log(premature, conf, message)
  if premature then
    return
  end

  local name

  if message.service and message.service.name then
    name = string_gsub(message.service.name ~= ngx.null and
                       message.service.name or message.service.host,
                       "%.", "_")

  elseif message.api and message.api.name then
    name = string_gsub(message.api.name, "%.", "_")

  else
    -- TODO: this follows the pattern used by
    -- https://github.com/Kong/kong/pull/2702 (which prevents an error from
    -- being thrown and avoids confusing reports as per our metrics keys), but
    -- as it stands, hides traffic from monitoring tools when the plugin is
    -- configured globally. In fact, this basically disables this plugin when
    -- it is configured to run globally, or per-consumer without an
    -- API/Route/Service.
    ngx_log(ngx.DEBUG,
            "[statsd] no Route/Service/API in context, skipping logging")
    return
  end

  -- 除了上面4种之外的统计名
  -- 处理比较简单
  local stat_name  = {
    request_size     = name .. ".request.size",
    response_size    = name .. ".response.size",
    latency          = name .. ".latency",
    upstream_latency = name .. ".upstream_latency",
    kong_latency     = name .. ".kong_latency",
    request_count    = name .. ".request.count",
  }
  -- 直接从请求中取值
  local stat_value = {
    request_size     = message.request.size,
    response_size    = message.response.size,
    latency          = message.latencies.request,
    upstream_latency = message.latencies.proxy,
    kong_latency     = message.latencies.kong,
    request_count    = 1,
  }

  local logger, err = statsd_logger:new(conf)
  if err then
    ngx_log(NGX_ERR, "failed to create Statsd logger: ", err)
    return
  end

  -- 取出数据库配置中的 metrics
  -- 是在新增本插件时手动加入的
  for _, metric_config in pairs(conf.metrics) do
    -- 是否为上面配置的4种
    local metric = metrics[metric_config.name]

    -- 是就直接是函数 发送
    if metric then
      metric(name, message, metric_config, logger)

    else
      -- 否则统一转换后发送
      local stat_name  = stat_name[metric_config.name]
      local stat_value = stat_value[metric_config.name]

      logger:send_statsd(stat_name, stat_value,
                         logger.stat_types[metric_config.stat_type],
                         metric_config.sample_rate, metric_config.tags)
    end
  end

  logger:close_socket()
end


function DatadogHandler:new()
  DatadogHandler.super.new(self, "datadog")
end

function DatadogHandler:log(conf)
  DatadogHandler.super.log(self)

  if not ngx.ctx.service and
     not ngx.ctx.api     then
    return
  end

  -- 这个message直接是整个请求的序列化
  local message = basic_serializer.serialize(ngx)

  local ok, err = ngx_timer_at(0, log, conf, message)
  if not ok then
    ngx_log(NGX_ERR, "failed to create timer: ", err)
  end
end

return DatadogHandler
