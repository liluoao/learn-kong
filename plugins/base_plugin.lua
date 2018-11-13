-- 插件基类

-- 面向对象编程的对象基类
local Object = require "kong.vendor.classic"
local BasePlugin = Object:extend()

local ngx_log = ngx.log
local DEBUG = ngx.DEBUG

-- 至少需要实现实例化方法
-- XXHandler.super.new(self, "XX")
function BasePlugin:new(name)
  self._name = name
end

-- 实现方法与入口文件相似
-- 提供了在ngx_lua_module各个生命周期中的方法

function BasePlugin:init_worker()
  ngx_log(DEBUG, "executing plugin \"", self._name, "\": init_worker")
end

function BasePlugin:certificate()
  ngx_log(DEBUG, "executing plugin \"", self._name, "\": certificate")
end

function BasePlugin:rewrite()
  ngx_log(DEBUG, "executing plugin \"", self._name, "\": rewrite")
end

function BasePlugin:access()
  ngx_log(DEBUG, "executing plugin \"", self._name, "\": access")
end

function BasePlugin:header_filter()
  ngx_log(DEBUG, "executing plugin \"", self._name, "\": header_filter")
end

function BasePlugin:body_filter()
  ngx_log(DEBUG, "executing plugin \"", self._name, "\": body_filter")
end

function BasePlugin:log()
  ngx_log(DEBUG, "executing plugin \"", self._name, "\": log")
end

return BasePlugin
