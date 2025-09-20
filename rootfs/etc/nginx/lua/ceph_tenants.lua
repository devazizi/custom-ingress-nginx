local ngx = ngx
local http = require "resty.http"
local cjson = require "cjson"
local _M = {}
local INTERVAL = 60
local data = ngx.shared.rgw_tenants
local INFO = ngx.WARN

local function fetch_data(premature)
  if premature then  -- if shutting down
    return
  end
  local httpc = http.new()
  httpc:set_timeout(1000, 1000, 2000)
  local ceph_addr = os.getenv("CEPH_TENANTS_ADDR")
  local res, err = httpc:request_uri(ceph_addr)

  if not res then
    ngx.log(ngx.ERR, "Failed to request the Tenants API: ", err)
    return
  end

  if res.status ~= 200 then
    ngx.log(ngx.ERR, "Tenants API returned non-200 status code: ", res.status)
    return
  end

  local data_table = cjson.decode(res.body)

  for zone, zone_data in pairs(data_table) do
    for data_type, type_data in pairs(zone_data) do  -- data_type is `key` or `bucket`
      local prefix = zone .. ":" .. data_type .. ":"
      for key, tenant in pairs(type_data) do
        data:set(prefix .. key, tenant)
      end
    end
  end
  ngx.log(INFO, "RGW data synced successfully")
end

local worker = tostring(math.random(0, ngx.worker.count() - 1))
data:set("worker", worker)
ngx.log(INFO, "RGW timer will run on worker " .. worker)

function _M.init_worker()
  if data:get("worker") == tostring(ngx.worker.id()) then
    local ok, err = ngx.timer.at(0, fetch_data)
    if not ok then
       ngx.log(ngx.ERR, "failed to create Tenants timer: ", err)
    end
    local ok, err = ngx.timer.every(INTERVAL * (1+math.random()/2), fetch_data)
    if not ok then
       ngx.log(ngx.ERR, "failed to create Tenants interval: ", err)
    end

  end
end


function _M.access_key(zone)
  local cred_header = ngx.var.http_x_amz_credential or ngx.var["arg_x-amz-credential"]
  local access_key = cred_header and cred_header:match("^[^%%/]*")
  if not access_key and ngx.var.http_authorization then
    access_key = ngx.var.http_authorization:match('redential=([^%%/]*)')
  end
  return access_key
end

function _M.tenants(zone)
    local cred_header = ngx.var.http_x_amz_credential or ngx.var["arg_x-amz-credential"]
    local access_key = cred_header and cred_header:match("^[^%%/]*")
    if not access_key and ngx.var.http_authorization then
        access_key = ngx.var.http_authorization:match('redential=([^%%/]*)')
    end
    local first_part = ngx.var.uri:match("^/([^/]*)")
    local bucket = ngx.var.host:match("^([^.]+)%.s3%.kubit%.ir%.?$")
    local tenant
    if not bucket and first_part == "admin" then
        return ""
    end
    bucket = bucket or first_part
    if access_key then
        tenant = data:get(zone .. ":key:" ..  access_key)
    end

    if not tenant and bucket ~= "" then
      tenant = data:get(zone .. ":bucket:" ..  bucket)
    end
    return "s3:" .. (tenant or "") .. ":" .. bucket
end

-- Expose the `tenants` in the Lua global scope
_G.tenants = _M
return _M
