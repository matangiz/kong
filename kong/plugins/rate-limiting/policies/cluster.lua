local luatz = require "luatz"


local kong = kong
local timetable = luatz.timetable
local concat = table.concat
local pairs = pairs
local floor = math.floor
local fmt = string.format


local EMPTY_UUID = "00000000-0000-0000-0000-000000000000"


local ms_check = timetable.new(20000, 1, 1, 0, 0, 0):timestamp()


local function get_timetable(now)
  if now > ms_check then
    return timetable.new_from_timestamp(now / 1000)
  end

  return timetable.new_from_timestamp(now)
end


local function get_timestamps(now)
  local timetable = get_timetable(now)
  local stamps = {}

  timetable.sec = floor(timetable.sec)   -- reduce to second precision
  stamps.second = timetable:timestamp() * 1000

  timetable.sec = 0
  stamps.minute = timetable:timestamp() * 1000

  timetable.min = 0
  stamps.hour = timetable:timestamp() * 1000

  timetable.hour = 0
  stamps.day = timetable:timestamp() * 1000

  timetable.day = 1
  stamps.month = timetable:timestamp() * 1000

  timetable.month = 1
  stamps.year = timetable:timestamp() * 1000

  return stamps
end


return {
  cassandra = {
    increment = function(db, limits, route_id, service_id, identifier, current_timestamp, value)
      local periods = get_timestamps(current_timestamp)

      for period, period_date in pairs(periods) do
        if limits[period] then
          local res, err = db:query([[
            UPDATE ratelimiting_metrics
               SET value = value + ?
             WHERE route_id = ?
               AND service_id = ?
               AND api_id = ?
               AND identifier = ?
               AND period_date = ?
               AND period = ?
          ]], {
            db.cassandra.counter(value),
            db.cassandra.uuid(route_id),
            db.cassandra.uuid(service_id),
            db.cassandra.uuid(EMPTY_UUID),
            identifier,
            db.cassandra.timestamp(period_date),
            period,
          })
          if not res then
            kong.log.err("cluster policy: could not increment cassandra counter for period '",
                         period, "': ", err)
          end
        end
      end

      return true
    end,
    increment_api = function(db, limits, api_id, identifier, current_timestamp, value)
      local periods = get_timestamps(current_timestamp)

      for period, period_date in pairs(periods) do
        if limits[period] then
          local res, err = db:query([[
            UPDATE ratelimiting_metrics
               SET value = value + ?
             WHERE api_id = ? AND
                   route_id = ? AND
                   service_id = ? AND
                   identifier = ? AND
                   period_date = ? AND
                   period = ?
          ]], {
            db.cassandra.counter(value),
            db.cassandra.uuid(api_id),
            db.cassandra.uuid(EMPTY_UUID),
            db.cassandra.uuid(EMPTY_UUID),
            identifier,
            db.cassandra.timestamp(period_date),
            period,
          })
          if not res then
            kong.log.err("cluster policy: could not increment cassandra counter for period '",
                         period, "': ", err)
          end
        end
      end

      return true
    end,
    find = function(db, route_id, service_id, identifier, current_timestamp, period)
      local periods = get_timestamps(current_timestamp)

      local rows, err = db:query([[
        SELECT value
          FROM ratelimiting_metrics
         WHERE route_id = ?
           AND service_id = ?
           AND api_id = ?
           AND identifier = ?
           AND period_date = ?
           AND period = ?
      ]], {
        db.cassandra.uuid(route_id),
        db.cassandra.uuid(service_id),
        db.cassandra.uuid(EMPTY_UUID),
        identifier,
        db.cassandra.timestamp(periods[period]),
        period,
      })

      if not rows then
        return nil, err
      elseif #rows <= 1 then
        return rows[1]
      else
        return nil, "bad rows result"
      end
    end,
    find_api = function(db, api_id, identifier, current_timestamp, period)
      local periods = get_timestamps(current_timestamp)

      local rows, err = db:query([[
        SELECT value
          FROM ratelimiting_metrics
         WHERE api_id = ? AND
               route_id = ? AND
               service_id = ? AND
               identifier = ? AND
               period_date = ? AND
               period = ?
      ]], {
        db.cassandra.uuid(api_id),
        db.cassandra.uuid(EMPTY_UUID),
        db.cassandra.uuid(EMPTY_UUID),
        identifier,
        db.cassandra.timestamp(periods[period]),
        period,
      })

      if not rows then
        return nil, err
      elseif #rows <= 1 then
        return rows[1]
      else
        return nil, "bad rows result" end
    end,
  },
  postgres = {
    increment = function(db, limits, route_id, service_id, identifier, current_timestamp, value)
      local buf = { "BEGIN" }
      local len = 1
      local periods = get_timestamps(current_timestamp)

      for period, period_date in pairs(periods) do
        if limits[period] then
          len = len + 1
          buf[len] = fmt([[
            INSERT INTO "ratelimiting_metrics" ("identifier", "period", "period_date", "service_id", "route_id", "api_id", "value")
                 VALUES ('%s', '%s', TO_TIMESTAMP('%s') AT TIME ZONE 'UTC', '%s', '%s', '%s', %d)
            ON CONFLICT ("identifier", "period", "period_date", "service_id", "route_id", "api_id") DO UPDATE
                    SET "value" = "ratelimiting_metrics"."value" + EXCLUDED."value"
          ]], identifier, floor(period, period_date / 1000), service_id, route_id, EMPTY_UUID, value)
        end
      end

      if len > 1 then
        local sql
        if len == 2 then
          sql = buf[2]

        else
          buf[len + 1] = "COMMIT;"
          sql = concat(buf, ";\n")
        end

        local res, err = db:query(sql)
        if not res then
          return nil, err
        end
      end

      return true
    end,
    increment_api = function(db, limits, api_id, identifier, current_timestamp, value)
      local buf = { "BEGIN" }
      local len = 1
      local periods = get_timestamps(current_timestamp)

      for period, period_date in pairs(periods) do
        if limits[period] then
          len = len + 1
          buf[len] = fmt([[
            INSERT INTO "ratelimiting_metrics" ("identifier", "period", "period_date", "service_id", "route_id", "api_id", "value")
                 VALUES ('%s', '%s', TO_TIMESTAMP('%s') AT TIME ZONE 'UTC', '%s', '%s', '%s', %d)
            ON CONFLICT ("identifier", "period", "period_date", "service_id", "route_id", "api_id") DO UPDATE
                    SET "value" = "ratelimiting_metrics"."value" + EXCLUDED."value"
          ]], identifier, period, floor(period_date / 1000), EMPTY_UUID, EMPTY_UUID, api_id, value)
        end
      end

      if len > 1 then
        local sql
        if len == 2 then
          sql = buf[2]

        else
          buf[len + 1] = "COMMIT;"
          sql = concat(buf, ";\n")
        end

        local res, err = db:query(sql)
        if not res then
          return nil, err
        end
      end

      return true
    end,
    find = function(db, route_id, service_id, identifier, current_timestamp, period)
      local periods = get_timestamps(current_timestamp)

      local sql = fmt([[
        SELECT "value"
          FROM "ratelimiting_metrics"
         WHERE "identifier" = '%s'
           AND "period_date" = TO_TIMESTAMP('%s') AT TIME ZONE 'UTC'
           AND "period" = '%s'
           AND "service_id" = '%s'
           AND "route_id" = '%s'
           AND "api_id" = '%s'
         LIMIT 1;
      ]], identifier, floor(periods[period] / 1000), period, service_id, route_id, EMPTY_UUID)

      local res, err = db:query(sql)
      if not res or err then
        return nil, err
      end

      return res[1]
    end,
    find_api = function(db, api_id, identifier, current_timestamp, period)
      local periods = get_timestamps(current_timestamp)

      local sql = fmt([[
        SELECT "value"
          FROM "ratelimiting_metrics"
         WHERE "identifier" = '%s'
           AND "period_date" = TO_TIMESTAMP('%s') AT TIME ZONE 'UTC'
           AND "period" = '%s'
           AND "service_id" = '%s'
           AND "route_id" = '%s'
           AND "api_id" = '%s'
         LIMIT 1;
      ]], identifier, floor(periods[period] / 1000), period, EMPTY_UUID, EMPTY_UUID, api_id)

      local res, err = db:query(sql)
      if not res or err then
        return nil, err
      end

      return res[1]
    end,
  },
}
