return {
  postgres = {
    up = [[
      ALTER TABLE IF EXISTS ONLY "response_ratelimiting_metrics"
        ALTER "period_date" TYPE TIMESTAMP WITH TIME ZONE USING "period_date" AT TIME ZONE 'UTC';

      CREATE OR REPLACE FUNCTION increment_response_rate_limits (r_id UUID, s_id UUID, i TEXT, p TEXT, p_date TIMESTAMP WITH TIME ZONE, v INTEGER) RETURNS void
      LANGUAGE plpgsql
      AS $$
        BEGIN
          INSERT INTO response_ratelimiting_metrics AS old(identifier, period, period_date, service_id, route_id, value)
               VALUES (i, p, p_date, s_id, r_id, v)
          ON CONFLICT ON CONSTRAINT response_ratelimiting_metrics_pkey
          DO UPDATE SET value = old.value + v;

        END;
        $$;

      CREATE OR REPLACE FUNCTION increment_response_rate_limits_api (a_id UUID, i TEXT, p TEXT, p_date TIMESTAMP WITH TIME ZONE, v INTEGER) RETURNS void
      LANGUAGE plpgsql
      AS $$
        BEGIN
          INSERT INTO response_ratelimiting_metrics AS old(identifier, period, period_date, api_id, value)
               VALUES (i, p, p_date, a_id, v)
          ON CONFLICT ON CONSTRAINT response_ratelimiting_metrics_pkey
          DO UPDATE SET value = old.value + v;
        END;
        $$;

    ]],
  },

  cassandra = {
    up = [[
    ]],
  },
}
