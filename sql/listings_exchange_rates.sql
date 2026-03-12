BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS postgis;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'record_status') THEN
        CREATE TYPE record_status AS ENUM ('active', 'inactive');
    END IF;
END
$$;

CREATE OR REPLACE FUNCTION generate_uuid_v7()
RETURNS uuid
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    unix_ts_ms bigint;
    uuid_bytes bytea;
BEGIN
    unix_ts_ms := floor(extract(epoch FROM clock_timestamp()) * 1000);
    uuid_bytes := gen_random_bytes(16);

    uuid_bytes := overlay(uuid_bytes placing substring(int8send(unix_ts_ms) FROM 3 FOR 6) FROM 1 FOR 6);
    uuid_bytes := set_byte(uuid_bytes, 6, (get_byte(uuid_bytes, 6) & 15) | 112);
    uuid_bytes := set_byte(uuid_bytes, 8, (get_byte(uuid_bytes, 8) & 63) | 128);

    RETURN encode(uuid_bytes, 'hex')::uuid;
END;
$$;

CREATE TABLE listings (
    id uuid NOT NULL DEFAULT generate_uuid_v7(),
    property_type varchar(20) NOT NULL CHECK (property_type IN ('apartment', 'house', 'land', 'commercial')),
    title text NOT NULL,
    description text,
    price_original numeric(18, 2) NOT NULL CHECK (price_original >= 0),
    currency_original varchar(8) NOT NULL,
    price_uzs_normalized numeric(18, 2) NOT NULL CHECK (price_uzs_normalized >= 0),
    land_area_sotix numeric(12, 2),
    living_area_m2 numeric(12, 2),
    room_count smallint CHECK (room_count IS NULL OR room_count >= 0),
    address text,
    location geometry(Point, 4326) NOT NULL,
    status record_status NOT NULL DEFAULT 'active',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT listings_pk PRIMARY KEY (id, status, created_at),
    CONSTRAINT listings_area_values_chk CHECK (
        (land_area_sotix IS NULL OR land_area_sotix >= 0)
        AND (living_area_m2 IS NULL OR living_area_m2 >= 0)
    ),
    CONSTRAINT listings_area_required_chk CHECK (
        CASE property_type
            WHEN 'land' THEN land_area_sotix IS NOT NULL
            WHEN 'house' THEN land_area_sotix IS NOT NULL AND living_area_m2 IS NOT NULL
            WHEN 'apartment' THEN living_area_m2 IS NOT NULL
            ELSE TRUE
        END
    )
) PARTITION BY LIST (status);

CREATE TABLE listings_active
    PARTITION OF listings
    FOR VALUES IN ('active')
    PARTITION BY RANGE (created_at);

CREATE TABLE listings_inactive
    PARTITION OF listings
    FOR VALUES IN ('inactive')
    PARTITION BY RANGE (created_at);

CREATE TABLE exchange_rates (
    id uuid NOT NULL DEFAULT generate_uuid_v7(),
    currency_code varchar(8) NOT NULL,
    rate_to_uzs numeric(18, 6) NOT NULL CHECK (rate_to_uzs > 0),
    rate_date date NOT NULL,
    status record_status NOT NULL DEFAULT 'active',
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT exchange_rates_pk PRIMARY KEY (id, status, created_at),
    CONSTRAINT exchange_rates_currency_code_chk CHECK (currency_code = upper(currency_code))
) PARTITION BY LIST (status);

CREATE TABLE exchange_rates_active
    PARTITION OF exchange_rates
    FOR VALUES IN ('active')
    PARTITION BY RANGE (created_at);

CREATE TABLE exchange_rates_inactive
    PARTITION OF exchange_rates
    FOR VALUES IN ('inactive')
    PARTITION BY RANGE (created_at);

CREATE OR REPLACE FUNCTION create_monthly_real_estate_partitions(partition_month date)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    month_start date := date_trunc('month', partition_month)::date;
    month_end date := (date_trunc('month', partition_month) + interval '1 month')::date;
    suffix text := to_char(date_trunc('month', partition_month), 'YYYYMM');
BEGIN
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS listings_active_%s PARTITION OF listings_active FOR VALUES FROM (%L) TO (%L)',
        suffix,
        month_start,
        month_end
    );

    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS listings_inactive_%s PARTITION OF listings_inactive FOR VALUES FROM (%L) TO (%L)',
        suffix,
        month_start,
        month_end
    );

    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS exchange_rates_active_%s PARTITION OF exchange_rates_active FOR VALUES FROM (%L) TO (%L)',
        suffix,
        month_start,
        month_end
    );

    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS exchange_rates_inactive_%s PARTITION OF exchange_rates_inactive FOR VALUES FROM (%L) TO (%L)',
        suffix,
        month_start,
        month_end
    );
END;
$$;

SELECT create_monthly_real_estate_partitions(current_date);
SELECT create_monthly_real_estate_partitions((current_date + interval '1 month')::date);

CREATE INDEX listings_location_gix ON listings USING GIST (location);
CREATE INDEX listings_created_at_idx ON listings (created_at);
CREATE INDEX exchange_rates_currency_rate_date_idx ON exchange_rates (currency_code, rate_date DESC);

COMMIT;
