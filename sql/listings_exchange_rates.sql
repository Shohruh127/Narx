BEGIN;

-- 1. Kengaytmalar (Extensions)
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS postgis;

-- 2. UUIDv7 Funksiyasi (Vaqtga asoslangan, B-Tree uchun optimallashtirilgan)
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

-- 3. Asosiy Jadvallar (Tables)
CREATE TABLE listings (
    id uuid NOT NULL DEFAULT generate_uuid_v7() PRIMARY KEY,
    
    -- UPSERT uchun eng muhim maydonlar
    source varchar(50) NOT NULL,       -- masalan: 'olx', 'uybor'
    source_id varchar(100) NOT NULL,   -- saytdagi asl e'lon ID'si
    
    -- Obyekt xususiyatlari
    property_type varchar(20),         -- 'apartment', 'house', 'land'
    deal_type varchar(20),             -- 'sale', 'rent'
    title text,
    description text,
    
    -- Narxlar
    price_original numeric(18, 2),
    currency_original varchar(10),     -- 'UZS', 'USD', 'y.e.'
    price_uzs_normalized numeric(18, 2),
    price_usd_normalized numeric(18, 2),
    
    -- Maydonlar (Hech qanday qat'iy CHECK yo'q, iflos ma'lumotga chidamli)
    land_area_sotix numeric(12, 2),
    living_area_m2 numeric(12, 2),
    room_count smallint,
    
    -- Lokatsiya
    address_raw text,
    location geometry(Point, 4326),
    
    -- Meta ma'lumotlar
    status varchar(20) NOT NULL DEFAULT 'active', -- 'active', 'inactive', 'sold'
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),

    -- Global unikallik (Upsert ishlashi uchun shart!)
    CONSTRAINT uq_listings_source_id UNIQUE (source, source_id)
);

CREATE TABLE exchange_rates (
    id uuid NOT NULL DEFAULT generate_uuid_v7() PRIMARY KEY,
    currency_code varchar(8) NOT NULL,
    rate_to_uzs numeric(18, 6) NOT NULL,
    rate_date date NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    
    -- Bir kunda bitta valyuta uchun faqat bitta kurs bo'lishi shart
    CONSTRAINT uq_exchange_rates_date UNIQUE (currency_code, rate_date)
);

-- 4. Geospatial va Qidiruv Indekslari
-- PostGIS orqali radius bo'yicha qidiruvni tezlashtirish uchun GiST indeksi.
CREATE INDEX idx_listings_location ON listings USING GIST (location);

-- Analitika va filterlar uchun B-Tree indekslar
CREATE INDEX idx_listings_status ON listings (status);
CREATE INDEX idx_listings_created_at ON listings (created_at DESC);
CREATE INDEX idx_listings_property_type ON listings (property_type);

-- 5. Avtomatik updated_at triggeri (Faqat shu trigger qoldirildi)
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER trg_listings_updated_at
BEFORE UPDATE ON listings
FOR EACH ROW
EXECUTE FUNCTION update_modified_column();

COMMIT;
