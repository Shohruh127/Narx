# Narx

`sql/listings_exchange_rates.sql` faylida PostgreSQL + PostGIS uchun `listings` va `exchange_rates` jadvallari DDL skripti bor. Skript UUIDv7 identifikatorlari, alohida `land_area_sotix` va `living_area_m2` ustunlari, narxni UZS ga normallashtirish, PostGIS `location` nuqtasi uchun GiST indeks, hamda `status` + oylik `created_at` partitioning'ni qamrab oladi.
