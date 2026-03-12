# Narx

`sql/listings_exchange_rates.sql` faylida PostgreSQL + PostGIS uchun `listings` va `exchange_rates` jadvallari DDL skripti bor. Skript UUIDv7 identifikatorlari, alohida `land_area_sotix` va `living_area_m2` ustunlari, narxni UZS ga normallashtirish, PostGIS `location` nuqtasi uchun GiST indeks, hamda `status` + oylik `created_at` partitioning'ni qamrab oladi.

`olx_client.py` modulida `httpx.AsyncClient` asosidagi OLX.uz async API klienti bor. U `api/v1/offers/` endpointidan sahifalab ma'lumot oladi, Pydantic V2 modellarida `id`, `url`, `title`, `price`, `params` maydonlarini validate qiladi va `tenacity` orqali 429/403/50x javoblarda 3 marta eksponensial backoff bilan qayta urinadi. So'rovlar mo'rt URL path parsing o'rniga `category_id` orqali quriladi, URL ichidagi query parametrlar esa saqlab qolinadi.

`dagster_pipeline.py` faylida 3 ta Dagster asset bor: `raw_olx_data`, `clean_real_estate_data`, `load_to_postgres`. PostgreSQL ulanish satri hardcode qilinmagan, u `POSTGRES_CONNECTION_STRING` muhit o'zgaruvchisidan Dagster `EnvVar` orqali olinadi va `load_to_postgres` asseti `INSERT ... ON CONFLICT (source, source_id) DO UPDATE` orqali faqat `price_original`, `price_uzs_normalized`, `status`, `updated_at` maydonlarini yangilaydi.

Bog'liqliklarni o'rnatish:

```bash
python -m pip install -r requirements.txt
```
