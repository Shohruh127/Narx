# Narx

`sql/listings_exchange_rates.sql` faylida PostgreSQL + PostGIS uchun `listings` va `exchange_rates` jadvallari DDL skripti bor. Skript UUIDv7 identifikatorlari, alohida `land_area_sotix` va `living_area_m2` ustunlari, narxni UZS ga normallashtirish, PostGIS `location` nuqtasi uchun GiST indeks, hamda `status` + oylik `created_at` partitioning'ni qamrab oladi.

`olx_client.py` modulida `httpx.AsyncClient` asosidagi OLX.uz async API klienti bor. U `api/v1/offers/` endpointidan sahifalab ma'lumot oladi, Pydantic V2 modellarida `id`, `url`, `title`, `price`, `params` maydonlarini validate qiladi va `tenacity` orqali 429/403/50x javoblarda 3 marta eksponensial backoff bilan qayta urinadi.

Bog'liqliklarni o'rnatish:

```bash
python -m pip install -r requirements.txt
```
