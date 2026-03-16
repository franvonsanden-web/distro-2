# distro-2

Scraper de uso interno de productos en Uruguay.

La startup Cabacua construyó una plataforma que compara más de 100.000 precios de supermercados en Montevideo a partir de datos recolectados de los catálogos de las cadenas

Esto implica hacer scraping de sitios como Tienda Inglesa, Disco, Devoto, Géant, etc. — todos tienen catálogos online.

# 🛒 Uruguay Supermarket Price Scraper (v5.1)

A high-performance, asynchronous web scraper designed to extract and track daily product prices from the major supermarket chains in Uruguay. 

**Version 5.1** completely removes the Playwright dependency in favor of direct HTTP requests (HTML parsing and GraphQL APIs). This reduces memory overhead by ~90%, speeds up execution drastically via `asyncio.gather()`, and optimizes database inserts to prevent exponential bloat.

## ✨ Key Features
* **🚀 True Concurrency:** Scrapes completely independent infrastructures (GDU, TaTa, Tienda Inglesa) in parallel using `asyncio.gather`.
* **🪶 Playwright-Free:** Uses `httpx` and `BeautifulSoup4` for HTML parsing, and reverse-engineered GraphQL endpoints for massive performance gains.
* **🛡️ Smart Database Upserts:** Bypasses Supabase/PostgREST 1,000-row limits using batched dynamic `IN` queries.
* **📉 Bloat Prevention:** Only inserts into `prices_history` if a product is brand new or if its price has mathematically changed (difference > $0.01), preventing exponential database growth.
* **🧵 Thread-Safe DB Calls:** Wraps synchronous Supabase SDK calls in `asyncio.to_thread()` to prevent freezing the async HTTP event loop.
* **🔥 Partial-Failure Tolerance:** If one category fails, the script continues and commits the successful categories, preventing total chain data loss.

## 🏪 Supported Supermarkets

| Chain | Extraction Method | Est. Products |
| :--- | :--- | :--- |
| **Disco** (GDU) | HTTP + BeautifulSoup (HTML) | ~8,000+ |
| **Devoto** (GDU) | HTTP + BeautifulSoup (HTML) | ~8,000+ |
| **Géant** (GDU) | HTTP + BeautifulSoup (HTML) | ~8,000+ |
| **TaTa** | HTTP + GraphQL API (POST) | ~10,000+ |
| **Tienda Inglesa** | HTTP + BeautifulSoup (HTML) | ~5,000+ |

## 📦 Prerequisites & Installation

1. **Clone the repository**
2. **Requires Python 3.10+**
3. **Install dependencies:**
   ```bash
   pip install httpx supabase beautifulsoup4 lxml python-dotenv
