"""
scraper.py v5.1 — distro
═══════════════════════════════════════════════════════════════════
Cambios vs v5.0:
  • Supabase batched IN queries (Bypasses PostgREST 1,000 row limit).
  • Conditional history logging (Stops DB exponential bloat).
  • True concurrency across chains via asyncio.gather().
  • Asynchronous DB wrapper to prevent event-loop blocking.
  • Category-level error boundaries (Prevents full chain data loss).
  • Updated User-Agent to newer Chrome versions.
═══════════════════════════════════════════════════════════════════
"""

import os, time, asyncio, logging, re, hashlib, json
import httpx
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from supabase import create_client, Client
from typing import Any

# Cargar .env si existe (local). En GitHub Actions las vars vienen de Secrets.
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")


def env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


MAX_PRICE        = 999_999.0
MAX_PAGES        = env_int("MAX_PAGES", 150)
MAX_CONSEC_EMPTY = env_int("MAX_CONSEC_EMPTY", 3)
GDU_PAGE_DELAY   = env_float("GDU_PAGE_DELAY", 1.2)
GDU_RETRY_WAIT   = env_float("GDU_RETRY_WAIT", 6.0)
GDU_RETRIES      = env_int("GDU_RETRIES", 2)

HTTP_TIMEOUT               = env_float("HTTP_TIMEOUT", 30.0)
HTTP_MAX_CONNECTIONS       = env_int("HTTP_MAX_CONNECTIONS", 40)
HTTP_KEEPALIVE_CONNECTIONS = env_int("HTTP_KEEPALIVE_CONNECTIONS", 20)
VTEX_CATEGORY_CONCURRENCY  = env_int("VTEX_CATEGORY_CONCURRENCY", 5)
GDU_CATEGORY_CONCURRENCY   = env_int("GDU_CATEGORY_CONCURRENCY", 2)
TATA_CATEGORY_CONCURRENCY  = env_int("TATA_CATEGORY_CONCURRENCY", 3)
TI_ALMACEN_MAX_PAGES       = env_int("TI_ALMACEN_MAX_PAGES", 200)
TI_TERM_MAX_PAGES          = env_int("TI_TERM_MAX_PAGES", 15)
DRY_RUN                    = os.environ.get("DRY_RUN", "0") == "1"
ENABLE_VTEX_ATTEMPT        = os.environ.get("ENABLE_VTEX_ATTEMPT", "1") == "1"

# Updated to a more recent Chrome version
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

HTML_HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
    "Accept-Language": "es-UY,es;q=0.9",
}

JSON_HEADERS = {
    "User-Agent": UA,
    "Accept": "application/json",
    "Accept-Language": "es-UY,es;q=0.9",
}

POST_JSON_HEADERS = {
    **JSON_HEADERS,
    "Content-Type": "application/json",
}

HTTP_LIMITS = httpx.Limits(
    max_connections=HTTP_MAX_CONNECTIONS,
    max_keepalive_connections=HTTP_KEEPALIVE_CONNECTIONS,
)


def build_async_client(*, headers: dict | None = None, timeout: float | None = None) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        headers=headers,
        timeout=timeout or HTTP_TIMEOUT,
        follow_redirects=True,
        limits=HTTP_LIMITS,
    )



def validate_runtime_config() -> None:
    if DRY_RUN:
        log.warning("DRY_RUN=1 → no se escribirá nada en Supabase.")
        return
    missing = []
    if not SUPABASE_URL:
        missing.append("SUPABASE_URL")
    if not SUPABASE_KEY:
        missing.append("SUPABASE_SERVICE_ROLE_KEY")
    if missing:
        raise RuntimeError(f"Faltan variables de entorno requeridas: {', '.join(missing)}")


def make_db_client() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def normalize_vtex_money(value: Any) -> float | None:
    """VTEX orderForm monetary values are returned in cents."""
    try:
        if value is None:
            return None
        return round(float(value) / 100.0, 2)
    except (TypeError, ValueError):
        return None


def safe_pct_change(old: float, new: float) -> float:
    if abs(old) < 0.0001:
        return 0.0
    pct = ((new - old) / old) * 100
    return max(-9999.99, min(9999.99, round(pct, 2)))

# ── VTEX API Configuration ───────────────────────────────────────────────────

# Many Uruguayan supermarket chains run on the VTEX e‑commerce platform.  VTEX
# exposes an unauthenticated, public API for its catalogue and checkout logic
# which returns well structured JSON, including the EAN barcode for each
# product, brand information, image URLs and both regular and promotional
# prices.  Leveraging this API yields significantly more data than scraping
# HTML pages and is dramatically faster because we avoid parsing full page
# layouts.  See analisis_scraping_supermercados_uy.html for more details.

# Map each chain to its VTEX base domain.  When adding support for new VTEX
# chains simply add an entry here.  The scraper will automatically build
# API endpoints from these domains.
VTEX_CHAINS: dict[str, str] = {
    "disco":  "disco.com.uy",
    "devoto": "devoto.com.uy",
    "geant":  "geant.com.uy",
    "tata":   "tata.com.uy",
}

# Maximum number of items returned per search request.  VTEX accepts values up
# to ~50.  Using the maximum reduces the number of HTTP requests required to
# enumerate large categories, improving throughput dramatically.  Adjust if
# servers begin throttling.
VTEX_PAGE_SIZE = 50


async def vtex_list_categories(client: httpx.AsyncClient, base_url: str) -> list[dict]:
    """List all third‑level categories from the VTEX catalogue tree.

    The endpoint `/api/catalog_system/pub/category/tree/3` returns a nested
    structure of categories.  Each entry has the keys `id`, `name` and
    potentially a `children` array.  Only the leaf categories (no further
    children) are scraped because they hold actual products.  Returns a list
    of objects with `id` and `name`.

    Args:
        client: an `httpx.AsyncClient` instance.
        base_url: e.g. "disco.com.uy".  Do not include protocol or trailing
            slash.  The function will prepend `https://`.

    Returns:
        A list of dictionaries with keys `id` (int) and `name` (str).
    """
    url = f"https://{base_url}/api/catalog_system/pub/category/tree/3"
    try:
        r = await client.get(url, headers=JSON_HEADERS, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        data = r.json()  # expected to be a list
    except Exception as e:
        log.error(f"[vtex][{base_url}] Error listing categories: {e}")
        return []

    categories: list[dict] = []

    def collect(leaves: list[dict]):
        for c in leaves:
            # If the category has children then recurse, otherwise collect it
            if c.get("children"):
                collect(c["children"])
            else:
                cid = c.get("id")
                name = c.get("name") or c.get("Title")
                if cid is not None and name:
                    categories.append({"id": cid, "name": name})

    if isinstance(data, list):
        collect(data)

    # Deduplicate by id
    uniq = {}
    for cat in categories:
        uniq[cat["id"]] = cat
    return list(uniq.values())


async def vtex_fetch_category_products(client: httpx.AsyncClient, base_url: str, category_id: int) -> list[dict]:
    """Fetch all products within a VTEX category.

    VTEX exposes a search endpoint that accepts a category filter via the
    `fq=C:{category_id}` query parameter.  Pagination is controlled by the
    `_from` and `_to` parameters, which are zero‑based inclusive indexes.
    Example: `_from=0&_to=49` returns the first `50` products.  We loop until
    an empty response is returned.

    The response is a JSON array of products.  Each product contains basic
    information (`productId`, `productName`, `brand`, etc.) and one or more
    items (variants) under the `items` field.  Each item has an `itemId`
    (this is the SKU), an `images` list and an array of `sellers` which
    includes `commertialOffer` objects containing `Price` and `ListPrice`.

    Args:
        client: an `httpx.AsyncClient` instance.
        base_url: e.g. "disco.com.uy".
        category_id: the integer id of the category obtained from
            `vtex_list_categories`.

    Returns:
        A list of dictionaries with basic product and SKU information.  Price
        and stock are not included here; they are fetched separately by
        `vtex_fetch_prices`.
    """
    products: list[dict] = []
    start = 0
    while True:
        end = start + VTEX_PAGE_SIZE - 1
        url = (
            f"https://{base_url}/api/catalog_system/pub/products/search/"
            f"?fq=C:{category_id}&_from={start}&_to={end}"
        )
        try:
            r = await client.get(url, headers=JSON_HEADERS, timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            log.warning(f"[vtex][{base_url}] cat {category_id} range {start}-{end}: {e}")
            break
        if not data:
            break
        for prod in data:
            prod_id = prod.get("productId")
            name = prod.get("productName") or prod.get("productNameComplete")
            brand = prod.get("brand") or prod.get("brandName") or ""
            for item in prod.get("items", []):
                sku_id = item.get("itemId")
                ean = None
                # Extract EAN from variations or additionalInfo when present
                # The EAN is often stored in the `referenceId` list
                reference_ids = item.get("referenceId") or []
                for ref in reference_ids:
                    if ref.get("Key", "").lower() in {"ean", "ean13", "gtin"}:
                        ean = ref.get("Value")
                        break
                # Choose the first image URL if available
                images = item.get("images") or []
                img_url = images[0].get("imageUrl", "") if images else ""
                products.append({
                    "product_id": prod_id,
                    "sku_id": sku_id,
                    "name": name,
                    "brand": brand,
                    "ean": ean,
                    "image_url": img_url,
                    "category_id": category_id,
                })
        start += VTEX_PAGE_SIZE
    return products


async def vtex_fetch_prices(client: httpx.AsyncClient, base_url: str, skus: list[str]) -> dict[str, tuple[float, float, bool]]:
    """Fetch current prices and stock availability for a list of SKU IDs.

    The `/api/checkout/pub/orderForms/simulation` endpoint accepts a JSON
    payload with an `items` list.  Each item must contain `id` (SKU),
    `quantity` and `seller` (usually "1").  The response contains an
    `items` array with `price`, `listPrice` and `availability` flags.

    To avoid extremely large payloads and potential rate limiting this
    function processes the input SKUs in batches of 50.  Returns a mapping
    of SKU IDs to a triple `(price, list_price, available)`.

    Args:
        client: an `httpx.AsyncClient` instance.
        base_url: e.g. "disco.com.uy".
        skus: list of SKU strings.

    Returns:
        Dict mapping sku_id to (price, list_price, available).  If a price is
        missing or invalid it will be omitted from the result mapping.
    """
    results: dict[str, tuple[float, float, bool]] = {}
    batch_size = 50
    for i in range(0, len(skus), batch_size):
        batch = skus[i : i + batch_size]
        payload = {
            "items": [
                {"id": sku, "quantity": 1, "seller": "1"}
                for sku in batch
            ]
        }
        url = f"https://{base_url}/api/checkout/pub/orderForms/simulation"
        try:
            r = await client.post(url, headers=POST_JSON_HEADERS, json=payload, timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            data = r.json()
            for item in data.get("items", []):
                sku = item.get("id")
                price = item.get("price")
                list_price = item.get("listPrice") or price
                avail = item.get("availability", "") == "available"
                if sku and isinstance(price, (int, float)):
                    results[str(sku)] = (
                        normalize_vtex_money(price),
                        normalize_vtex_money(list_price) if list_price is not None else normalize_vtex_money(price),
                        avail,
                    )
        except Exception as e:
            log.warning(f"[vtex][{base_url}] price fetch batch starting {i}: {e}")
    return results


async def scrape_vtex_chain(chain: str, domain: str) -> list[dict]:
    """Scrape a full VTEX chain by listing categories, enumerating products and
    fetching their prices.

    This function orchestrates calls to `vtex_list_categories`,
    `vtex_fetch_category_products` and `vtex_fetch_prices`.  Categories are
    processed concurrently to maximise throughput.  After retrieving basic
    product information for all categories, price and availability data
    are fetched in batches via the simulation endpoint.  Results are
    deduplicated by SKU ID and filtered for price sanity.

    Args:
        chain: the chain identifier (e.g. "disco").  Will be stored in the
            resulting rows.
        domain: the VTEX base domain (e.g. "disco.com.uy").

    Returns:
        List of dictionaries ready for DB insertion with the fields:
        `ean`, `chain`, `product_id`, `sku_id`, `name`, `brand`, `category`,
        `image_url`, `price`, `list_price`, `available` and `scraped_at`.
    """
    log.info(f"[{chain}] VTEX scrape starting")
    rows: list[dict] = []
    if not ENABLE_VTEX_ATTEMPT:
        log.info(f"[{chain}] VTEX deshabilitado por configuración")
        return rows
    async with build_async_client() as client:
        categories = await vtex_list_categories(client, domain)
        if not categories:
            log.warning(f"[{chain}] No categories returned from VTEX API, falling back to legacy scraper")
            return rows

        # Build tasks to fetch products per category concurrently.  Use a
        # semaphore to limit concurrency to 5 categories at a time to avoid
        # overwhelming the server.
        sem = asyncio.Semaphore(5)

        async def fetch_cat(cat):
            async with sem:
                cid = cat["id"]
                cname = cat["name"]
                prods = await vtex_fetch_category_products(client, domain, cid)
                for p in prods:
                    p["category"] = cname
                return prods

        tasks = [fetch_cat(cat) for cat in categories]
        all_prods_lists = await asyncio.gather(*tasks, return_exceptions=True)
        basic_products: list[dict] = []
        for res in all_prods_lists:
            if isinstance(res, Exception):
                log.warning(f"[{chain}] Category fetch error: {res}")
            else:
                basic_products.extend(res)

        if not basic_products:
            log.warning(f"[{chain}] No products found via VTEX API")
            return rows

        # Fetch prices for all SKUs
        sku_ids = list({str(p["sku_id"]) for p in basic_products if p.get("sku_id")})
        price_map = await vtex_fetch_prices(client, domain, sku_ids)

        for prod in basic_products:
            sku_id = str(prod.get("sku_id"))
            price_tuple = price_map.get(sku_id)
            # Skip products with missing or invalid price
            if not price_tuple:
                continue
            price, list_price, available = price_tuple
            if price is None:
                continue
            # Filter out crazy prices
            if not (0 < price < MAX_PRICE):
                continue
            ean = prod.get("ean")
            rows.append({
                "ean":        ean,
                "chain":      chain,
                "product_id": prod.get("product_id"),
                "sku_id":     sku_id,
                "name":       (prod.get("name") or "")[:200],
                "brand":      (prod.get("brand") or "")[:100],
                "category":   prod.get("category") or "",
                "image_url":  (prod.get("image_url") or "")[:500],
                "price":      price,
                "list_price": list_price,
                "available":  available,
                "scraped_at": now_iso(),
            })
    # Deduplicate final rows by sku_id
    deduped = {r["sku_id"]: r for r in rows}
    unique_rows = list(deduped.values())
    log.info(f"[{chain}] VTEX scrape finished: {len(unique_rows)} unique items")
    return unique_rows


async def scrape_chain_with_fallback(
    chain: str,
    domain: str,
    fallback_func,
    *fallback_args,
) -> dict:
    """Intenta VTEX y cae al scraper legado si no devuelve filas."""
    if ENABLE_VTEX_ATTEMPT:
        try:
            rows = await scrape_vtex_chain(chain, domain)
            if rows:
                return {"rows": rows, "source": "vtex"}
        except Exception as e:
            log.warning(f"[{chain}] VTEX scrape error, falling back: {e}")
    rows = await fallback_func(*fallback_args)
    return {"rows": rows, "source": fallback_func.__name__}


# ── Utilidades comunes ────────────────────────────────────────────────────────

def parse_price(raw: str) -> float | None:
    s = re.sub(r'[^0-9.,]', '', raw).strip()
    if not s:
        return None
    if ',' in s and '.' in s:
        s = s.replace('.', '').replace(',', '.')
    elif ',' in s:
        s = s.replace(',', '.')
    elif s.count('.') > 1:
        s = s.replace('.', '')
    try:
        v = float(s)
        return v if 0 < v < MAX_PRICE else None
    except ValueError:
        return None


def make_sku_id(slug_or_name: str, chain: str) -> str:
    slug = slug_or_name.strip().rstrip('/')
    # Tomar el último segmento de la URL/slug
    parts = [p for p in slug.split('/') if p]
    if parts:
        candidate = parts[-1]
        if len(candidate) > 3:
            return candidate[:120]
    return hashlib.md5(f"{chain}:{slug_or_name}".encode()).hexdigest()[:20]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── GDU: Disco / Devoto / Géant (HTML parsing) ───────────────────────────────

GDU_CHAINS = {
    "disco":  "https://www.disco.com.uy",
    "devoto": "https://www.devoto.com.uy",
    "geant":  "https://www.geant.com.uy",
}

# Categorías confirmadas (probadas en probe_apis4.py)
GDU_CATEGORIES =[
    ("almacen",           "almacen"),
    ("bebidas",           "bebidas"),
    ("frescos",           "frescos"),
    ("limpieza",          "perfumeria-y-limpieza"),
    ("mascotas",          "mascotas"),
    ("congelados",        "congelados"),
    ("lacteos",           "lacteos"),
    ("carniceria",        "carniceria"),
    ("panaderia",         "panaderia"),
]


def _parse_gdu_page(html: str, chain: str, category: str) -> list[dict]:
    """Extrae productos de una página HTML de Disco/Devoto/Géant."""
    soup = BeautifulSoup(html, "lxml")
    products =[]

    for card in soup.select("div.product-item"):
        # Nombre
        name = ""
        h3 = card.find("h3")
        if h3:
            name = h3.get_text(strip=True)
        if not name:
            a = card.find("a", title=True)
            if a:
                name = a.get("title", "").strip()

        # Precio: span.mon + span.val dentro de div.product-prices
        price_str = ""
        prices_div = card.select_one("div.product-prices")
        if prices_div:
            mon = prices_div.select_one("span.mon")
            val = prices_div.select_one("span.val")
            mon_txt = mon.get_text(strip=True) if mon else "$"
            val_txt = val.get_text(strip=True) if val else ""
            price_str = mon_txt + val_txt

        # Precio de lista (tachado) si existe
        list_price_str = ""
        list_div = card.select_one("div.product-prices-before")
        if list_div:
            lv = list_div.select_one("span.val")
            if lv:
                list_price_str = "$" + lv.get_text(strip=True)

        # Href → sku_id
        href = ""
        link = card.select_one("a[href*='/product']")
        if link:
            href = link.get("href", "")

        # Imagen
        img = ""
        img_el = card.find("img")
        if img_el:
            img = img_el.get("src") or img_el.get("data-src") or ""

        price = parse_price(price_str)
        if not name or price is None:
            continue

        list_price = parse_price(list_price_str) or price
        sku_id = make_sku_id(href or name, chain)

        products.append({
            "ean":        None,
            "chain":      chain,
            "product_id": sku_id,
            "sku_id":     sku_id,
            "name":       name[:200],
            "brand":      "",
            "category":   category,
            "image_url":  img[:500],
            "price":      price,
            "list_price": list_price,
            "available":  True,
            "scraped_at": now_iso(),
        })

    return products


async def _fetch_gdu_page(
    client: httpx.AsyncClient,
    url: str,
    chain: str,
    cat_name: str,
    page_num: int,
    cat_slug: str,
    base_url: str,
) -> str | None:
    """Descarga una página GDU con reintentos ante respuesta vacía."""
    headers = {**HTML_HEADERS, "Referer": f"{base_url}/{cat_slug}"}
    for attempt in range(1, GDU_RETRIES + 2):  # intento 1, 2, 3
        try:
            r = await client.get(url, headers=headers)
            r.raise_for_status()
            # Verificar que la respuesta tiene productos antes de devolverla
            if "product-item" in r.text:
                return r.text
            # Página llegó pero sin productos — esperar antes de reintentar
            if attempt <= GDU_RETRIES:
                log.debug(f"[{chain}][{cat_name}] p{page_num} sin productos (intento {attempt}), esperando {GDU_RETRY_WAIT}s...")
                await asyncio.sleep(GDU_RETRY_WAIT)
            else:
                return r.text  # devolver igual en el último intento para que el caller decida
        except Exception as e:
            log.warning(f"[{chain}][{cat_name}] p{page_num} intento {attempt} error: {e}")
            if attempt <= GDU_RETRIES:
                await asyncio.sleep(GDU_RETRY_WAIT)
    return None


async def scrape_gdu_category(
    client: httpx.AsyncClient,
    chain: str,
    base_url: str,
    cat_name: str,
    cat_slug: str,
) -> list[dict]:
    rows: list[dict] =[]
    seen_skus: set  = set()
    consec_empty    = 0

    for page_num in range(1, MAX_PAGES + 1):
        url = f"{base_url}/products/category/{cat_slug}/{page_num}"

        html = await _fetch_gdu_page(client, url, chain, cat_name, page_num, cat_slug, base_url)
        if html is None:
            consec_empty += 1
            if consec_empty >= MAX_CONSEC_EMPTY:
                break
            continue

        products = _parse_gdu_page(html, chain, cat_name)

        new_in_page = 0
        for p in products:
            if p["sku_id"] not in seen_skus:
                seen_skus.add(p["sku_id"])
                rows.append(p)
                new_in_page += 1

        if new_in_page == 0:
            consec_empty += 1
            log.debug(f"[{chain}][{cat_name}] p{page_num}: vacía ({consec_empty}/{MAX_CONSEC_EMPTY})")
            if consec_empty >= MAX_CONSEC_EMPTY:
                log.info(f"[{chain}][{cat_name}] {MAX_CONSEC_EMPTY} páginas vacías → fin (total={len(rows)})")
                break
        else:
            consec_empty = 0
            log.info(f"[{chain}][{cat_name}] p{page_num}: +{new_in_page} (total={len(rows)})")

        await asyncio.sleep(GDU_PAGE_DELAY)

    return rows


async def scrape_gdu_chain(chain: str, base_url: str) -> list[dict]:
    log.info(f"[{chain}] Iniciando (httpx HTML)")
    all_rows: list[dict] = []

    async with build_async_client(timeout=20) as client:
        sem = asyncio.Semaphore(GDU_CATEGORY_CONCURRENCY)

        async def run_category(cat_name: str, cat_slug: str) -> list[dict]:
            async with sem:
                try:
                    rows = await scrape_gdu_category(client, chain, base_url, cat_name, cat_slug)
                    log.info(f"[{chain}][{cat_name}] ✓ {len(rows)}")
                    return rows
                except Exception as e:
                    log.error(f"[{chain}][{cat_name}] FALLO: {e}")
                    return []

        results = await asyncio.gather(
            *(run_category(cat_name, cat_slug) for cat_name, cat_slug in GDU_CATEGORIES)
        )
        for rows in results:
            all_rows.extend(rows)

    seen: dict = {}
    for r in all_rows:
        seen[r["sku_id"]] = r
    unique = list(seen.values())
    log.info(f"[{chain}] Total únicos: {len(unique)}")
    return unique


# ── TaTa (GraphQL POST) ───────────────────────────────────────────────────────

TATA_URL = "https://www.tata.com.uy/api/graphql"

TATA_HEADERS = {
    "User-Agent": UA,
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Origin": "https://www.tata.com.uy",
    "Referer": "https://www.tata.com.uy/almacen",
}

TATA_QUERY = """query ProductsQuery($first: Int!, $after: String, $sort: StoreSort, $term: String, $selectedFacets: [IStoreSelectedFacet!]) {
  search(first: $first, after: $after, sort: $sort, term: $term, selectedFacets: $selectedFacets) {
    products {
      pageInfo { totalCount }
      edges {
        node {
          id
          slug
          sku
          name
          gtin
          brand { name brandName }
          isVariantOf { name productGroupID }
          image { url alt }
          offers {
            lowPrice
            offers {
              price
              listPrice
              availability
              seller { identifier }
            }
          }
        }
      }
    }
  }
}"""

# Categorías confirmadas con totalCount > 0
TATA_CATEGORIES =[
    ("almacen",     "almacen"),
    ("frescos",     "frescos"),
    ("bebidas",     "bebidas"),
    ("limpieza",    "limpieza"),
    ("perfumeria",  "perfumeria"),
    ("congelados",  "congelados"),
    ("mascotas",    "mascotas"),
    ("bebes",       "bebes"),
]

TATA_PAGE_SIZE = 50   # máximo razonable por request


async def _tata_fetch_page(
    client: httpx.AsyncClient,
    cat_facet: str,
    after: int,
) -> list | None:
    variables = {
        "first": TATA_PAGE_SIZE,
        "after": str(after),
        "sort": "score_desc",
        "term": "",
        "selectedFacets": [{"key": "category-1", "value": cat_facet}],
    }
    payload = {"operationName": "ProductsQuery", "variables": variables, "query": TATA_QUERY}

    for attempt in range(1, 4):  # hasta 3 intentos
        try:
            r = await client.post(TATA_URL, json=payload, headers=TATA_HEADERS)
            if r.status_code == 500:
                wait = attempt * 10.0
                log.warning(f"[tata][{cat_facet}] after={after} 500, esperando {wait}s (intento {attempt}/3)")
                await asyncio.sleep(wait)
                continue
            r.raise_for_status()
            data = r.json()
            if "errors" in data:
                log.warning(f"[tata] GraphQL errors: {data['errors']}")
                return None
            return data["data"]["search"]["products"]["edges"]
        except Exception as e:
            wait = attempt * 5.0
            log.warning(f"[tata][{cat_facet}] after={after} intento {attempt} error: {e}, esperando {wait}s")
            await asyncio.sleep(wait)
    return None


def _tata_node_to_row(node: dict, category: str) -> dict | None:
    name = node.get("name", "").strip()
    if not name:
        return None

    price = None
    list_price = None
    available = False
    offers_data = node.get("offers", {})

    low = offers_data.get("lowPrice")
    if low is not None:
        try:
            price = float(low)
        except (TypeError, ValueError):
            pass

    raw_offers = offers_data.get("offers", [])
    if raw_offers:
        o = raw_offers[0]
        try:
            lp = float(o.get("listPrice", 0))
            list_price = lp if lp > 0 else price
        except (TypeError, ValueError):
            list_price = price
        available = o.get("availability", "") == "https://schema.org/InStock"

    if price is None or price <= 0 or price >= MAX_PRICE:
        return None
    if list_price is None:
        list_price = price

    gtin = node.get("gtin") or None
    # Prefer GTIN or SKU first over dynamic slug if possible
    sku_id = gtin or node.get("sku") or node.get("slug") or hashlib.md5(name.encode()).hexdigest()[:20]
    brand = (node.get("brand") or {}).get("name", "") or (node.get("brand") or {}).get("brandName", "")
    img_list = node.get("image") or []
    img = img_list[0].get("url", "") if img_list else ""

    return {
        "ean":        gtin,
        "chain":      "tata",
        "product_id": sku_id,
        "sku_id":     sku_id,
        "name":       name[:200],
        "brand":      brand[:100],
        "category":   category,
        "image_url":  img[:500],
        "price":      price,
        "list_price": list_price,
        "available":  available,
        "scraped_at": now_iso(),
    }


async def scrape_tata_category(
    client: httpx.AsyncClient,
    cat_name: str,
    cat_facet: str,
) -> list[dict]:
    rows: list[dict] =[]
    seen_skus: set  = set()
    after = 0

    while True:
        edges = await _tata_fetch_page(client, cat_facet, after)
        if edges is None:
            log.warning(f"[tata][{cat_name}] after={after}: error, abortando categoría")
            break
        if not edges:
            log.info(f"[tata][{cat_name}] after={after}: sin más productos → fin ({len(rows)} total)")
            break

        new_in_page = 0
        for edge in edges:
            node = edge.get("node", {})
            row = _tata_node_to_row(node, cat_name)
            if row and row["sku_id"] not in seen_skus:
                seen_skus.add(row["sku_id"])
                rows.append(row)
                new_in_page += 1

        log.info(f"[tata][{cat_name}] after={after}: +{new_in_page} (total={len(rows)})")

        if len(edges) < TATA_PAGE_SIZE:
            # Última página
            break

        after += TATA_PAGE_SIZE
        await asyncio.sleep(0.3)

    return rows


async def scrape_tata() -> list[dict]:
    log.info("[tata] Iniciando (GraphQL POST)")
    all_rows: list[dict] = []

    async with build_async_client(timeout=30) as client:
        sem = asyncio.Semaphore(TATA_CATEGORY_CONCURRENCY)

        async def run_category(cat_name: str, cat_facet: str) -> list[dict]:
            async with sem:
                try:
                    rows = await scrape_tata_category(client, cat_name, cat_facet)
                    log.info(f"[tata][{cat_name}] ✓ {len(rows)}")
                    return rows
                except Exception as e:
                    log.error(f"[tata][{cat_name}] FALLO: {e}")
                    return []

        results = await asyncio.gather(
            *(run_category(cat_name, cat_facet) for cat_name, cat_facet in TATA_CATEGORIES)
        )
        for rows in results:
            all_rows.extend(rows)

    seen: dict = {}
    for r in all_rows:
        seen[r["sku_id"]] = r
    unique = list(seen.values())
    log.info(f"[tata] Total únicos: {len(unique)}")
    return unique


# ── Tienda Inglesa (sin cambios desde v4) ────────────────────────────────────

TI_HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html",
    "Accept-Language": "es-UY,es;q=0.9",
}

TI_SEARCH_TERMS = [
    ("bebidas", ["agua mineral", "jugo", "refresco", "cerveza", "vino", "gaseosa"]),
    ("lacteos", ["leche", "queso", "yogur", "manteca", "crema de leche"]),
    ("limpieza", ["detergente", "lavandina", "jabon loza", "suavizante", "desengrasante"]),
    ("perfumeria", ["shampoo", "desodorante", "jabon liquido", "crema corporal", "pasta dental"]),
    ("congelados", ["helado", "pizza congelada", "empanada congelada", "milanesa congelada"]),
    ("bebes", ["pañales", "toallitas humedas", "formula infantil", "papilla"]),
    ("mascotas", ["racion perro", "racion gato", "arena sanitaria", "snack perro"]),
    ("frescos", ["manzana", "banana", "tomate", "pollo", "carne picada"]),
]


async def _parse_ti_card(card, cat_name: str) -> dict | None:
    href = card.get("href", "")
    if not href:
        return None

    img = ""
    title_candidates = []

    for attr in ("title", "aria-label", "data-name"):
        value = (card.get(attr) or "").strip()
        if value:
            title_candidates.append(value)

    img_el = card.find("img")
    if img_el:
        img = img_el.get("src") or img_el.get("data-src") or ""
        alt = (img_el.get("alt") or "").strip()
        if alt:
            title_candidates.append(alt)

    text = re.sub(r"\s+", " ", card.get_text(" ", strip=True)).strip()
    price_matches = re.findall(r"\$\s*[\d\.,]+", text)
    if not price_matches:
        return None

    price = parse_price(price_matches[0])
    if price is None:
        return None

    list_price = parse_price(price_matches[1]) if len(price_matches) > 1 else price
    if list_price is None or list_price < price:
        list_price = price

    name = ""
    for candidate in title_candidates:
        candidate = re.sub(r"\s+", " ", candidate).strip(" -")
        if candidate and "$" not in candidate and len(candidate) >= 3:
            name = candidate
            break

    if not name:
        name = re.split(r"\$\s*[\d\.,]+", text)[0].strip(" -")

    for junk in ("Agregar", "Comprar", "Añadir", "Ver detalle"):
        name = name.replace(junk, "").strip()

    if len(name) < 3:
        name = text[:200]

    prod_id = href.split("?")[-1].split(",")[0] if "?" in href else make_sku_id(href or name, "tienda_inglesa")

    return {
        "ean": None,
        "chain": "tienda_inglesa",
        "product_id": prod_id,
        "sku_id": prod_id,
        "name": name[:200],
        "brand": "",
        "category": cat_name,
        "image_url": img[:500],
        "price": price,
        "list_price": list_price,
        "available": True,
        "scraped_at": now_iso(),
    }


async def scrape_tienda_inglesa() -> list[dict]:
    chain = "tienda_inglesa"
    rows: list[dict] =[]
    seen_ids: set   = set()
    log.info(f"[{chain}] Iniciando")

    async with build_async_client(headers=TI_HEADERS, timeout=20) as client:

        # Almacén: paginación directa
        page_num, consec_empty, seen_hrefs = 0, 0, set()
        while consec_empty < 2 and page_num < TI_ALMACEN_MAX_PAGES:
            url = (f"https://www.tiendainglesa.com.uy/supermercado/categoria"
                   f"/almacen/busqueda?0,0,*:*,78,0,0,,,false,,,,{page_num}")
            try:
                r = await client.get(url)
                if r.status_code == 403:
                    break
                r.raise_for_status()
            except Exception as e:
                log.warning(f"[{chain}][almacen] p{page_num}: {e}")
                break

            soup  = BeautifulSoup(r.text, "lxml")
            cards = soup.select("a[href*='.producto']")
            hrefs = frozenset(c.get("href", "") for c in cards)
            if hrefs and hrefs.issubset(seen_hrefs):
                break
            seen_hrefs.update(hrefs)

            found = 0
            for card in cards:
                prod = await _parse_ti_card(card, "almacen")
                if prod and prod["sku_id"] not in seen_ids:
                    seen_ids.add(prod["sku_id"])
                    rows.append(prod)
                    found += 1

            log.info(f"[{chain}][almacen] p{page_num}: +{found} (total={len(rows)})")
            consec_empty = 0 if found > 0 else consec_empty + 1
            page_num += 1
            await asyncio.sleep(0.35)

        # Otras categorías: búsqueda por términos
        for cat_name, terms in TI_SEARCH_TERMS:
            cat_found = 0
            for term in terms:
                encoded = term.replace(" ", "+")
                for pg in range(0, TI_TERM_MAX_PAGES):
                    url = (f"https://www.tiendainglesa.com.uy/supermercado/busqueda"
                           f"?0,0,{encoded},0,0,0,,,false,,,{pg}")
                    try:
                        r = await client.get(url)
                        if r.status_code in (403, 404):
                            break
                        r.raise_for_status()
                    except Exception as e:
                        log.warning(f"[{chain}][{cat_name}][{term}] p{pg}: {e}")
                        break

                    soup  = BeautifulSoup(r.text, "lxml")
                    cards = soup.select("a[href*='.producto']")
                    if not cards:
                        break

                    found = 0
                    for card in cards:
                        prod = await _parse_ti_card(card, cat_name)
                        if prod and prod["sku_id"] not in seen_ids:
                            seen_ids.add(prod["sku_id"])
                            rows.append(prod)
                            found += 1
                            cat_found += 1
                    if found == 0:
                        break
                    await asyncio.sleep(0.3)

            log.info(f"[{chain}][{cat_name}] via búsqueda: {cat_found}")

    log.info(f"[{chain}] Total: {len(rows)}")
    return rows


# ── Supabase ──────────────────────────────────────────────────────────────────

def upsert_prices(sb: Client, rows: list, chain: str) -> dict:
    """Inserta y actualiza en lote. Bypassea límite de 1,000 filas de PostgREST y evita bloat exponencial."""
    if not rows:
        return {"inserted": 0, "updated": 0, "unchanged": 0, "errors": 0}

    stats = {"inserted": 0, "updated": 0, "unchanged": 0, "errors": 0}

    # Deduplicar en memoria
    deduped = {r["sku_id"]: r for r in rows}
    deduped_list = list(deduped.values())

    price_changes =[]
    BATCH = 500

    for i in range(0, len(deduped_list), BATCH):
        batch = deduped_list[i: i + BATCH]
        batch_skus = [r["sku_id"] for r in batch]
        
        # 1. Obtener precios existentes SOLAMENTE para los SKUs en este batch (Bypassea límite de 1,000)
        existing = {}
        try:
            resp = sb.table("prices_current").select("sku_id,price") \
                     .eq("chain", chain).in_("sku_id", batch_skus).execute()
            for row in resp.data:
                existing[row["sku_id"]] = float(row["price"])
        except Exception as e:
            log.warning(f"No se pudo cargar precios actuales para {chain} batch {i//BATCH}: {e}")

        history_batch = []
        current_batch =[]

        # 2. Comparar y categorizar datos
        for row in batch:
            old = existing.get(row["sku_id"])
            if old is None:
                stats["inserted"] += 1
                history_batch.append(row)   # Guardar historial si es NUEVO
                current_batch.append(row)
            elif abs(old - row["price"]) > 0.01:
                stats["updated"] += 1
                history_batch.append(row)   # Guardar historial si CAMBIÓ EL PRECIO
                current_batch.append(row)
                
                pct = safe_pct_change(old, row["price"])
                price_changes.append({
                    "chain":      row["chain"],
                    "sku_id":     row["sku_id"],
                    "name":       row["name"],
                    "old_price":  old,
                    "new_price":  row["price"],
                    "pct_change": pct,
                    "detected_at": now_iso(),
                })
            else:
                stats["unchanged"] += 1
                current_batch.append(row)   # Se hace upsert a current para actualizar "scraped_at"

        # 3. Ejecución Independiente de Queries
        if history_batch:
            try:
                sb.table("prices_history").insert(history_batch).execute()
            except Exception as e:
                log.error(f"Error insertando history batch {i//BATCH}: {e}")
                stats["errors"] += len(history_batch)

        if current_batch:
            try:
                sb.table("prices_current").upsert(current_batch, on_conflict="chain,sku_id").execute()
            except Exception as e:
                log.error(f"Error upserting current batch {i//BATCH}: {e}")
                stats["errors"] += len(current_batch)

    # 4. Insertar notificaciones de cambios de precio (en batches seguros)
    for i in range(0, len(price_changes), BATCH):
        try:
            sb.table("price_changes").insert(price_changes[i:i+BATCH]).execute()
        except Exception as e:
            log.warning(f"Error insertando price_changes: {e}")

    return stats


def log_run(sb: Client, chain: str, run_start: datetime, rows: list, stats: dict, elapsed: float, status: str):
    try:
        sb.table("scrape_logs").insert({
            "chain":         chain,
            "run_at":        run_start.isoformat(),
            "total_scraped": len(rows),
            "inserted":      stats["inserted"],
            "updated":       stats["updated"],
            "unchanged":     stats["unchanged"],
            "errors":        stats["errors"],
            "elapsed_s":     elapsed,
            "status":        status,
        }).execute()
    except Exception as e:
        log.error(f"Error log {chain}: {e}")


def write_local_summary(results: list[dict], run_start: datetime, total_elapsed: float) -> str:
    payload = {
        "run_started_at": run_start.isoformat(),
        "elapsed_s": round(total_elapsed, 2),
        "results": results,
    }
    path = os.path.abspath("scrape_summary.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
    return path


def write_github_step_summary(results: list[dict], run_start: datetime, total_elapsed: float) -> None:
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return

    total_scraped   = sum(r["scraped"] for r in results)
    total_inserted  = sum(r["inserted"] for r in results)
    total_updated   = sum(r["updated"] for r in results)
    total_unchanged = sum(r["unchanged"] for r in results)
    total_errors    = sum(r["errors"] for r in results)

    lines = [
        "## Scraper run summary\n",
        f"- **Run started:** `{run_start.isoformat()}`\n",
        f"- **Elapsed:** `{round(total_elapsed, 1)}s`\n",
        f"- **Chains:** `{len(results)}`\n",
        f"- **Scraped total:** `{total_scraped}`\n",
        f"- **New:** `{total_inserted}`\n",
        f"- **Updated:** `{total_updated}`\n",
        f"- **Unchanged:** `{total_unchanged}`\n",
        f"- **Errors:** `{total_errors}`\n\n",
        "| Chain | Source | Scraped | New | Updated | Unchanged | Errors | Elapsed (s) | Status |\n",
        "|---|---|---:|---:|---:|---:|---:|---:|---|\n",
    ]
    for r in sorted(results, key=lambda x: x["chain"]):
        lines.append(
            f"| {r['chain']} | {r['source']} | {r['scraped']} | {r['inserted']} | "
            f"{r['updated']} | {r['unchanged']} | {r['errors']} | {r['elapsed']:.1f} | {r['status']} |\n"
        )

    try:
        with open(summary_path, "a", encoding="utf-8") as fh:
            fh.writelines(lines)
        log.info(f"GitHub Actions summary escrito en: {summary_path}")
    except Exception as e:
        log.warning(f"No se pudo escribir GITHUB_STEP_SUMMARY: {e}")


def print_run_summary(results: list[dict], run_start: datetime, total_elapsed: float) -> None:
    total_scraped   = sum(r["scraped"] for r in results)
    total_inserted  = sum(r["inserted"] for r in results)
    total_updated   = sum(r["updated"] for r in results)
    total_unchanged = sum(r["unchanged"] for r in results)
    total_errors    = sum(r["errors"] for r in results)

    log.info("")
    log.info("=" * 104)
    log.info("RESUMEN GENERAL DEL RUN")
    log.info("=" * 104)
    log.info(
        f"Inicio: {run_start.isoformat()} | Chains: {len(results)} | Scraped total: {total_scraped} | "
        f"New: {total_inserted} | Updated: {total_updated} | Unchanged: {total_unchanged} | "
        f"Errors: {total_errors} | Elapsed: {round(total_elapsed, 1)}s"
    )
    log.info("-" * 104)
    log.info(
        f"{'CHAIN':<18} {'SOURCE':<20} {'SCRAPED':>8} {'NEW':>8} {'UPDATED':>9} "
        f"{'UNCHANGED':>11} {'ERRORS':>8} {'ELAPSED(s)':>11} {'STATUS':>10}"
    )
    log.info("-" * 104)
    for r in sorted(results, key=lambda x: x["chain"]):
        log.info(
            f"{r['chain']:<18} {r['source']:<20} {r['scraped']:>8} {r['inserted']:>8} "
            f"{r['updated']:>9} {r['unchanged']:>11} {r['errors']:>8} {r['elapsed']:>11.1f} {r['status']:>10}"
        )
    log.info("=" * 104)

    try:
        summary_json = write_local_summary(results, run_start, total_elapsed)
        log.info(f"Resumen JSON escrito en: {summary_json}")
    except Exception as e:
        log.warning(f"No se pudo escribir scrape_summary.json: {e}")

    write_github_step_summary(results, run_start, total_elapsed)


def normalize_scrape_result(result: Any, default_source: str) -> tuple[list[dict], str]:
    if isinstance(result, dict) and "rows" in result:
        return result.get("rows", []) or [], str(result.get("source") or default_source)
    return result or [], default_source


# ── Wrapper Concurrente ───────────────────────────────────────────────────────

async def run_and_save_chain(chain: str, run_start: datetime, scraper_func, *args):
    """Ejecuta un scraper, guarda en Supabase y devuelve stats para resumen global."""
    t0 = time.time()
    rows: list[dict] = []
    stats = {"inserted": 0, "updated": 0, "unchanged": 0, "errors": 0}
    source = scraper_func.__name__

    try:
        result = await scraper_func(*args)
        rows, source = normalize_scrape_result(result, source)

        if DRY_RUN:
            log.info(f"[{chain}] DRY_RUN activo → omitiendo escritura en Supabase")
        else:
            chain_sb = make_db_client()
            stats = await asyncio.to_thread(upsert_prices, chain_sb, rows, chain)
    except Exception as e:
        log.error(f"[{chain}] FALLO CRITICO: {e}")
        stats["errors"] += 1

    elapsed = round(time.time() - t0, 1)
    if stats["errors"] > 0 and len(rows) == 0:
        status = "failed"
    elif stats["errors"] > 0:
        status = "partial"
    elif len(rows) == 0:
        status = "empty"
    else:
        status = "ok"

    if not DRY_RUN:
        try:
            log_sb = make_db_client()
            await asyncio.to_thread(log_run, log_sb, chain, run_start, rows, stats, elapsed, status)
        except Exception as e:
            log.error(f"[{chain}] Error escribiendo scrape_logs: {e}")

    log.info(
        f"[{chain}] FINALIZADO: source={source} scraped={len(rows)} new={stats.get('inserted', 0)} "
        f"updated={stats.get('updated', 0)} unchanged={stats.get('unchanged', 0)} "
        f"errors={stats.get('errors', 0)} status={status} ({elapsed}s)"
    )

    return {
        "chain": chain,
        "source": source,
        "scraped": len(rows),
        "inserted": stats.get("inserted", 0),
        "updated": stats.get("updated", 0),
        "unchanged": stats.get("unchanged", 0),
        "errors": stats.get("errors", 0),
        "elapsed": elapsed,
        "status": status,
    }


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    started_at = time.time()
    log.info("=" * 60 + " scraper v6.0")
    validate_runtime_config()
    run_start = datetime.now(timezone.utc)

    tasks = [
        run_and_save_chain(
            "disco",
            run_start,
            scrape_chain_with_fallback,
            "disco",
            VTEX_CHAINS["disco"],
            scrape_gdu_chain,
            "disco",
            GDU_CHAINS["disco"],
        ),
        run_and_save_chain(
            "devoto",
            run_start,
            scrape_chain_with_fallback,
            "devoto",
            VTEX_CHAINS["devoto"],
            scrape_gdu_chain,
            "devoto",
            GDU_CHAINS["devoto"],
        ),
        run_and_save_chain(
            "geant",
            run_start,
            scrape_chain_with_fallback,
            "geant",
            VTEX_CHAINS["geant"],
            scrape_gdu_chain,
            "geant",
            GDU_CHAINS["geant"],
        ),
        run_and_save_chain(
            "tata",
            run_start,
            scrape_chain_with_fallback,
            "tata",
            VTEX_CHAINS["tata"],
            scrape_tata,
        ),
        run_and_save_chain("tienda_inglesa", run_start, scrape_tienda_inglesa),
    ]

    results = await asyncio.gather(*tasks)
    total_elapsed = time.time() - started_at
    print_run_summary(results, run_start, total_elapsed)
    log.info("=" * 60 + " FIN")


if __name__ == "__main__":
    asyncio.run(main())
