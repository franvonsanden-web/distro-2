"""
scraper.py v5 — distro
═══════════════════════════════════════════════════════════════════
Cambios vs v4:
  • Disco / Devoto / Géant → httpx + BeautifulSoup (HTML parsing)
    Sin Playwright. 9 categorías × ~45 páginas ≈ 8.000+ prods/cadena.
  • TaTa → httpx + GraphQL POST
    Sin Playwright. 8 categorías, ~10.000 productos.
  • Tienda Inglesa → igual que v4 (httpx + BeautifulSoup)
  • Playwright eliminado por completo → más rápido y más estable.
═══════════════════════════════════════════════════════════════════
"""

import os, time, asyncio, logging, re, hashlib
import httpx
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from supabase import create_client, Client

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

MAX_PRICE        = 999_999.0
MAX_PAGES        = 150       # límite de seguridad
MAX_CONSEC_EMPTY = 3         # páginas vacías consecutivas antes de parar
GDU_PAGE_DELAY   = 1.5       # segundos entre páginas (evita throttling desde datacenter)
GDU_RETRY_WAIT   = 8.0       # espera antes de reintentar una página vacía
GDU_RETRIES      = 2         # reintentos por página vacía antes de contarla como vacía

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36"

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
GDU_CATEGORIES = [
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
    products = []

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
    rows: list[dict] = []
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

    async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
        for cat_name, cat_slug in GDU_CATEGORIES:
            rows = await scrape_gdu_category(client, chain, base_url, cat_name, cat_slug)
            all_rows.extend(rows)
            log.info(f"[{chain}][{cat_name}] ✓ {len(rows)} — acumulado: {len(all_rows)}")
            await asyncio.sleep(0.4)

    # Deduplicar por sku_id
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
TATA_CATEGORIES = [
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
    sku_id = node.get("slug") or node.get("sku") or hashlib.md5(name.encode()).hexdigest()[:20]
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
    rows: list[dict] = []
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

    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        for cat_name, cat_facet in TATA_CATEGORIES:
            rows = await scrape_tata_category(client, cat_name, cat_facet)
            all_rows.extend(rows)
            log.info(f"[tata][{cat_name}] ✓ {len(rows)} — acumulado: {len(all_rows)}")
            await asyncio.sleep(0.5)

    # Deduplicar
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
    ("bebidas",    ["agua mineral", "jugo", "refresco", "cerveza", "vino", "gaseosa"]),
    ("lacteos",    ["leche", "queso", "yogur", "manteca", "crema de leche"]),
    ("limpieza",   ["detergente", "lavandina", "jabon loza", "suavizante", "desengrasante"]),
    ("perfumeria", ["shampoo", "desodorante", "jabon liquido", "crema corporal", "pasta dental"]),
    ("congelados", ["helado", "pizza congelada", "empanada congelada", "milanesa congelada"]),
]


async def _parse_ti_card(card, cat_name: str) -> dict | None:
    name_el = card.select_one("span, div")
    name = name_el.get_text(strip=True) if name_el else card.get_text(" ", strip=True)
    if not name or len(name) < 3:
        return None

    price_el = card.find(string=lambda t: t and "$" in t)
    if not price_el:
        for sib in card.next_siblings:
            txt = getattr(sib, "get_text", lambda **k: "")()
            if "$" in txt:
                price_el = txt
                break
    if not price_el:
        return None

    price = parse_price(str(price_el))
    if price is None:
        return None

    href = card.get("href", "")
    prod_id = href.split("?")[-1].split(",")[0] if "?" in href else name[:50]

    return {
        "ean": None, "chain": "tienda_inglesa",
        "product_id": prod_id, "sku_id": prod_id,
        "name": name[:200], "brand": "", "category": cat_name,
        "image_url": "", "price": price, "list_price": price,
        "available": True, "scraped_at": now_iso(),
    }


async def scrape_tienda_inglesa() -> list[dict]:
    chain = "tienda_inglesa"
    rows: list[dict] = []
    seen_ids: set   = set()
    log.info(f"[{chain}] Iniciando")

    async with httpx.AsyncClient(headers=TI_HEADERS, timeout=20, follow_redirects=True) as client:

        # Almacén: paginación directa
        page_num, consec_empty, seen_hrefs = 0, 0, set()
        while consec_empty < 2 and page_num < 200:
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
            cards = soup.select("a[href*='.producto']")[:40]
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
                for pg in range(0, 15):
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
                    cards = soup.select("a[href*='.producto']")[:40]
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
    if not rows:
        return {"inserted": 0, "updated": 0, "unchanged": 0, "errors": 0}

    stats = {"inserted": 0, "updated": 0, "unchanged": 0, "errors": 0}

    # Cargar precios actuales para detectar cambios
    existing: dict = {}
    try:
        resp = sb.table("prices_current").select("sku_id,price").eq("chain", chain).execute()
        for row in resp.data:
            existing[row["sku_id"]] = float(row["price"])
    except Exception as e:
        log.warning(f"No se pudo cargar precios actuales de {chain}: {e}")

    # Deduplicar
    deduped: dict = {}
    for row in rows:
        deduped[row["sku_id"]] = row
    deduped_list = list(deduped.values())

    price_changes: list = []
    BATCH = 500

    for i in range(0, len(deduped_list), BATCH):
        batch = deduped_list[i: i + BATCH]

        for row in batch:
            old = existing.get(row["sku_id"])
            if old is None:
                stats["inserted"] += 1
            elif abs(old - row["price"]) > 0.01:
                stats["updated"] += 1
                pct = ((row["price"] - old) / old) * 100
                price_changes.append({
                    "chain":      row["chain"],
                    "sku_id":     row["sku_id"],
                    "name":       row["name"],
                    "old_price":  old,
                    "new_price":  row["price"],
                    "pct_change": max(-9999.99, min(9999.99, round(pct, 2))),
                    "detected_at": now_iso(),
                })
            else:
                stats["unchanged"] += 1

        try:
            sb.table("prices_history").insert(batch).execute()
            sb.table("prices_current").upsert(batch, on_conflict="chain,sku_id").execute()
        except Exception as e:
            log.error(f"Error upsert batch {i // BATCH}: {e}")
            stats["errors"] += len(batch)

    if price_changes:
        try:
            sb.table("price_changes").insert(price_changes).execute()
        except Exception as e:
            log.warning(f"Error price_changes: {e}")

    return stats


def log_run(sb: Client, chain: str, run_start: datetime, rows: list, stats: dict, elapsed: float):
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
            "status":        "ok" if stats["errors"] == 0 else "partial",
        }).execute()
    except Exception as e:
        log.error(f"Error log {chain}: {e}")


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    log.info("=" * 60 + " scraper v5")
    sb        = create_client(SUPABASE_URL, SUPABASE_KEY)
    run_start = datetime.now(timezone.utc)

    # GDU chains (Disco, Devoto, Géant)
    for chain, base_url in GDU_CHAINS.items():
        t0 = time.time()
        try:
            rows  = await scrape_gdu_chain(chain, base_url)
            stats = upsert_prices(sb, rows, chain)
        except Exception as e:
            log.error(f"[{chain}] FALLO: {e}")
            rows, stats = [], {"inserted": 0, "updated": 0, "unchanged": 0, "errors": 1}
        elapsed = round(time.time() - t0, 1)
        log_run(sb, chain, run_start, rows, stats, elapsed)
        log.info(f"[{chain}] scraped={len(rows)} new={stats['inserted']} updated={stats['updated']} ({elapsed}s)")

    # TaTa
    t0 = time.time()
    try:
        rows  = await scrape_tata()
        stats = upsert_prices(sb, rows, "tata")
    except Exception as e:
        log.error(f"[tata] FALLO: {e}")
        rows, stats = [], {"inserted": 0, "updated": 0, "unchanged": 0, "errors": 1}
    elapsed = round(time.time() - t0, 1)
    log_run(sb, "tata", run_start, rows, stats, elapsed)
    log.info(f"[tata] scraped={len(rows)} new={stats['inserted']} updated={stats['updated']} ({elapsed}s)")

    # Tienda Inglesa
    t0 = time.time()
    try:
        rows  = await scrape_tienda_inglesa()
        stats = upsert_prices(sb, rows, "tienda_inglesa")
    except Exception as e:
        log.error(f"[tienda_inglesa] FALLO: {e}")
        rows, stats = [], {"inserted": 0, "updated": 0, "unchanged": 0, "errors": 1}
    elapsed = round(time.time() - t0, 1)
    log_run(sb, "tienda_inglesa", run_start, rows, stats, elapsed)
    log.info(f"[tienda_inglesa] scraped={len(rows)} new={stats['inserted']} ({elapsed}s)")

    log.info("=" * 60 + " FIN")


if __name__ == "__main__":
    asyncio.run(main())
