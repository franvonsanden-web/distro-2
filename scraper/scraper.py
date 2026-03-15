"""
scraper.py v4 — precios-uy
Fixes vs v3:
  1. Precio con overflow → validación + cap a $999,999
  2. Tienda Inglesa loop infinito → stop cuando página repite los mismos hrefs
  3. TaTa sku_id colapsado → mejor extracción desde href FastStore
"""

import os, time, asyncio, logging, re, hashlib
import httpx
from datetime import datetime, timezone
from playwright.async_api import async_playwright, Page
from bs4 import BeautifulSoup
from supabase import create_client, Client

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

# ── Precio máximo razonable en pesos uruguayos ────────────────────────────────
MAX_PRICE = 999_999.0   # $999.999 UYU — cualquier cosa mayor es un error de parseo

# ── Categorías ────────────────────────────────────────────────────────────────
DISCO_CATEGORIES = [
    ("almacen",    "almacen"),
    ("frescos",    "frescos"),
    ("bebidas",    "bebidas"),
    ("congelados", "products/category/congelados/6"),
    ("limpieza",   "products/category/perfumeria-y-limpieza/12"),
    ("mascotas",   "mascotas"),
    ("bebes",      "products/category/bebes/100"),
    ("hogar",      "hogar"),
]
DEVOTO_CATEGORIES = DISCO_CATEGORIES
GEANT_CATEGORIES  = DISCO_CATEGORIES

TATA_CATEGORIES = [
    ("almacen",    "almacen"),
    ("frescos",    "frescos"),
    ("bebidas",    "bebidas"),
    ("congelados", "congelados"),
    ("limpieza",   "limpieza"),
    ("perfumeria", "perfumeria"),
    ("mascotas",   "mascotas"),
    ("bebes",      "bebes"),
    ("hogar",      "hogar"),
]

VTEX_CHAINS = {
    "disco":   ("https://www.disco.com.uy",  DISCO_CATEGORIES),
    "devoto":  ("https://www.devoto.com.uy", DEVOTO_CATEGORIES),
    "geant":   ("https://www.geant.com.uy",  GEANT_CATEGORIES),
    "tata":    ("https://www.tata.com.uy",   TATA_CATEGORIES),
}

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36"
MAX_PAGES  = 50
PAGE_WAIT  = 5000
ITEM_WAIT  = 4000

VTEX_IO_SELECTORS = [
    "section.vtex-product-summary-2-x-container",
    "article[class*='vtex-product-summary']",
    "div.vtex-search-result-3-x-galleryItem",
    "[class*='productSummary']",
    ".product-item",
]

FASTSTORE_SELECTORS = [
    "article[class*='product-card-module--fs-product-card']",
    "article[class*='fs-product-card']",
    "li[class*='product-grid-module']",
    "div.plp__shelf-products-container article",
    "ul[class*='product-grid'] li",
]


# ── Precio parsing con validación ────────────────────────────────────────────

def parse_price(raw: str) -> float | None:
    """Convierte '$1.234,56' → 1234.56. Rechaza valores > MAX_PRICE."""
    s = re.sub(r'[^0-9.,]', '', raw).strip()
    if not s:
        return None
    if ',' in s and '.' in s:
        # formato UY: 1.234,56
        s = s.replace('.', '').replace(',', '.')
    elif ',' in s:
        s = s.replace(',', '.')
    elif s.count('.') > 1:
        # 1.234.567 → miles sin decimales
        s = s.replace('.', '')
    try:
        v = float(s)
    except ValueError:
        return None
    if v <= 0 or v > MAX_PRICE:
        return None   # FIX: descarta precios inválidos / overflow
    return v


# ── VTEX Playwright ───────────────────────────────────────────────────────────

async def find_selector(page: Page, selectors: list, timeout: int = ITEM_WAIT) -> str | None:
    for sel in selectors:
        try:
            await page.wait_for_selector(sel, timeout=timeout)
            if await page.locator(sel).count() > 0:
                return sel
        except:
            continue
    return None


async def extract_products(page: Page, selector: str) -> list[dict]:
    return await page.evaluate("""(sel) => {
        const cards = document.querySelectorAll(sel);
        const results = [];
        cards.forEach(card => {
            const nameEl =
                card.querySelector('[class*="productName"]') ||
                card.querySelector('[class*="product-name"]') ||
                card.querySelector('[class*="fs-product-card__title"]') ||
                card.querySelector('[class*="ProductCard_title"]') ||
                card.querySelector('[class*="title"]') ||
                card.querySelector('h3') || card.querySelector('h2') ||
                card.querySelector('a[title]');

            let name = '';
            if (nameEl) name = (nameEl.title || nameEl.innerText || '').trim();

            const priceEl =
                card.querySelector('[class*="sellingPriceValue"]') ||
                card.querySelector('[class*="sellingPrice"]') ||
                card.querySelector('[class*="selling-price"]') ||
                card.querySelector('[class*="price__selling"]') ||
                card.querySelector('[class*="fs-price"]') ||
                card.querySelector('[class*="Price_selling"]') ||
                card.querySelector('[class*="bestPrice"]') ||
                card.querySelector('[class*="price"]');

            const priceRaw = priceEl ? priceEl.innerText.trim() : '';

            const imgEl =
                card.querySelector('img[class*="image"]') ||
                card.querySelector('img[class*="Image"]') ||
                card.querySelector('img');
            const img = imgEl ? (imgEl.src || imgEl.dataset?.src || '') : '';

            // FIX TaTa: recoger TODOS los hrefs de la card, no solo el primero
            const links = Array.from(card.querySelectorAll('a[href]'))
                .map(a => a.href)
                .filter(h => h && !h.includes('javascript'));
            const href = links.find(h => h.includes('/p') || h.length > 30) || links[0] || '';

            if (name && priceRaw) {
                results.push({ name, price_raw: priceRaw, image_url: img, href });
            }
        });
        return results;
    }""", selector)


def make_sku_id(name: str, href: str, chain: str) -> str:
    """
    Genera un sku_id robusto.
    Prioriza el slug del href. Fallback: hash del nombre.
    FIX TaTa: FastStore usa URLs tipo /almacen/arroz-gallo-1kg-123456/p
    """
    if href:
        # Extraer slug: último segmento antes de /p o ?
        path = href.rstrip('/').split('?')[0]
        segments = [s for s in path.split('/') if s]
        if segments:
            slug = segments[-1]
            if slug == 'p' and len(segments) > 1:
                slug = segments[-2]
            if len(slug) > 3:
                return slug[:120]
    # Fallback: hash determinístico del nombre+cadena
    return hashlib.md5(f"{chain}:{name}".encode()).hexdigest()[:20]


async def scrape_category(
    page: Page,
    base_url: str,
    chain: str,
    cat_name: str,
    cat_slug: str,
    selectors: list,
) -> list[dict]:
    rows      = []
    seen_skus = set()

    for page_num in range(1, MAX_PAGES + 1):
        sep = "?" if "?" not in cat_slug else "&"
        url = f"{base_url}/{cat_slug}{sep + 'page=' + str(page_num) if page_num > 1 else ''}"
        log.info(f"[{chain}][{cat_name}] p{page_num} → {url}")

        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(PAGE_WAIT)
            for _ in range(3):
                await page.evaluate("window.scrollBy(0, window.innerHeight * 2)")
                await page.wait_for_timeout(700)
        except Exception as e:
            log.warning(f"[{chain}][{cat_name}] Error navegando p{page_num}: {e}")
            break

        sel = await find_selector(page, selectors)
        if not sel:
            if page_num == 1:
                log.warning(f"[{chain}][{cat_name}] Sin productos en p1 — skip")
            else:
                log.info(f"[{chain}][{cat_name}] Sin más productos en p{page_num} — fin")
            break

        products = await extract_products(page, sel)
        if not products:
            log.info(f"[{chain}][{cat_name}] 0 productos en p{page_num} — fin")
            break

        new_in_page = 0
        for p in products:
            price = parse_price(p["price_raw"])
            if price is None:
                continue

            sku_id = make_sku_id(p["name"], p.get("href", ""), chain)

            if sku_id in seen_skus:
                continue
            seen_skus.add(sku_id)
            new_in_page += 1

            rows.append({
                "ean":        None,
                "chain":      chain,
                "product_id": sku_id,
                "sku_id":     sku_id,
                "name":       p["name"][:200],
                "brand":      "",
                "category":   cat_name,
                "image_url":  p.get("image_url", "")[:500],
                "price":      price,
                "list_price": price,
                "available":  True,
                "scraped_at": datetime.now(timezone.utc).isoformat(),
            })

        log.info(f"[{chain}][{cat_name}] p{page_num}: {new_in_page} nuevos / {len(products)} vistos / total={len(rows)}")

        if new_in_page == 0:
            log.info(f"[{chain}][{cat_name}] Solo duplicados en p{page_num} — fin")
            break

        await asyncio.sleep(0.6)

    return rows


async def scrape_vtex_chain(chain: str, base_url: str, categories: list) -> list[dict]:
    all_rows  = []
    selectors = FASTSTORE_SELECTORS if chain == "tata" else VTEX_IO_SELECTORS
    log.info(f"[{chain}] Iniciando")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(
            user_agent=UA,
            viewport={"width": 1280, "height": 900},
            locale="es-UY",
            extra_http_headers={"Accept-Language": "es-UY,es;q=0.9"},
        )
        page = await ctx.new_page()

        for cat_name, cat_slug in categories:
            rows = await scrape_category(page, base_url, chain, cat_name, cat_slug, selectors)
            all_rows.extend(rows)
            log.info(f"[{chain}][{cat_name}] ✓ {len(rows)} — acumulado: {len(all_rows)}")
            await asyncio.sleep(0.5)

        await browser.close()

    seen = {}
    for r in all_rows:
        seen[r["sku_id"]] = r
    unique = list(seen.values())
    log.info(f"[{chain}] Total únicos: {len(unique)}")
    return unique


# ── Tienda Inglesa ────────────────────────────────────────────────────────────

# Términos de búsqueda por categoría para TI (las URLs con ID solo funcionan en almacén)
TI_SEARCH_TERMS = [
    ("bebidas",    ["agua mineral", "jugo", "refresco", "cerveza", "vino", "gaseosa"]),
    ("lacteos",    ["leche", "queso", "yogur", "manteca", "crema de leche"]),
    ("limpieza",   ["detergente", "lavandina", "jabon loza", "suavizante", "desengrasante"]),
    ("perfumeria", ["shampoo", "desodorante", "jabon liquido", "crema corporal", "pasta dental"]),
    ("congelados", ["helado", "pizza congelada", "empanada congelada", "milanesa congelada"]),
]

TI_CATEGORIES = [
    ("almacen", 78),   # único que funciona con paginación directa
]

TI_HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html",
    "Accept-Language": "es-UY,es;q=0.9",
}


async def _parse_ti_card(card, cat_name: str, chain: str) -> dict | None:
    """Parsea una card HTML de Tienda Inglesa."""
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
        "ean": None, "chain": chain, "product_id": prod_id, "sku_id": prod_id,
        "name": name[:200], "brand": "", "category": cat_name, "image_url": "",
        "price": price, "list_price": price, "available": True,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    }


async def scrape_tienda_inglesa() -> list[dict]:
    """
    TI v2: almacén via paginación directa + otras categorías via búsqueda por términos.
    """
    chain = "tienda_inglesa"
    rows  = []
    seen_ids: set = set()
    log.info(f"[{chain}] Iniciando v2")

    async with httpx.AsyncClient(headers=TI_HEADERS, timeout=20, follow_redirects=True) as client:

        # ── Almacén: paginación directa (ID 78 funciona) ──
        page_num, consec_empty, seen_hrefs = 0, 0, set()
        while consec_empty < 2 and page_num < 200:
            url = (f"https://www.tiendainglesa.com.uy/supermercado/categoria"
                   f"/almacen/busqueda?0,0,*:*,78,0,0,,,false,,,,{page_num}")
            try:
                r = await client.get(url)
                if r.status_code == 403: break
                r.raise_for_status()
            except Exception as e:
                log.warning(f"[{chain}][almacen] p{page_num}: {e}"); break

            soup  = BeautifulSoup(r.text, "lxml")
            cards = soup.select("a[href*='.producto']")[:40]
            hrefs = frozenset(c.get("href","") for c in cards)
            if hrefs and hrefs.issubset(seen_hrefs):
                log.info(f"[{chain}][almacen] Hrefs repetidos en p{page_num} — fin")
                break
            seen_hrefs.update(hrefs)

            found = 0
            for card in cards:
                prod = await _parse_ti_card(card, "almacen", chain)
                if prod and prod["sku_id"] not in seen_ids:
                    seen_ids.add(prod["sku_id"])
                    rows.append(prod)
                    found += 1
            log.info(f"[{chain}][almacen] p{page_num}: {found} (total={len(rows)})")
            consec_empty = 0 if found > 0 else consec_empty + 1
            page_num += 1
            await asyncio.sleep(0.35)

        # ── Otras categorías: búsqueda por términos ──
        for cat_name, terms in TI_SEARCH_TERMS:
            cat_found = 0
            for term in terms:
                encoded = term.replace(" ", "+")
                for pg in range(0, 15):
                    url = (f"https://www.tiendainglesa.com.uy/supermercado/busqueda"
                           f"?0,0,{encoded},0,0,0,,,false,,,{pg}")
                    try:
                        r = await client.get(url)
                        if r.status_code in (403, 404): break
                        r.raise_for_status()
                    except Exception as e:
                        log.warning(f"[{chain}][{cat_name}][{term}] p{pg}: {e}"); break

                    soup  = BeautifulSoup(r.text, "lxml")
                    cards = soup.select("a[href*='.producto']")[:40]
                    if not cards: break

                    found = 0
                    for card in cards:
                        prod = await _parse_ti_card(card, cat_name, chain)
                        if prod and prod["sku_id"] not in seen_ids:
                            seen_ids.add(prod["sku_id"])
                            rows.append(prod)
                            found += 1
                            cat_found += 1
                    if found == 0: break
                    await asyncio.sleep(0.3)

            log.info(f"[{chain}][{cat_name}] via búsqueda: {cat_found} productos")

    log.info(f"[{chain}] Total: {len(rows)}")
    return rows
# ── Supabase ──────────────────────────────────────────────────────────────────

def upsert_prices(sb: Client, rows: list, chain: str) -> dict:
    if not rows:
        return {"inserted": 0, "updated": 0, "unchanged": 0, "errors": 0}

    stats    = {"inserted": 0, "updated": 0, "unchanged": 0, "errors": 0}
    existing = {}
    try:
        resp = sb.table("prices_current").select("sku_id,price").eq("chain", chain).execute()
        for row in resp.data:
            existing[row["sku_id"]] = float(row["price"])
    except Exception as e:
        log.warning(f"No se pudo cargar precios actuales para {chain}: {e}")

    price_changes = []
    BATCH = 500

    # Deduplicar por sku_id dentro del mismo envío (evita ON CONFLICT error)
    deduped_map: dict = {}
    for row in rows:
        deduped_map[row["sku_id"]] = row
    deduped = list(deduped_map.values())

    for i in range(0, len(deduped), BATCH):
        batch = deduped[i : i + BATCH]
        for row in batch:
            old = existing.get(row["sku_id"])
            if old is None:
                stats["inserted"] += 1
            elif abs(old - row["price"]) > 0.01:
                stats["updated"] += 1
                pct = ((row["price"] - old) / old) * 100
                price_changes.append({
                    "chain": row["chain"], "sku_id": row["sku_id"],
                    "name": row["name"], "old_price": old,
                    "new_price": row["price"],
                    "pct_change": max(-9999.99, min(9999.99, round(pct, 2))),
                    "detected_at": datetime.now(timezone.utc).isoformat(),
                })
            else:
                stats["unchanged"] += 1

        try:
            sb.table("prices_history").insert(batch).execute()
            sb.table("prices_current").upsert(batch, on_conflict="chain,sku_id").execute()
        except Exception as e:
            log.error(f"Error upsert batch {i//BATCH}: {e}")
            stats["errors"] += len(batch)

    if price_changes:
        try:
            sb.table("price_changes").insert(price_changes).execute()
        except Exception as e:
            log.warning(f"Error price_changes: {e}")

    return stats


def log_run(sb, chain, run_start, rows, stats, elapsed):
    try:
        sb.table("scrape_logs").insert({
            "chain": chain, "run_at": run_start.isoformat(),
            "total_scraped": len(rows), "inserted": stats["inserted"],
            "updated": stats["updated"], "unchanged": stats["unchanged"],
            "errors": stats["errors"], "elapsed_s": elapsed,
            "status": "ok" if stats["errors"] == 0 else "partial",
        }).execute()
    except Exception as e:
        log.error(f"Error log {chain}: {e}")


# ── Main ──────────────────────────────────────────────────────────────────────

async def main_async():
    log.info("=" * 60 + " precios-uy v4")
    sb        = create_client(SUPABASE_URL, SUPABASE_KEY)
    run_start = datetime.now(timezone.utc)

    for chain, (base_url, categories) in VTEX_CHAINS.items():
        t0 = time.time()
        try:
            rows  = await scrape_vtex_chain(chain, base_url, categories)
            stats = upsert_prices(sb, rows, chain)
        except Exception as e:
            log.error(f"[{chain}] FALLO: {e}")
            rows, stats = [], {"inserted":0,"updated":0,"unchanged":0,"errors":1}
        elapsed = round(time.time() - t0, 1)
        log_run(sb, chain, run_start, rows, stats, elapsed)
        log.info(f"[{chain}] scraped={len(rows)} new={stats['inserted']} updated={stats['updated']} ({elapsed}s)")

    t0 = time.time()
    try:
        rows  = await scrape_tienda_inglesa()
        stats = upsert_prices(sb, rows, "tienda_inglesa")
    except Exception as e:
        log.error(f"[tienda_inglesa] FALLO: {e}")
        rows, stats = [], {"inserted":0,"updated":0,"unchanged":0,"errors":1}
    elapsed = round(time.time() - t0, 1)
    log_run(sb, "tienda_inglesa", run_start, rows, stats, elapsed)
    log.info(f"[tienda_inglesa] scraped={len(rows)} new={stats['inserted']} ({elapsed}s)")
    log.info("=" * 60 + " FIN")


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
