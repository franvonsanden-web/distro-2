"""
scraper.py v3 — precios-uy
- Async Playwright (más rápido)
- Paginación real via ?page=N (no solo scroll)
- TaTa FastStore: selector correcto
- URLs de categorías corregidas con IDs reales
- Tienda Inglesa: async requests
"""

import os, time, asyncio, logging, re
import httpx
from datetime import datetime, timezone
from playwright.async_api import async_playwright, Page
from bs4 import BeautifulSoup
from supabase import create_client, Client

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

# ── Categorías por cadena ─────────────────────────────────────────────────────
# Disco / Devoto / Géant — URLs reales descubiertas via DOM del menú
DISCO_CATEGORIES = [
    ("almacen",             "almacen"),
    ("frescos",             "frescos"),
    ("bebidas",             "bebidas"),
    ("congelados",          "products/category/congelados/6"),
    ("limpieza",            "products/category/perfumeria-y-limpieza/12"),
    ("mascotas",            "mascotas"),
    ("bebes",               "products/category/bebes/100"),
    ("hogar",               "hogar"),
]

# Devoto y Géant comparten la misma estructura que Disco
DEVOTO_CATEGORIES = DISCO_CATEGORIES
GEANT_CATEGORIES  = DISCO_CATEGORIES

# TaTa usa FastStore — slugs simples funcionan
TATA_CATEGORIES = [
    ("almacen",     "almacen"),
    ("frescos",     "frescos"),
    ("bebidas",     "bebidas"),
    ("congelados",  "congelados"),
    ("limpieza",    "limpieza"),
    ("perfumeria",  "perfumeria"),
    ("mascotas",    "mascotas"),
    ("bebes",       "bebes"),
    ("hogar",       "hogar"),
]

VTEX_CHAINS = {
    "disco":   ("https://www.disco.com.uy",  DISCO_CATEGORIES),
    "devoto":  ("https://www.devoto.com.uy", DEVOTO_CATEGORIES),
    "geant":   ("https://www.geant.com.uy",  GEANT_CATEGORIES),
    "tata":    ("https://www.tata.com.uy",   TATA_CATEGORIES),
}

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36"
MAX_PAGES   = 50     # límite de seguridad por categoría
PAGE_WAIT   = 5000   # ms tras goto
ITEM_WAIT   = 4000   # ms esperando selector


# ── Selectores por tipo de stack ──────────────────────────────────────────────

# VTEX IO (Disco, Devoto, Géant) — versión moderna
VTEX_IO_SELECTORS = [
    "section.vtex-product-summary-2-x-container",
    "article[class*='vtex-product-summary']",
    "div.vtex-search-result-3-x-galleryItem",
    "[class*='productSummary']",
    ".product-item",
]

# VTEX FastStore (TaTa)
FASTSTORE_SELECTORS = [
    "article[class*='product-card-module--fs-product-card']",
    "article[class*='fs-product-card']",
    "li[class*='product-grid-module']",
    "div.plp__shelf-products-container article",
    "ul[class*='product-grid'] li",
]


async def find_selector(page: Page, selectors: list[str], timeout: int = ITEM_WAIT) -> str | None:
    """Devuelve el primer selector que matchea en la página."""
    for sel in selectors:
        try:
            await page.wait_for_selector(sel, timeout=timeout)
            count = await page.locator(sel).count()
            if count > 0:
                return sel
        except:
            continue
    return None


async def extract_products(page: Page, selector: str, chain: str, cat: str) -> list[dict]:
    """Extrae productos de la página actual usando el selector detectado."""
    return await page.evaluate("""([sel, chain, cat]) => {
        const cards = document.querySelectorAll(sel);
        const results = [];

        cards.forEach(card => {
            // ── Nombre ──
            const nameEl =
                card.querySelector('[class*="productName"]') ||
                card.querySelector('[class*="product-name"]') ||
                card.querySelector('[class*="fs-product-card__title"]') ||
                card.querySelector('[class*="ProductCard_title"]') ||
                card.querySelector('h3') || card.querySelector('h2') ||
                card.querySelector('a[title]');
            
            let name = nameEl
                ? (nameEl.title || nameEl.innerText || '').trim()
                : '';
            if (!name && nameEl) name = nameEl.getAttribute('title') || '';

            // ── Precio ──
            const priceEl =
                card.querySelector('[class*="sellingPriceValue"]') ||
                card.querySelector('[class*="sellingPrice"]') ||
                card.querySelector('[class*="selling-price"]') ||
                card.querySelector('[class*="price__selling"]') ||
                card.querySelector('[class*="fs-price"]') ||
                card.querySelector('[class*="Price_selling"]') ||
                card.querySelector('[class*="price"]');
            
            const priceRaw = priceEl ? priceEl.innerText.trim() : '';

            // ── Imagen ──
            const imgEl =
                card.querySelector('img[class*="image"]') ||
                card.querySelector('img[class*="Image"]') ||
                card.querySelector('img');
            const img = imgEl ? (imgEl.src || imgEl.dataset?.src || '') : '';

            // ── Link / SKU ──
            const linkEl = card.querySelector('a[href]');
            const href   = linkEl ? linkEl.href : '';

            if (name && priceRaw) {
                results.push({ name, price_raw: priceRaw, image_url: img, href });
            }
        });

        return results;
    }""", [selector, chain, cat])


def parse_price(raw: str) -> float | None:
    """Convierte '$1.234,56' o '$ 1234' a float."""
    s = re.sub(r'[^0-9.,]', '', raw).strip()
    # Formato uruguayo: punto=miles, coma=decimal
    if ',' in s and '.' in s:
        s = s.replace('.', '').replace(',', '.')
    elif ',' in s:
        s = s.replace(',', '.')
    elif s.count('.') > 1:
        s = s.replace('.', '')
    try:
        return float(s)
    except:
        return None


async def scrape_category(
    page: Page,
    base_url: str,
    chain: str,
    cat_name: str,
    cat_slug: str,
    selectors: list[str],
) -> list[dict]:
    """Scrapea todas las páginas de una categoría con paginación ?page=N."""
    rows      = []
    seen_skus = set()

    for page_num in range(1, MAX_PAGES + 1):
        url = f"{base_url}/{cat_slug}{'?page=' + str(page_num) if page_num > 1 else ''}"
        log.info(f"[{chain}][{cat_name}] p{page_num} → {url}")

        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(PAGE_WAIT)

            # Scroll para trigger lazy load
            for _ in range(3):
                await page.evaluate("window.scrollBy(0, window.innerHeight * 2)")
                await page.wait_for_timeout(800)
        except Exception as e:
            log.warning(f"[{chain}][{cat_name}] Error navegando p{page_num}: {e}")
            break

        # Detectar selector
        sel = await find_selector(page, selectors)
        if not sel:
            if page_num == 1:
                log.warning(f"[{chain}][{cat_name}] Sin productos en p1 — skip categoría")
            else:
                log.info(f"[{chain}][{cat_name}] Sin más productos en p{page_num} — fin")
            break

        products = await extract_products(page, sel, chain, cat_name)
        if not products:
            log.info(f"[{chain}][{cat_name}] 0 productos en p{page_num} — fin")
            break

        new_in_page = 0
        for p in products:
            price = parse_price(p["price_raw"])
            if price is None:
                continue

            href   = p.get("href", "")
            sku_id = href.rstrip("/").split("/")[-1].split("?")[0] or p["name"][:50]

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

        # Si la página trajo 0 productos nuevos (duplicados) → fin de paginación
        if new_in_page == 0:
            log.info(f"[{chain}][{cat_name}] Solo duplicados en p{page_num} — fin")
            break

        await asyncio.sleep(0.6)

    return rows


async def scrape_vtex_chain(chain: str, base_url: str, categories: list) -> list[dict]:
    """Lanza el browser y scrapea todas las categorías de una cadena."""
    all_rows = []
    selectors = FASTSTORE_SELECTORS if chain == "tata" else VTEX_IO_SELECTORS

    log.info(f"[{chain}] Iniciando (async Playwright)")

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
            log.info(f"[{chain}][{cat_name}] ✓ {len(rows)} productos — acumulado: {len(all_rows)}")
            await asyncio.sleep(0.5)

        await browser.close()

    # Deduplicar globalmente
    seen = {}
    for r in all_rows:
        seen[r["sku_id"]] = r
    unique = list(seen.values())
    log.info(f"[{chain}] Total únicos: {len(unique)}")
    return unique


# ── Tienda Inglesa (httpx async) ──────────────────────────────────────────────

TI_CATEGORIES = [
    ("almacen",    78),
    ("bebidas",    79),
    ("lacteos",    80),
    ("carnes",     81),
    ("verduleria", 82),
    ("limpieza",   83),
    ("perfumeria", 84),
    ("congelados", 85),
]

TI_HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html",
    "Accept-Language": "es-UY,es;q=0.9",
}


async def scrape_tienda_inglesa() -> list[dict]:
    chain = "tienda_inglesa"
    rows  = []
    log.info(f"[{chain}] Iniciando")

    async with httpx.AsyncClient(headers=TI_HEADERS, timeout=20, follow_redirects=True) as client:
        for cat_name, cat_id in TI_CATEGORIES:
            page_num = 0
            empty    = 0

            while empty < 2:
                url = (
                    f"https://www.tiendainglesa.com.uy/supermercado/categoria"
                    f"/{cat_name}/busqueda?0,0,*:*,{cat_id},0,0,,,false,,,,{page_num}"
                )
                try:
                    r = await client.get(url)
                    if r.status_code == 403:
                        log.warning(f"[{chain}][{cat_name}] 403 — stop")
                        break
                    r.raise_for_status()
                except Exception as e:
                    log.warning(f"[{chain}][{cat_name}] Error p{page_num}: {e}")
                    break

                soup  = BeautifulSoup(r.text, "lxml")
                cards = soup.select("a[href*='.producto']")[:40]

                found = 0
                for card in cards:
                    name_el = card.select_one("span, div")
                    name    = name_el.get_text(strip=True) if name_el else card.get_text(" ", strip=True)
                    if not name or len(name) < 3:
                        continue

                    price_el = card.find(string=lambda t: t and "$" in t)
                    if not price_el:
                        for sib in card.next_siblings:
                            txt = getattr(sib, "get_text", lambda **k: "")()
                            if "$" in txt:
                                price_el = txt
                                break
                    if not price_el:
                        continue

                    price = parse_price(str(price_el))
                    if price is None:
                        continue

                    href    = card.get("href", "")
                    prod_id = href.split("?")[-1].split(",")[0] if "?" in href else name[:50]

                    rows.append({
                        "ean": None, "chain": chain,
                        "product_id": prod_id, "sku_id": prod_id,
                        "name": name[:200], "brand": "",
                        "category": cat_name, "image_url": "",
                        "price": price, "list_price": price,
                        "available": True,
                        "scraped_at": datetime.now(timezone.utc).isoformat(),
                    })
                    found += 1

                log.info(f"[{chain}][{cat_name}] p{page_num}: {found} productos (total={len(rows)})")
                empty = 0 if found > 0 else empty + 1
                page_num += 1
                await asyncio.sleep(0.35)

    log.info(f"[{chain}] Total: {len(rows)}")
    return rows


# ── Supabase upsert ───────────────────────────────────────────────────────────

def upsert_prices(sb: Client, rows: list[dict], chain: str) -> dict:
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

    for i in range(0, len(rows), BATCH):
        batch = rows[i : i + BATCH]
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
                    "new_price": row["price"], "pct_change": round(pct, 2),
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
            log.info(f"[{chain}] {len(price_changes)} cambios de precio")
        except Exception as e:
            log.warning(f"Error price_changes: {e}")

    return stats


def log_run(sb: Client, chain: str, run_start: datetime, rows: list, stats: dict, elapsed: float):
    try:
        sb.table("scrape_logs").insert({
            "chain": chain, "run_at": run_start.isoformat(),
            "total_scraped": len(rows), "inserted": stats["inserted"],
            "updated": stats["updated"], "unchanged": stats["unchanged"],
            "errors": stats["errors"], "elapsed_s": elapsed,
            "status": "ok" if stats["errors"] == 0 else "partial",
        }).execute()
    except Exception as e:
        log.error(f"Error guardando log {chain}: {e}")


# ── Main ──────────────────────────────────────────────────────────────────────

async def main_async():
    log.info("=" * 60 + " precios-uy v3")
    sb        = create_client(SUPABASE_URL, SUPABASE_KEY)
    run_start = datetime.now(timezone.utc)

    # VTEX chains — secuencial para evitar rate-limit
    for chain, (base_url, categories) in VTEX_CHAINS.items():
        t0 = time.time()
        try:
            rows  = await scrape_vtex_chain(chain, base_url, categories)
            stats = upsert_prices(sb, rows, chain)
        except Exception as e:
            log.error(f"[{chain}] FALLO TOTAL: {e}")
            rows, stats = [], {"inserted":0,"updated":0,"unchanged":0,"errors":1}
        elapsed = round(time.time() - t0, 1)
        log_run(sb, chain, run_start, rows, stats, elapsed)
        log.info(f"[{chain}] scraped={len(rows)} new={stats['inserted']} updated={stats['updated']} ({elapsed}s)")

    # Tienda Inglesa
    t0 = time.time()
    try:
        rows  = await scrape_tienda_inglesa()
        stats = upsert_prices(sb, rows, "tienda_inglesa")
    except Exception as e:
        log.error(f"[tienda_inglesa] FALLO TOTAL: {e}")
        rows, stats = [], {"inserted":0,"updated":0,"unchanged":0,"errors":1}
    elapsed = round(time.time() - t0, 1)
    log_run(sb, "tienda_inglesa", run_start, rows, stats, elapsed)
    log.info(f"[tienda_inglesa] scraped={len(rows)} new={stats['inserted']} ({elapsed}s)")

    log.info("=" * 60 + " FIN")


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
