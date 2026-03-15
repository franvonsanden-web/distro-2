"""
scraper.py v2 — precios-uy
Fixes: selector VTEX corregido, URLs de categorías faltantes, TaTa habilitado
"""

import os, time, logging, requests
from datetime import datetime, timezone
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
from supabase import create_client, Client

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

# ── Categorías VTEX con slugs corregidos ─────────────────────────────────────
# congelados/limpieza/perfumeria/bebes usaban slugs incorrectos → corregidos
VTEX_CATEGORIES = [
    "almacen",
    "frescos",
    "bebidas",
    "congelados",
    "limpieza-y-cuidado",       # fix: era "limpieza"
    "perfumeria-y-cuidado",     # fix: era "perfumeria"
    "mascotas",
    "bebes-y-ninos",            # fix: era "bebes"
    "hogar",
]

# TaTa usa slugs distintos a Disco/Devoto/Géant
TATA_CATEGORIES = [
    "almacen",
    "frescos",
    "bebidas",
    "congelados",
    "limpieza",
    "perfumeria",
    "mascotas",
    "bebes",
    "hogar",
]

VTEX_CHAINS = {
    "disco":   ("https://www.disco.com.uy",  VTEX_CATEGORIES),
    "devoto":  ("https://www.devoto.com.uy", VTEX_CATEGORIES),
    "geant":   ("https://www.geant.com.uy",  VTEX_CATEGORIES),
    "tata":    ("https://www.tata.com.uy",   TATA_CATEGORIES),
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml",
}

SCROLL_ROUNDS = 6
SCROLL_WAIT   = 1800   # ms entre scrolls
PAGE_WAIT     = 12000  # ms para carga inicial
TIMEOUT       = 25000  # ms para wait_for_selector


# ── VTEX Playwright scraper ───────────────────────────────────────────────────

def scrape_vtex_page(page, url: str, chain: str, cat: str) -> list[dict]:
    """
    Abre una URL de categoría VTEX, hace scroll para cargar lazy elements,
    y extrae productos con los selectores reales de VTEX.
    
    Selectores corregidos (v2):
      - Card:   article[class*="vtex-product-summary"]   (era .product-item)
      - Nombre: .vtex-product-summary-2-x-productName
      - Precio: .vtex-product-price-1-x-sellingPriceValue
      - Imagen: img.vtex-product-summary-2-x-image
    """
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
        
        # Intentar el selector VTEX nuevo primero, luego fallback al viejo
        loaded = False
        for selector in [
            'article[class*="vtex-product-summary"]',
            'section[class*="vtex-product-summary"]',
            '.product-item',
            '[class*="productSummary"]',
        ]:
            try:
                page.wait_for_selector(selector, timeout=TIMEOUT)
                loaded = True
                break
            except:
                continue
        
        if not loaded:
            log.warning(f"[{chain}] Timeout en {cat} — sin productos visibles")
            return []

        # Scroll para cargar lazy-loaded products
        for _ in range(SCROLL_ROUNDS):
            page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
            page.wait_for_timeout(SCROLL_WAIT)

        # Extraer via evaluate (más robusto que query_selector_all para VTEX)
        products = page.evaluate("""() => {
            const results = [];
            
            // Selector principal VTEX moderno
            let cards = document.querySelectorAll('article[class*="vtex-product-summary"]');
            
            // Fallbacks
            if (!cards.length) cards = document.querySelectorAll('section[class*="vtex-product-summary"]');
            if (!cards.length) cards = document.querySelectorAll('.product-item');
            if (!cards.length) cards = document.querySelectorAll('[class*="productSummary"]');
            
            cards.forEach(card => {
                // Nombre — múltiples selectores posibles en distintas versiones VTEX
                const nameEl = 
                    card.querySelector('.vtex-product-summary-2-x-productName') ||
                    card.querySelector('[class*="productName"]') ||
                    card.querySelector('h3') ||
                    card.querySelector('h2');
                
                // Precio — precio de venta (con descuento si hay)
                const priceEl =
                    card.querySelector('.vtex-product-price-1-x-sellingPriceValue') ||
                    card.querySelector('[class*="sellingPriceValue"]') ||
                    card.querySelector('[class*="sellingPrice"]') ||
                    card.querySelector('[class*="price"]');
                
                // Imagen
                const imgEl = 
                    card.querySelector('img[class*="vtex-product-summary-2-x-image"]') ||
                    card.querySelector('img[class*="productImage"]') ||
                    card.querySelector('img');
                
                // Link (para usar como sku_id)
                const linkEl = card.querySelector('a[href]');
                
                const name  = nameEl  ? nameEl.innerText.trim()  : null;
                const price = priceEl ? priceEl.innerText.trim()  : null;
                const img   = imgEl   ? (imgEl.src || imgEl.dataset.src || '') : '';
                const href  = linkEl  ? linkEl.href : '';
                
                if (name && price) {
                    results.push({ name, price_raw: price, image_url: img, href });
                }
            });
            
            return results;
        }""")

        rows = []
        seen = set()
        for p in products:
            # Parsear precio — formato uruguayo: "$1.234,56" o "$ 1234"
            price_str = p["price_raw"]
            price_str = price_str.replace("$", "").replace("\u00a0", "").strip()
            price_str = price_str.replace(".", "").replace(",", ".")
            try:
                price = float(price_str)
            except ValueError:
                continue

            # sku_id desde el href
            href    = p.get("href", "")
            sku_id  = href.split("/")[-1].split("?")[0] or p["name"][:40]
            
            if sku_id in seen:
                continue
            seen.add(sku_id)

            rows.append({
                "ean":        None,
                "chain":      chain,
                "product_id": sku_id,
                "sku_id":     sku_id,
                "name":       p["name"],
                "brand":      "",
                "category":   cat,
                "image_url":  p.get("image_url", ""),
                "price":      price,
                "list_price": price,
                "available":  True,
                "scraped_at": datetime.now(timezone.utc).isoformat(),
            })

        return rows

    except Exception as e:
        log.warning(f"[{chain}] Error en {cat}: {e}")
        return []


def scrape_vtex_chain(chain: str, base_url: str, categories: list) -> list[dict]:
    all_rows = []
    log.info(f"[{chain}] Iniciando Playwright VTEX")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 900},
            locale="es-UY",
        )
        page = ctx.new_page()
        page.set_extra_http_headers({"Accept-Language": "es-UY,es;q=0.9"})

        for cat in categories:
            url  = f"{base_url}/{cat}"
            log.info(f"[{chain}] Cargando {url}")
            rows = scrape_vtex_page(page, url, chain, cat)
            all_rows.extend(rows)
            log.info(f"[{chain}][cat={cat}] products={len(rows)} total={len(all_rows)}")
            time.sleep(0.5)

        browser.close()

    # Deduplicar por sku_id
    seen = {}
    for r in all_rows:
        seen[r["sku_id"]] = r
    unique = list(seen.values())
    log.info(f"[{chain}] Total productos únicos: {len(unique)}")
    return unique


# ── Tienda Inglesa (requests + BS4) ──────────────────────────────────────────

def scrape_tienda_inglesa() -> list[dict]:
    chain = "tienda_inglesa"
    rows  = []

    # IDs verificados + URLs corregidas (v2)
    categories = [
        ("almacen",    78),
        ("bebidas",    79),
        ("lacteos",    80),
        ("carnes",     81),
        ("verduleria", 82),
        ("limpieza",   83),
        ("perfumeria", 84),
        ("congelados", 85),
    ]

    log.info(f"[{chain}] Iniciando")

    for cat_name, cat_id in categories:
        page_num = 0
        consecutive_empty = 0

        while consecutive_empty < 2:
            url = (
                f"https://www.tiendainglesa.com.uy/supermercado/categoria"
                f"/{cat_name}/busqueda?0,0,*:*,{cat_id},0,0,,,false,,,,{page_num}"
            )
            try:
                r = requests.get(url, headers=HEADERS, timeout=20)
                if r.status_code == 403:
                    log.warning(f"[{chain}][{cat_name}] 403 en p{page_num} — stop")
                    break
                r.raise_for_status()
            except Exception as e:
                log.warning(f"[{chain}][{cat_name}] Error p{page_num}: {e}")
                break

            soup  = BeautifulSoup(r.text, "lxml")
            cards = soup.select("a[href*='.producto']")
            # Solo cards del contenido principal (excluye nav/footer)
            main_cards = [c for c in cards if c.find_parent(class_=lambda x: x and "result" in x.lower())] or cards[:40]

            products_found = 0
            for card in main_cards:
                name_el = card.select_one("span[class*='name'], div[class*='name'], span, div")
                name    = name_el.get_text(strip=True) if name_el else card.get_text(" ", strip=True)
                if not name or len(name) < 3:
                    continue

                price_el = card.find(string=lambda t: t and "$" in t)
                if not price_el:
                    # buscar precio en elementos hermanos
                    for sib in card.next_siblings:
                        txt = getattr(sib, "get_text", lambda **k: "")()
                        if "$" in txt:
                            price_el = txt
                            break
                if not price_el:
                    continue

                price_str = str(price_el).replace("$","").replace(".","").replace(",",".").strip()
                try:
                    price = float(price_str)
                except ValueError:
                    continue

                href    = card.get("href", "")
                prod_id = href.split("?")[-1].split(",")[0] if "?" in href else name[:40]

                rows.append({
                    "ean":        None,
                    "chain":      chain,
                    "product_id": prod_id,
                    "sku_id":     prod_id,
                    "name":       name,
                    "brand":      "",
                    "category":   cat_name,
                    "image_url":  "",
                    "price":      price,
                    "list_price": price,
                    "available":  True,
                    "scraped_at": datetime.now(timezone.utc).isoformat(),
                })
                products_found += 1

            log.info(f"[{chain}][{cat_name}] p{page_num}: {products_found} productos (total={len(rows)})")

            if products_found == 0:
                consecutive_empty += 1
            else:
                consecutive_empty = 0

            page_num += 1
            time.sleep(0.4)

    log.info(f"[{chain}] Total: {len(rows)}")
    return rows


# ── Supabase upsert ───────────────────────────────────────────────────────────

def upsert_prices(sb: Client, rows: list[dict], chain: str) -> dict:
    if not rows:
        return {"inserted": 0, "updated": 0, "unchanged": 0, "errors": 0}

    stats = {"inserted": 0, "updated": 0, "unchanged": 0, "errors": 0}
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
                    "chain":       row["chain"],
                    "sku_id":      row["sku_id"],
                    "name":        row["name"],
                    "old_price":   old,
                    "new_price":   row["price"],
                    "pct_change":  round(pct, 2),
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
            log.info(f"[{chain}] {len(price_changes)} cambios de precio registrados")
        except Exception as e:
            log.warning(f"Error guardando price_changes: {e}")

    return stats


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60 + " precios-uy v2")
    sb        = create_client(SUPABASE_URL, SUPABASE_KEY)
    run_start = datetime.now(timezone.utc)

    # VTEX chains (incluye TaTa ahora)
    for chain, (base_url, categories) in VTEX_CHAINS.items():
        t0 = time.time()
        try:
            rows  = scrape_vtex_chain(chain, base_url, categories)
            stats = upsert_prices(sb, rows, chain)
        except Exception as e:
            log.error(f"[{chain}] FALLO TOTAL: {e}")
            rows, stats = [], {"inserted":0,"updated":0,"unchanged":0,"errors":1}

        elapsed = round(time.time() - t0, 1)
        try:
            sb.table("scrape_logs").insert({
                "chain": chain, "run_at": run_start.isoformat(),
                "total_scraped": len(rows), "inserted": stats["inserted"],
                "updated": stats["updated"], "unchanged": stats["unchanged"],
                "errors": stats["errors"], "elapsed_s": elapsed,
                "status": "ok" if stats["errors"] == 0 else "partial",
            }).execute()
        except Exception as e:
            log.error(f"Error guardando log para {chain}: {e}")

        log.info(f"[{chain}] scraped={len(rows)} new={stats['inserted']} "
                 f"updated={stats['updated']} errors={stats['errors']} ({elapsed}s)")

    # Tienda Inglesa
    t0 = time.time()
    try:
        rows  = scrape_tienda_inglesa()
        stats = upsert_prices(sb, rows, "tienda_inglesa")
    except Exception as e:
        log.error(f"[tienda_inglesa] FALLO TOTAL: {e}")
        rows, stats = [], {"inserted":0,"updated":0,"unchanged":0,"errors":1}

    elapsed = round(time.time() - t0, 1)
    try:
        sb.table("scrape_logs").insert({
            "chain": "tienda_inglesa", "run_at": run_start.isoformat(),
            "total_scraped": len(rows), "inserted": stats["inserted"],
            "updated": stats["updated"], "unchanged": stats["unchanged"],
            "errors": stats["errors"], "elapsed_s": elapsed,
            "status": "ok" if stats["errors"] == 0 else "partial",
        }).execute()
    except Exception as e:
        log.error(f"Error guardando log tienda_inglesa: {e}")

    log.info(f"[tienda_inglesa] scraped={len(rows)} new={stats['inserted']} ({elapsed}s)")
    log.info("=" * 60 + " FIN")


if __name__ == "__main__":
    main()
