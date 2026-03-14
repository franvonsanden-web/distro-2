"""
Scraper de precios supermercados Uruguay — v2
Fixes: VTEX IO API + Tienda Inglesa pagination/price parsing
"""

import os, time, logging, requests
from datetime import datetime, timezone
from supabase import create_client, Client
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

VTEX_CHAINS = {
    "disco":   "https://www.disco.com.uy",
    "devoto":  "https://www.devoto.com.uy",
    "geant":   "https://www.geant.com.uy",
    "tata":    "https://www.tata.com.uy",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "es-UY,es;q=0.9",
}

PAGE_SIZE = 50
SLEEP_MS  = 400


# ── VTEX IO Intelligent Search API ───────────────────────────────────────────
# Estas cadenas usan VTEX IO (tienda moderna), que expone un endpoint distinto
# al VTEX clásico. El endpoint correcto es /api/io/_v/api/intelligent-search/

def vtex_io_get_products(base_url: str, page: int, count: int = PAGE_SIZE) -> dict:
    """
    Llama la VTEX IO Intelligent Search API.
    Devuelve el JSON completo (con 'products' y 'pagination').
    """
    url = (
        f"{base_url}/api/io/_v/api/intelligent-search/product_search/"
        f"?count={count}&page={page}&sort=orders%3Adesc"
    )
    r = requests.get(url, headers=HEADERS, timeout=20)
    if r.status_code in (404, 400):
        return {}
    r.raise_for_status()
    return r.json()


def parse_vtex_io_product(product: dict, chain: str) -> list[dict]:
    """Convierte un producto VTEX IO a filas de precios."""
    rows = []
    for item in product.get("items", []):
        ean     = item.get("ean")
        sellers = item.get("sellers", [])
        if not sellers:
            continue
        offer = sellers[0].get("commertialOffer", {})
        price = offer.get("Price")
        if price is None:
            continue
        rows.append({
            "ean":        ean,
            "chain":      chain,
            "product_id": str(product.get("productId", "")),
            "sku_id":     str(item.get("itemId", "")),
            "name":       product.get("productName", ""),
            "brand":      product.get("brand", ""),
            "category":   (product.get("categories") or [""])[0].strip("/").split("/")[-1],
            "image_url":  (item.get("images") or [{}])[0].get("imageUrl", ""),
            "price":      float(price),
            "list_price": float(offer.get("ListPrice") or price),
            "available":  offer.get("AvailableQuantity", 0) > 0,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        })
    return rows


def scrape_vtex_chain(chain: str, base_url: str) -> list[dict]:
    """Itera todas las páginas de una cadena VTEX IO."""
    all_rows = []
    log.info(f"[{chain}] Iniciando VTEX IO → {base_url}")

    for page in range(1, 500):  # VTEX IO es 1-indexed
        try:
            data = vtex_io_get_products(base_url, page)
        except Exception as e:
            log.warning(f"[{chain}] Error en página {page}: {e}")
            break

        products = data.get("products", [])
        if not products:
            log.info(f"[{chain}] Sin más productos en página {page}. Total: {len(all_rows)}")
            break

        for p in products:
            all_rows.extend(parse_vtex_io_product(p, chain))

        # Verificar si hay más páginas usando el objeto pagination
        pagination = data.get("pagination", {})
        total      = pagination.get("count", 0)
        per_page   = pagination.get("perPage", PAGE_SIZE)
        if total and page * per_page >= total:
            log.info(f"[{chain}] Última página ({page}). Total: {len(all_rows)}")
            break

        log.info(f"[{chain}] Página {page} → {len(products)} prods | acum={len(all_rows)}")
        time.sleep(SLEEP_MS / 1000)

    return all_rows


# ── Tienda Inglesa (HTML scraper) ─────────────────────────────────────────────
# Fix 1: el precio está en el elemento PADRE del <a>, no dentro del <a>
# Fix 2: detectar fin de paginación comparando el primer producto de cada página

def scrape_tienda_inglesa() -> list[dict]:
    chain = "tienda_inglesa"
    rows  = []

    categories = [
        ("almacen",    78),
        ("bebidas",    79),
        ("lacteos",    80),
        ("limpieza",   83),
        ("perfumeria", 84),
        ("congelados", 85),
    ]

    log.info(f"[{chain}] Iniciando HTML scraper")

    for cat_name, cat_id in categories:
        first_product_prev_page = None  # para detectar el fin de paginación

        for page in range(200):
            url = (
                f"https://www.tiendainglesa.com.uy/supermercado/categoria"
                f"/{cat_name}/busqueda?0,0,*:*,{cat_id},0,0,,,false,,,{page}"
            )
            try:
                r = requests.get(url, headers={**HEADERS, "Accept": "text/html"}, timeout=20)
                r.raise_for_status()
            except Exception as e:
                log.warning(f"[{chain}][{cat_name}] Error página {page}: {e}")
                break

            soup  = BeautifulSoup(r.text, "html.parser")
            cards = soup.select("a[href*='.producto']")

            if not cards:
                log.info(f"[{chain}][{cat_name}] Sin cards en página {page} → fin")
                break

            # Detectar loop: si el primer producto es igual al de la página anterior, paramos
            first_href = cards[0].get("href", "")
            if first_href and first_href == first_product_prev_page:
                log.info(f"[{chain}][{cat_name}] Página {page} repite contenido → fin real")
                break
            first_product_prev_page = first_href

            page_rows = 0
            for card in cards:
                href = card.get("href", "")
                prod_id = href.split("?")[-1].split(",")[0] if "?" in href else ""

                # Nombre: está dentro del <a>
                name_el = card.select_one("span, div")
                name    = name_el.get_text(strip=True) if name_el else card.get_text(strip=True)
                if not name:
                    continue

                # FIX: el precio está en el elemento PADRE del <a>, no dentro
                parent   = card.parent
                price_el = None
                if parent:
                    # Buscar texto con "$" en el padre (excluyendo el <a> en sí)
                    for sibling in parent.children:
                        text = getattr(sibling, 'string', None) or (
                            sibling.get_text(strip=True) if hasattr(sibling, 'get_text') else str(sibling)
                        )
                        if text and "$" in text and sibling != card:
                            price_el = text.strip()
                            break
                    # Fallback: cualquier texto con $ en el padre
                    if not price_el:
                        price_el = parent.find(string=lambda t: t and "$" in t and t.strip() != "$")

                if not price_el:
                    continue

                price_str = str(price_el).strip().replace("$", "").replace(".", "").replace(",", ".").strip()
                try:
                    price = float(price_str)
                    if price <= 0 or price > 999999:
                        continue
                except (ValueError, TypeError):
                    continue

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
                page_rows += 1

            log.info(f"[{chain}][{cat_name}] Página {page}: {len(cards)} cards, {page_rows} con precio")
            time.sleep(SLEEP_MS / 1000)

    log.info(f"[{chain}] Total: {len(rows)}")
    return rows


# ── Supabase upsert ───────────────────────────────────────────────────────────

def upsert_prices(supabase: Client, rows: list[dict], chain: str) -> dict:
    if not rows:
        return {"inserted": 0, "updated": 0, "unchanged": 0, "errors": 0}

    stats = {"inserted": 0, "updated": 0, "unchanged": 0, "errors": 0}

    # Traer precios actuales para detectar cambios
    existing = {}
    try:
        resp = supabase.table("prices_current").select("sku_id,price").eq("chain", chain).execute()
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
            supabase.table("prices_history").insert(batch).execute()
            supabase.table("prices_current").upsert(batch, on_conflict="chain,sku_id").execute()
        except Exception as e:
            log.error(f"Batch error: {e}")
            stats["errors"] += len(batch)

    if price_changes:
        try:
            supabase.table("price_changes").insert(price_changes).execute()
            log.info(f"[{chain}] {len(price_changes)} cambios de precio registrados")
        except Exception as e:
            log.warning(f"Error guardando price_changes: {e}")

    return stats


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("precios-uy scraper v2 — VTEX IO + TI fix")
    log.info("=" * 60)

    sb        = create_client(SUPABASE_URL, SUPABASE_KEY)
    run_start = datetime.now(timezone.utc)

    scrapers = {
        **{chain: ("vtex", url) for chain, url in VTEX_CHAINS.items()},
        "tienda_inglesa": ("ti", None),
    }

    for chain, (kind, url) in scrapers.items():
        t0 = time.time()
        try:
            rows  = scrape_vtex_chain(chain, url) if kind == "vtex" else scrape_tienda_inglesa()
            stats = upsert_prices(sb, rows, chain)
        except Exception as e:
            log.error(f"[{chain}] FALLO TOTAL: {e}")
            rows, stats = [], {"inserted": 0, "updated": 0, "unchanged": 0, "errors": 1}

        elapsed = round(time.time() - t0, 1)
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
            log.error(f"Error guardando log: {e}")

        log.info(
            f"[{chain}] scraped={len(rows)} new={stats['inserted']} "
            f"updated={stats['updated']} errors={stats['errors']} ({elapsed}s)"
        )

    log.info("=" * 60 + " FIN")


if __name__ == "__main__":
    main()
