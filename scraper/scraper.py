"""
Scraper de precios supermercados Uruguay — v3
Fixes:
  - VTEX: usa API clásica con filtro por categoría fq=C:/id/
  - TI: URL paginación correcta (4 comas), dedup por sku_id, regex precio
"""

import os, re, time, logging, requests
from datetime import datetime, timezone
from supabase import create_client, Client
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "es-UY,es;q=0.9",
}

SLEEP_MS  = 400
PAGE_SIZE = 50

# Categorías VTEX por cadena: (nombre_display, category_id_en_vtex)
VTEX_CHAINS = {
    "disco": {
        "base": "https://www.disco.com.uy",
        "cats": [10, 11, 12, 14, 15, 20, 21, 22, 30, 31, 40, 41, 42, 43, 50],
    },
    "devoto": {
        "base": "https://www.devoto.com.uy",
        "cats": [10, 11, 12, 14, 15, 20, 21, 22, 30, 31, 40, 41, 42, 43, 50],
    },
    "geant": {
        "base": "https://www.geant.com.uy",
        "cats": [10, 11, 12, 14, 15, 20, 21, 22, 30, 31, 40, 41, 42, 43, 50],
    },
    "tata": {
        "base": "https://www.tata.com.uy",
        "cats": [10, 11, 12, 14, 15, 20, 21, 22, 30, 31, 40, 41],
    },
}

# ── VTEX clásico con filtro por categoría ─────────────────────────────────────

def vtex_search_page(base: str, cat_id: int, from_: int) -> list[dict]:
    """Llama la VTEX Search API filtrando por categoría."""
    url = (
        f"{base}/api/catalog_system/pub/products/search/"
        f"?fq=C%3A%2F{cat_id}%2F"
        f"&_from={from_}&_to={from_ + PAGE_SIZE - 1}"
        f"&O=OrderByTopSaleDESC"
    )
    r = requests.get(url, headers=HEADERS, timeout=20)
    if r.status_code in (404, 400):
        return []
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, list) else []


def parse_vtex_product(product: dict, chain: str) -> list[dict]:
    rows = []
    cats = product.get("categories") or [""]
    cat  = cats[0].strip("/").split("/")[-1] if cats else ""
    for item in product.get("items", []):
        ean     = item.get("ean") or (item.get("referenceId") or [{}])[0].get("Value")
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
            "category":   cat,
            "image_url":  (item.get("images") or [{}])[0].get("imageUrl", ""),
            "price":      float(price),
            "list_price": float(offer.get("ListPrice") or price),
            "available":  offer.get("AvailableQuantity", 0) > 0,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        })
    return rows


def scrape_vtex_chain(chain: str, cfg: dict) -> list[dict]:
    base     = cfg["base"]
    cat_ids  = cfg["cats"]
    all_rows = {}  # dedup por sku_id

    log.info(f"[{chain}] Iniciando — {len(cat_ids)} categorías")

    for cat_id in cat_ids:
        page = 0
        while True:
            try:
                products = vtex_search_page(base, cat_id, page * PAGE_SIZE)
            except Exception as e:
                log.warning(f"[{chain}][cat={cat_id}] Error página {page}: {e}")
                break

            if not products:
                break

            for p in products:
                for row in parse_vtex_product(p, chain):
                    all_rows[row["sku_id"]] = row  # dedup

            log.info(f"[{chain}][cat={cat_id}] p{page} +{len(products)} | total={len(all_rows)}")
            page += 1
            time.sleep(SLEEP_MS / 1000)

    result = list(all_rows.values())
    log.info(f"[{chain}] Total productos únicos: {len(result)}")
    return result


# ── Tienda Inglesa ─────────────────────────────────────────────────────────────

# URL correcta de paginación — 4 comas después de 'false': ,,,false,,,,{page}
TI_CAT_URL = (
    "https://www.tiendainglesa.com.uy/supermercado/categoria"
    "/{cat}/busqueda?0,0,*:*,{cat_id},0,0,,,false,,,,{page}"
)

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

PRICE_RE = re.compile(r'\$\s*([\d.,]+)')


def parse_price(text: str) -> float | None:
    """Extrae float de texto tipo '$ 1.234,56' o '$ 236'."""
    if not text:
        return None
    m = PRICE_RE.search(text)
    if not m:
        return None
    raw = m.group(1).replace(".", "").replace(",", ".")
    try:
        val = float(raw)
        return val if 1 < val < 999999 else None
    except ValueError:
        return None


def scrape_tienda_inglesa() -> list[dict]:
    chain     = "tienda_inglesa"
    seen      = {}   # sku_id → row (dedup dentro de la misma cadena)

    log.info(f"[{chain}] Iniciando")

    for cat_name, cat_id in TI_CATEGORIES:
        prev_first_href = None

        for page in range(300):
            url = TI_CAT_URL.format(cat=cat_name, cat_id=cat_id, page=page)
            try:
                r = requests.get(
                    url,
                    headers={**HEADERS, "Accept": "text/html"},
                    timeout=20
                )
                r.raise_for_status()
            except Exception as e:
                log.warning(f"[{chain}][{cat_name}] Error p{page}: {e}")
                break

            soup = BeautifulSoup(r.text, "html.parser")

            # Detectar fin real: el paginador incluye el total "(1 - 40 de 2815)"
            # Si no hay ese breadcrumb, la página no existe
            breadcrumb = soup.find(string=re.compile(r'\d+\s*-\s*\d+\s+de\s+\d+'))
            if not breadcrumb and page > 0:
                log.info(f"[{chain}][{cat_name}] Sin breadcrumb en p{page} → fin")
                break

            # Solo los <a> que tienen texto (no los de imágenes)
            name_links = [
                a for a in soup.select("a[href*='.producto']")
                if a.get_text(strip=True)
            ]

            if not name_links:
                log.info(f"[{chain}][{cat_name}] Sin productos en p{page} → fin")
                break

            # Anti-loop: detectar si la página repite el primer producto
            first_href = name_links[0].get("href", "")
            if first_href == prev_first_href:
                log.info(f"[{chain}][{cat_name}] Página {page} repite contenido → fin")
                break
            prev_first_href = first_href

            found = 0
            for a_name in name_links:
                href    = a_name.get("href", "")
                prod_id = href.split("?")[-1].split(",")[0] if "?" in href else ""
                name    = a_name.get_text(strip=True)

                if not name or not prod_id:
                    continue

                # El precio es un text node suelto en el mismo contenedor padre
                # Buscamos hacia arriba hasta encontrar un bloque que contenga "$"
                price = None
                container = a_name.parent
                for _ in range(4):  # máximo 4 niveles arriba
                    if container is None:
                        break
                    full_text = container.get_text(" ", strip=True)
                    price = parse_price(full_text)
                    if price:
                        break
                    container = container.parent

                if price is None:
                    continue

                seen[prod_id] = {
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
                }
                found += 1

            log.info(f"[{chain}][{cat_name}] p{page}: {len(name_links)} links, {found} con precio")
            time.sleep(SLEEP_MS / 1000)

    result = list(seen.values())
    log.info(f"[{chain}] Total: {len(result)}")
    return result


# ── Supabase upsert ───────────────────────────────────────────────────────────

def upsert_prices(supabase: Client, rows: list[dict], chain: str) -> dict:
    if not rows:
        return {"inserted": 0, "updated": 0, "unchanged": 0, "errors": 0}

    stats    = {"inserted": 0, "updated": 0, "unchanged": 0, "errors": 0}
    existing = {}
    try:
        resp = supabase.table("prices_current").select("sku_id,price").eq("chain", chain).execute()
        for r in resp.data:
            existing[r["sku_id"]] = float(r["price"])
    except Exception as e:
        log.warning(f"No se pudo cargar precios actuales: {e}")

    price_changes = []
    BATCH = 500

    for i in range(0, len(rows), BATCH):
        batch = rows[i : i + BATCH]

        # Dedup dentro del batch (por si acaso)
        seen_in_batch = {}
        for row in batch:
            seen_in_batch[row["sku_id"]] = row
        batch = list(seen_in_batch.values())

        for row in batch:
            old = existing.get(row["sku_id"])
            if old is None:
                stats["inserted"] += 1
            elif abs(old - row["price"]) > 0.01:
                stats["updated"] += 1
                pct = ((row["price"] - old) / old) * 100
                price_changes.append({
                    "chain":       chain,
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
            supabase.table("prices_current").upsert(
                batch, on_conflict="chain,sku_id"
            ).execute()
        except Exception as e:
            log.error(f"Batch {i//BATCH} error: {e}")
            stats["errors"] += len(batch)

    if price_changes:
        try:
            supabase.table("price_changes").insert(price_changes).execute()
            log.info(f"[{chain}] {len(price_changes)} cambios de precio")
        except Exception as e:
            log.warning(f"price_changes error: {e}")

    return stats


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("precios-uy v3")
    log.info("=" * 60)

    sb        = create_client(SUPABASE_URL, SUPABASE_KEY)
    run_start = datetime.now(timezone.utc)

    jobs = {
        **{chain: ("vtex", cfg) for chain, cfg in VTEX_CHAINS.items()},
        "tienda_inglesa": ("ti", None),
    }

    for chain, (kind, cfg) in jobs.items():
        t0 = time.time()
        try:
            rows  = scrape_vtex_chain(chain, cfg) if kind == "vtex" else scrape_tienda_inglesa()
            stats = upsert_prices(sb, rows, chain)
        except Exception as e:
            log.error(f"[{chain}] FALLO: {e}")
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
            log.error(f"Log error: {e}")

        log.info(
            f"[{chain}] scraped={len(rows)} "
            f"new={stats['inserted']} updated={stats['updated']} "
            f"errors={stats['errors']} ({elapsed}s)"
        )

    log.info("=" * 60 + " FIN")


if __name__ == "__main__":
    main()
