import os, json, time, logging, requests
from datetime import datetime, timezone
from supabase import create_client, Client
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

VTEX_CHAINS = {
    "disco":  "https://www.disco.com.uy",
    "devoto": "https://www.devoto.com.uy",
    "geant":  "https://www.geant.com.uy",
    "tata":   "https://www.tata.com.uy",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
}

PAGE_SIZE = 50
MAX_PAGES = 200
SLEEP_MS  = 400

def vtex_get_products_page(base_url, from_, to):
    url = f"{base_url}/api/catalog_system/pub/products/search/?O=OrderByTopSaleDESC&_from={from_}&_to={to}"
    r = requests.get(url, headers=HEADERS, timeout=20)
    if r.status_code == 404: return []
    r.raise_for_status()
    return r.json()

def parse_vtex_product(product, chain):
    rows = []
    for item in product.get("items", []):
        ean = item.get("ean") or (item.get("referenceId") or [{}])[0].get("Value")
        sellers = item.get("sellers", [])
        if not sellers: continue
        offer = sellers[0].get("commertialOffer", {})
        price = offer.get("Price")
        if price is None: continue
        rows.append({
            "ean": ean,
            "chain": chain,
            "product_id": str(product.get("productId", "")),
            "sku_id": str(item.get("itemId", "")),
            "name": product.get("productName", ""),
            "brand": product.get("brand", ""),
            "category": (product.get("categories") or [""])[0].strip("/").split("/")[-1],
            "image_url": (item.get("images") or [{}])[0].get("imageUrl", ""),
            "price": float(price),
            "list_price": float(offer.get("ListPrice") or price),
            "available": offer.get("AvailableQuantity", 0) > 0,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        })
    return rows

def scrape_vtex_chain(chain, base_url):
    rows = []
    log.info(f"[{chain}] Iniciando")
    for page in range(MAX_PAGES):
        from_ = page * PAGE_SIZE
        try:
            products = vtex_get_products_page(base_url, from_, from_ + PAGE_SIZE - 1)
        except Exception as e:
            log.warning(f"[{chain}] Error página {page}: {e}"); break
        if not products:
            log.info(f"[{chain}] Fin en página {page}. Total: {len(rows)}"); break
        for p in products:
            rows.extend(parse_vtex_product(p, chain))
        log.info(f"[{chain}] Página {page} — acumulados: {len(rows)}")
        time.sleep(SLEEP_MS / 1000)
    return rows

def scrape_tienda_inglesa():
    chain = "tienda_inglesa"
    rows = []
    categories = [("almacen",78),("bebidas",79),("lacteos",80),("limpieza",83),("perfumeria",84)]
    log.info(f"[{chain}] Iniciando")
    for cat_name, cat_id in categories:
        for page in range(100):
            url = f"https://www.tiendainglesa.com.uy/supermercado/categoria/{cat_name}/busqueda?0,0,*:*,{cat_id},0,0,,,false,,,{page}"
            try:
                r = requests.get(url, headers={**HEADERS,"Accept":"text/html"}, timeout=20)
                r.raise_for_status()
            except Exception as e:
                log.warning(f"[{chain}][{cat_name}] Error: {e}"); break
            soup = BeautifulSoup(r.text, "html.parser")
            cards = soup.select("a[href*='.producto']")
            if not cards: break
            for card in cards:
                name_el = card.select_one("span, div")
                price_el = card.find(string=lambda t: t and "$" in t)
                name = name_el.get_text(strip=True) if name_el else card.get_text(strip=True)
                if not price_el: continue
                try:
                    price = float(price_el.strip().replace("$","").replace(".","").replace(",","."))
                except: continue
                href = card.get("href","")
                prod_id = href.split("?")[-1].split(",")[0] if "?" in href else ""
                rows.append({"ean":None,"chain":chain,"product_id":prod_id,"sku_id":prod_id,
                    "name":name,"brand":"","category":cat_name,"image_url":"",
                    "price":price,"list_price":price,"available":True,
                    "scraped_at":datetime.now(timezone.utc).isoformat()})
            log.info(f"[{chain}][{cat_name}] Página {page}: {len(cards)} productos")
            time.sleep(SLEEP_MS / 1000)
    return rows

def upsert_prices(supabase, rows, chain):
    if not rows: return {"inserted":0,"updated":0,"unchanged":0,"errors":0}
    stats = {"inserted":0,"updated":0,"unchanged":0,"errors":0}
    existing = {}
    try:
        resp = supabase.table("prices_current").select("sku_id,price").eq("chain",chain).execute()
        for row in resp.data: existing[row["sku_id"]] = row["price"]
    except: pass
    price_changes = []
    BATCH = 500
    for i in range(0, len(rows), BATCH):
        batch = rows[i:i+BATCH]
        for row in batch:
            old = existing.get(row["sku_id"])
            if old is None: stats["inserted"] += 1
            elif abs(old - row["price"]) > 0.01:
                stats["updated"] += 1
                pct = ((row["price"] - old) / old) * 100
                price_changes.append({"chain":row["chain"],"sku_id":row["sku_id"],
                    "name":row["name"],"old_price":old,"new_price":row["price"],
                    "pct_change":round(pct,2),"detected_at":datetime.now(timezone.utc).isoformat()})
            else: stats["unchanged"] += 1
        try:
            supabase.table("prices_history").insert(batch).execute()
            supabase.table("prices_current").upsert(batch, on_conflict="chain,sku_id").execute()
        except Exception as e:
            log.error(f"Error upsert: {e}"); stats["errors"] += len(batch)
    if price_changes:
        try: supabase.table("price_changes").insert(price_changes).execute()
        except Exception as e: log.warning(f"Error price_changes: {e}")
    return stats

def main():
    log.info("="*50 + " Iniciando scraper precios-uy")
    sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    run_start = datetime.now(timezone.utc)

    all_scrapers = {**{k: (scrape_vtex_chain, k, v) for k,v in VTEX_CHAINS.items()},
                    "tienda_inglesa": (scrape_tienda_inglesa,)}

    for chain, scraper_info in all_scrapers.items():
        t0 = time.time()
        try:
            if chain == "tienda_inglesa":
                rows = scrape_tienda_inglesa()
            else:
                _, c, url = scraper_info
                rows = scrape_vtex_chain(c, url)
            stats = upsert_prices(sb, rows, chain)
        except Exception as e:
            log.error(f"[{chain}] FALLO: {e}")
            rows, stats = [], {"inserted":0,"updated":0,"unchanged":0,"errors":1}
        elapsed = round(time.time()-t0, 1)
        sb.table("scrape_logs").insert({"chain":chain,"run_at":run_start.isoformat(),
            "total_scraped":len(rows),"inserted":stats["inserted"],"updated":stats["updated"],
            "unchanged":stats["unchanged"],"errors":stats["errors"],"elapsed_s":elapsed,
            "status":"ok" if stats["errors"]==0 else "partial"}).execute()
        log.info(f"[{chain}] scraped={len(rows)} new={stats['inserted']} updated={stats['updated']} ({elapsed}s)")

if __name__ == "__main__":
    main()
