"""
Scraper de precios supermercados Uruguay.

Objetivos de esta versión:
- No explotar si faltan credenciales de Supabase.
- Usar HTTP Session con retries y backoff.
- Descubrir categorías VTEX dinámicamente en vez de hardcodearlas.
- Hacer parsing más defensivo en Tienda Inglesa.
- Permitir exportar JSON local para diagnosticar sin escribir en DB.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from supabase import Client, create_client
from urllib3.util.retry import Retry

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/html;q=0.9, */*;q=0.8",
    "Accept-Language": "es-UY,es;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

SLEEP_MS = int(os.getenv("SCRAPER_SLEEP_MS", "300"))
PAGE_SIZE = int(os.getenv("SCRAPER_PAGE_SIZE", "50"))
REQUEST_TIMEOUT = int(os.getenv("SCRAPER_TIMEOUT_S", "25"))
OUTPUT_DIR = os.getenv("SCRAPER_OUTPUT_DIR", "")
TI_MAX_PAGES = int(os.getenv("TI_MAX_PAGES", "120"))

VTEX_CHAINS = {
    "disco": {"base": "https://www.disco.com.uy"},
    "devoto": {"base": "https://www.devoto.com.uy"},
    "geant": {"base": "https://www.geant.com.uy"},
    "tata": {"base": "https://www.tata.com.uy"},
}

TI_CAT_URL = (
    "https://www.tiendainglesa.com.uy/supermercado/categoria"
    "/{cat}/busqueda?0,0,*:*,{cat_id},0,0,,,false,,,,{page}"
)
TI_CATEGORIES = [
    ("almacen", 78),
    ("bebidas", 79),
    ("lacteos", 80),
    ("carnes", 81),
    ("verduleria", 82),
    ("limpieza", 83),
    ("perfumeria", 84),
    ("congelados", 85),
]

PRICE_RE = re.compile(r"\$\s*([\d.,]+)")
TI_PRODUCT_ID_RE = re.compile(r"\?(\d+)")
RANGE_RE = re.compile(r"\((\d+)\s*-\s*(\d+)\s+de\s+(\d+)\)")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_session() -> requests.Session:
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        status=4,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD", "OPTIONS"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    session = requests.Session()
    session.headers.update(HEADERS)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def maybe_sleep() -> None:
    if SLEEP_MS > 0:
        time.sleep(SLEEP_MS / 1000)


def get_env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def get_supabase_client() -> Client | None:
    url = get_env("SUPABASE_URL")
    key = get_env("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        log.warning("Supabase no configurado. Se correrá en modo export-only.")
        return None
    return create_client(url, key)


def export_rows(chain: str, rows: list[dict]) -> None:
    if not OUTPUT_DIR:
        return
    out_dir = Path(OUTPUT_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{chain}.json"
    path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info("[%s] Exportado JSON local: %s", chain, path)


# ───────────────────────── VTEX ─────────────────────────

def flatten_category_ids(nodes: Iterable[dict]) -> list[int]:
    ids: list[int] = []
    for node in nodes or []:
        try:
            ids.append(int(node["id"]))
        except Exception:
            continue
        ids.extend(flatten_category_ids(node.get("children") or []))
    return ids


def fetch_vtex_category_ids(session: requests.Session, base: str) -> list[int]:
    url = f"{base}/api/catalog_system/pub/category/tree/10"
    r = session.get(url, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    ids = sorted(set(flatten_category_ids(data if isinstance(data, list) else [])))
    if not ids:
        raise RuntimeError(f"No se pudieron resolver categorías VTEX para {base}")
    return ids


def vtex_search_page(session: requests.Session, base: str, cat_id: int, from_: int) -> list[dict]:
    url = (
        f"{base}/api/catalog_system/pub/products/search/"
        f"?fq=C%3A%2F{cat_id}%2F"
        f"&_from={from_}&_to={from_ + PAGE_SIZE - 1}"
        f"&O=OrderByTopSaleDESC"
    )
    r = session.get(url, timeout=REQUEST_TIMEOUT)
    if r.status_code in (400, 404):
        return []
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, list) else []


def parse_vtex_product(product: dict, chain: str) -> list[dict]:
    rows: list[dict] = []
    categories = product.get("categories") or [""]
    category = categories[0].strip("/").split("/")[-1] if categories else ""

    for item in product.get("items") or []:
        sellers = item.get("sellers") or []
        if not sellers:
            continue

        seller0 = sellers[0] or {}
        offer = seller0.get("commertialOffer") or seller0.get("commercialOffer") or {}
        price = offer.get("Price")
        sku_id = str(item.get("itemId") or "").strip()
        if price is None or not sku_id:
            continue

        reference_ids = item.get("referenceId") or []
        ean = item.get("ean") or next(
            (ref.get("Value") for ref in reference_ids if ref.get("Key") in {"RefId", "EAN"}),
            None,
        )

        rows.append(
            {
                "ean": ean,
                "chain": chain,
                "product_id": str(product.get("productId") or ""),
                "sku_id": sku_id,
                "name": product.get("productName") or "",
                "brand": product.get("brand") or "",
                "category": category,
                "image_url": ((item.get("images") or [{}])[0] or {}).get("imageUrl", ""),
                "price": float(price),
                "list_price": float(offer.get("ListPrice") or price),
                "available": bool((offer.get("AvailableQuantity") or 0) > 0),
                "scraped_at": utc_now_iso(),
            }
        )
    return rows


def scrape_vtex_chain(chain: str, cfg: dict, session: requests.Session) -> list[dict]:
    base = cfg["base"]
    all_rows: dict[str, dict] = {}
    category_ids = fetch_vtex_category_ids(session, base)

    log.info("[%s] Iniciando VTEX con %s categorías detectadas", chain, len(category_ids))

    for cat_id in category_ids:
        page = 0
        while True:
            try:
                products = vtex_search_page(session, base, cat_id, page * PAGE_SIZE)
            except Exception as exc:
                log.warning("[%s][cat=%s] Error página %s: %s", chain, cat_id, page, exc)
                break

            if not products:
                break

            before = len(all_rows)
            for product in products:
                for row in parse_vtex_product(product, chain):
                    all_rows[row["sku_id"]] = row

            log.info(
                "[%s][cat=%s] p%s: products=%s unique_delta=%s total=%s",
                chain,
                cat_id,
                page,
                len(products),
                len(all_rows) - before,
                len(all_rows),
            )

            page += 1
            if len(products) < PAGE_SIZE:
                break
            maybe_sleep()

    result = list(all_rows.values())
    log.info("[%s] Total productos únicos: %s", chain, len(result))
    export_rows(chain, result)
    return result


# ─────────────────────── Tienda Inglesa ───────────────────────

def parse_price(text: str) -> float | None:
    if not text:
        return None
    match = PRICE_RE.search(text)
    if not match:
        return None
    raw = match.group(1).replace(".", "").replace(",", ".")
    try:
        value = float(raw)
        return value if 0 < value < 999999 else None
    except ValueError:
        return None


def extract_ti_product_id(href: str) -> str:
    match = TI_PRODUCT_ID_RE.search(href or "")
    return match.group(1) if match else ""


def iter_name_links(soup: BeautifulSoup) -> list:
    links = []
    seen = set()
    for a in soup.select("a[href*='.producto']"):
        name = a.get_text(" ", strip=True)
        href = a.get("href", "")
        if not name or not href:
            continue
        key = (href, name)
        if key in seen:
            continue
        seen.add(key)
        links.append(a)
    return links


def extract_price_near_anchor(anchor) -> float | None:
    steps = 0
    for el in anchor.next_elements:
        if el is anchor:
            continue
        if getattr(el, "name", None) == "a" and el is not anchor:
            href = el.get("href", "") if hasattr(el, "get") else ""
            txt = el.get_text(" ", strip=True) if hasattr(el, "get_text") else ""
            if ".producto" in href and txt:
                break

        text = None
        if isinstance(el, str):
            text = el.strip()
        elif hasattr(el, "get_text"):
            text = el.get_text(" ", strip=True)

        if text:
            price = parse_price(text)
            if price is not None:
                return price

        steps += 1
        if steps > 120:
            break
    return None


def page_range_info(soup: BeautifulSoup) -> tuple[int, int, int] | None:
    text = soup.get_text(" ", strip=True)
    match = RANGE_RE.search(text)
    if not match:
        return None
    return tuple(int(x) for x in match.groups())


def scrape_tienda_inglesa(session: requests.Session) -> list[dict]:
    chain = "tienda_inglesa"
    seen: dict[str, dict] = {}

    log.info("[%s] Iniciando", chain)

    for cat_name, cat_id in TI_CATEGORIES:
        prev_first_href = None

        for page in range(TI_MAX_PAGES):
            url = TI_CAT_URL.format(cat=cat_name, cat_id=cat_id, page=page)
            try:
                r = session.get(url, headers={**HEADERS, "Accept": "text/html"}, timeout=REQUEST_TIMEOUT)
                r.raise_for_status()
            except Exception as exc:
                log.warning("[%s][%s] Error p%s: %s", chain, cat_name, page, exc)
                break

            soup = BeautifulSoup(r.text, "lxml")
            links = iter_name_links(soup)
            if not links:
                log.info("[%s][%s] Sin links de productos en p%s → fin", chain, cat_name, page)
                break

            # La página incluye una sección de recomendaciones después del listado principal.
            # Nos quedamos con el primer bloque de resultados de catálogo.
            page_info = page_range_info(soup)
            expected = 40
            if page_info:
                start_n, end_n, total_n = page_info
                expected = max(1, min(40, end_n - start_n + 1))
                if start_n > total_n:
                    break
            main_links = links[:expected]

            first_href = main_links[0].get("href", "") if main_links else ""
            if first_href and first_href == prev_first_href:
                log.info("[%s][%s] Página %s repite contenido → fin", chain, cat_name, page)
                break
            prev_first_href = first_href

            found = 0
            with_price = 0
            for anchor in main_links:
                href = anchor.get("href", "")
                name = anchor.get_text(" ", strip=True)
                product_id = extract_ti_product_id(href)
                if not name or not product_id:
                    continue

                price = extract_price_near_anchor(anchor)
                if price is None:
                    continue

                seen[product_id] = {
                    "ean": None,
                    "chain": chain,
                    "product_id": product_id,
                    "sku_id": product_id,
                    "name": name,
                    "brand": "",
                    "category": cat_name,
                    "image_url": "",
                    "price": price,
                    "list_price": price,
                    "available": True,
                    "scraped_at": utc_now_iso(),
                }
                found += 1
                with_price += 1

            log.info(
                "[%s][%s] p%s: links_total=%s links_main=%s con_precio=%s total=%s",
                chain,
                cat_name,
                page,
                len(links),
                len(main_links),
                with_price,
                len(seen),
            )

            if not main_links or found == 0:
                log.info("[%s][%s] Sin items útiles en p%s → fin", chain, cat_name, page)
                break

            if page_info and page_info[1] >= page_info[2]:
                break

            maybe_sleep()

    result = list(seen.values())
    log.info("[%s] Total: %s", chain, len(result))
    export_rows(chain, result)
    return result


# ─────────────────────── Persistencia ───────────────────────

def upsert_prices(supabase: Client | None, rows: list[dict], chain: str) -> dict:
    if not rows:
        return {"inserted": 0, "updated": 0, "unchanged": 0, "errors": 0}

    if supabase is None:
        return {"inserted": len(rows), "updated": 0, "unchanged": 0, "errors": 0}

    stats = {"inserted": 0, "updated": 0, "unchanged": 0, "errors": 0}
    existing: dict[str, float] = {}

    try:
        resp = supabase.table("prices_current").select("sku_id,price").eq("chain", chain).execute()
        for row in resp.data or []:
            if row.get("sku_id") is None or row.get("price") is None:
                continue
            existing[str(row["sku_id"])] = float(row["price"])
    except Exception as exc:
        log.warning("[%s] No se pudo cargar prices_current: %s", chain, exc)

    price_changes = []
    batch_size = 500

    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        deduped = {row["sku_id"]: row for row in batch if row.get("sku_id")}
        batch = list(deduped.values())

        for row in batch:
            old = existing.get(row["sku_id"])
            if old is None:
                stats["inserted"] += 1
            elif abs(old - row["price"]) > 0.01:
                stats["updated"] += 1
                pct = ((row["price"] - old) / old) * 100 if old else 0
                price_changes.append(
                    {
                        "chain": chain,
                        "sku_id": row["sku_id"],
                        "name": row["name"],
                        "old_price": old,
                        "new_price": row["price"],
                        "pct_change": round(pct, 2),
                        "detected_at": utc_now_iso(),
                    }
                )
            else:
                stats["unchanged"] += 1

        try:
            supabase.table("prices_history").insert(batch).execute()
            supabase.table("prices_current").upsert(batch, on_conflict="chain,sku_id").execute()
        except Exception as exc:
            log.error("[%s] Batch %s error: %s", chain, i // batch_size, exc)
            stats["errors"] += len(batch)

    if price_changes:
        try:
            supabase.table("price_changes").insert(price_changes).execute()
            log.info("[%s] %s cambios de precio", chain, len(price_changes))
        except Exception as exc:
            log.warning("[%s] price_changes error: %s", chain, exc)

    return stats


def write_scrape_log(supabase: Client | None, payload: dict) -> None:
    if supabase is None:
        return
    try:
        supabase.table("scrape_logs").insert(payload).execute()
    except Exception as exc:
        log.error("Log error: %s", exc)


# ───────────────────────── Main ─────────────────────────

def main() -> None:
    log.info("=" * 60)
    log.info("precios-uy")
    log.info("=" * 60)

    session = get_session()
    supabase = get_supabase_client()
    run_start = utc_now_iso()

    jobs = {
        **{chain: ("vtex", cfg) for chain, cfg in VTEX_CHAINS.items()},
        "tienda_inglesa": ("ti", None),
    }

    for chain, (kind, cfg) in jobs.items():
        t0 = time.time()
        try:
            rows = scrape_vtex_chain(chain, cfg, session) if kind == "vtex" else scrape_tienda_inglesa(session)
            stats = upsert_prices(supabase, rows, chain)
            status = "ok" if stats["errors"] == 0 else "partial"
        except Exception as exc:
            log.exception("[%s] FALLO", chain)
            rows = []
            stats = {"inserted": 0, "updated": 0, "unchanged": 0, "errors": 1}
            status = f"error: {exc}"

        elapsed = round(time.time() - t0, 1)
        write_scrape_log(
            supabase,
            {
                "chain": chain,
                "run_at": run_start,
                "total_scraped": len(rows),
                "inserted": stats["inserted"],
                "updated": stats["updated"],
                "unchanged": stats["unchanged"],
                "errors": stats["errors"],
                "elapsed_s": elapsed,
                "status": status,
            },
        )
        log.info(
            "[%s] scraped=%s new=%s updated=%s unchanged=%s errors=%s (%ss)",
            chain,
            len(rows),
            stats["inserted"],
            stats["updated"],
            stats["unchanged"],
            stats["errors"],
            elapsed,
        )

    log.info("=" * 60 + " FIN")


if __name__ == "__main__":
    main()
