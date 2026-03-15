"""
Diagnóstico — correlo en Cursor para ver qué falla exactamente.
No escribe nada en Supabase.
"""
import requests, traceback, re

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
}
import requests, traceback, re, time
from playwright.sync_api import sync_playwright

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
}

print("\n" + "="*60)
print("TEST 1 — VTEX Playwright (disco)")
print("="*60)
try:
    with sync_playwright() as p:
        b = p.chromium.launch(headless=True)
        page = b.new_page(user_agent=HEADERS["User-Agent"])
        print("Cargando https://www.disco.com.uy/almacen ...")
        page.goto("https://www.disco.com.uy/almacen", timeout=30000)
        page.wait_for_selector(".product-item", timeout=15000)
        
        products = page.evaluate("""() => {
            const els = document.querySelectorAll('.product-item');
            return Array.from(els).slice(0, 3).map(el => {
                const nameNode = el.querySelector('.prod-desc');
                const priceNode = el.querySelector('.price .val');
                let name = '';
                if (nameNode) {
                    name = nameNode.innerText.split('Agregar')[0].split('\\n').join(' ').trim();
                }
                return name + " - " + (priceNode ? priceNode.innerText.trim() : '');
            });
        }""")
        print(f"Productos encontrados: {len(products)}")
        for i, prod in enumerate(products):
            print(f"  {i+1}: {prod}")
        b.close()
except Exception:
    traceback.print_exc()

print("\n" + "="*60)
print("TEST 2 — Tienda Inglesa: URL categoria almacen pagina 0")
print("="*60)
try:
    r = requests.get(
        "https://www.tiendainglesa.com.uy/supermercado/categoria/almacen/busqueda?0,0,*:*,78,0,0,,,false,,,,0",
        headers={**HEADERS, "Accept": "text/html"},
        timeout=15
    )
    print(f"Status: {r.status_code}")
    prices = re.findall(r'\$\s*[\d.,]+', r.text)
    print(f"Precios encontrados: {prices[:5]}")
    products = re.findall(r'href="[^"]+\.producto[^"]*"', r.text)
    print(f"Links de productos: {len(products)} encontrados")
except Exception:
    traceback.print_exc()

print("\nDiagnóstico completo.")
