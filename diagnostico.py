"""
Diagnóstico — correlo en Cursor para ver qué falla exactamente.
No escribe nada en Supabase.
"""
import requests, traceback, re

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
}

print("\n" + "="*60)
print("TEST 1 — VTEX: árbol de categorías de Disco")
print("="*60)
try:
    r = requests.get(
        "https://www.disco.com.uy/api/catalog_system/pub/category/tree/3",
        headers=HEADERS, timeout=15
    )
    print(f"Status: {r.status_code}")
    print(f"Primeros 500 chars: {r.text[:500]}")
except Exception:
    traceback.print_exc()

print("\n" + "="*60)
print("TEST 2 — VTEX: búsqueda clásica sin filtro (disco)")
print("="*60)
try:
    r = requests.get(
        "https://www.disco.com.uy/api/catalog_system/pub/products/search/?_from=0&_to=4",
        headers=HEADERS, timeout=15
    )
    print(f"Status: {r.status_code}")
    print(f"Primeros 500 chars: {r.text[:500]}")
except Exception:
    traceback.print_exc()

print("\n" + "="*60)
print("TEST 3 — VTEX: búsqueda con fq=C:/10/ (disco)")
print("="*60)
try:
    r = requests.get(
        "https://www.disco.com.uy/api/catalog_system/pub/products/search/?fq=C%3A%2F10%2F&_from=0&_to=4",
        headers=HEADERS, timeout=15
    )
    print(f"Status: {r.status_code}")
    print(f"Primeros 500 chars: {r.text[:500]}")
except Exception:
    traceback.print_exc()

print("\n" + "="*60)
print("TEST 4 — VTEX IO (disco)")
print("="*60)
try:
    r = requests.get(
        "https://www.disco.com.uy/api/io/_v/api/intelligent-search/product_search/?count=3&page=1",
        headers=HEADERS, timeout=15
    )
    print(f"Status: {r.status_code}")
    print(f"Primeros 500 chars: {r.text[:500]}")
except Exception:
    traceback.print_exc()

print("\n" + "="*60)
print("TEST 5 — Tienda Inglesa: URL categoría almacén página 0")
print("="*60)
try:
    r = requests.get(
        "https://www.tiendainglesa.com.uy/supermercado/categoria/almacen/busqueda?0,0,*:*,78,0,0,,,false,,,,0",
        headers={**HEADERS, "Accept": "text/html"},
        timeout=15
    )
    print(f"Status: {r.status_code}")
    # Buscar precio en la respuesta
    prices = re.findall(r'\$\s*[\d.,]+', r.text)
    print(f"Precios encontrados: {prices[:10]}")
    # Buscar links de productos
    import re as re2
    products = re2.findall(r'href="[^"]+\.producto[^"]*"', r.text)
    print(f"Links de productos: {len(products)} encontrados")
    print(f"Primeros 3: {products[:3]}")
except Exception:
    traceback.print_exc()

print("\n" + "="*60)
print("TEST 6 — Tienda Inglesa: URL con 3 comas (la vieja)")
print("="*60)
try:
    r = requests.get(
        "https://www.tiendainglesa.com.uy/supermercado/categoria/almacen/busqueda?0,0,*:*,78,0,0,,,false,,,0",
        headers={**HEADERS, "Accept": "text/html"},
        timeout=15
    )
    print(f"Status: {r.status_code}")
    prices = re.findall(r'\$\s*[\d.,]+', r.text)
    print(f"Precios encontrados: {prices[:10]}")
    products = re.findall(r'href="[^"]+\.producto[^"]*"', r.text)
    print(f"Links de productos: {len(products)}")
except Exception:
    traceback.print_exc()

print("\nDiagnóstico completo.")
