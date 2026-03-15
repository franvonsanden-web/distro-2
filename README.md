# distro-2

Scraper de uso interno de productos en Uruguay.

La startup Cabacua construyó una plataforma que compara más de 100.000 precios de supermercados en Montevideo a partir de datos recolectados de los catálogos de las cadenas

Esto implica hacer scraping de sitios como Tienda Inglesa, Disco, Devoto, Géant, etc. — todos tienen catálogos online.

VTEX es una plataforma de ecommerce con una API pública de catálogo. Esto significa que no necesitás scraping real para 4 de las 5 cadenas principales — podés consultarlas como si fueran una API REST.

VTEX te da el EAN (código de barras) de cada producto. Ese número es el mismo en todas las cadenas para el mismo producto físico. La normalización deja de ser un problema de NLP y se convierte en un JOIN por EAN.

Disco: "Arroz Gallo 1kg" → EAN 7891234567890
TaTa: "Arroz Largo Fino 1000g" → EAN 7891234567890


El resumen ejecutivo
Esto es mucho más fácil de lo que esperabas. El hallazgo clave es que tanto TaTa como Disco (y por extensión Devoto y Géant que son del mismo grupo) corren sobre VTEX Commerce Platform Tienda Inglesa — una plataforma de ecommerce latinoamericana con una API de catálogo que es pública y no requiere autenticación para lectura.
Esto significa que podés hacer:
GET https://disco.com.uy/api/catalog_system/pub/products/search/?fq=C:/10/&_from=0&_to=49
...y recibís un JSON con nombre, EAN, precio, imagen y marca. Sin Playwright, sin parsing de HTML, sin riesgo de que cambien el CSS.
El único scraping real necesario es Tienda Inglesa, que tiene stack propio pero con una estructura HTML bastante limpia y paginación predecible via query params.
El problema de normalización desaparece casi solo: VTEX expone el código EAN (código de barras) de cada producto. Ese número es universal — si Disco y TaTa venden el mismo arroz, el EAN es idéntico. Hacés un JOIN por EAN en lugar de matching semántico con embeddings.


GitHub Actions + Supabase + Retool (o Notion)

GitHub Actions corre tu scraper en Python todos los días de forma gratuita y confiable. Supabase es tu base de datos Postgres gratuita con interfaz visual. Retool (o Notion) es tu dashboard interno sin escribir código.



## Ejecutar local

```bash
python -m venv .venv
source .venv/bin/activate  # en Windows: .venv\Scripts\activate
pip install -r scraper/requirements.txt
cp .env.example .env
python scraper/scraper.py
```

## Modos de ejecución

- Con `SUPABASE_URL` y `SUPABASE_SERVICE_ROLE_KEY`: scrapea y persiste.
- Sin credenciales: corre en modo `export-only` y escribe JSON si `SCRAPER_OUTPUT_DIR` está definido.

## Notas

- No commitear `.env`.
- No commitear virtualenvs.
- Revisar límites legales y términos de uso de cada sitio antes de usar scraping en producción.
