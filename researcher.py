"""
researcher.py — Competition research module using Anthropic Claude API
with web_search_20250305 tool for real web searches.
Includes retry logic and rate-limit handling.
"""

import os
import json
import asyncio
import logging
from datetime import datetime
from typing import Optional, Callable
import anthropic

logger = logging.getLogger(__name__)

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
MODEL = os.getenv("CLAUDE_MODEL", "claude-sonnet-4-20250514")
RESEARCH_MODEL = os.getenv("RESEARCH_MODEL", "claude-haiku-4-5-20251001")
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT_SEARCHES", "5"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
RETRY_BASE_DELAY = 10  # seconds

# Brands that belong to EDUCA EDTECH Group — never treat as competition
EDUCA_BRANDS = [
    "Euroinnova", "INESEM", "INEAF", "RedEduca.net", "RedEduca",
    "Inesalud", "Edusport", "Capman", "Educa Business School", "educa.net",
    "CEUPE", "Structuralia", "Educa.pro", "UDAVINCI",
    "UNIMIAMI", "International University of Miami",
    "ESIBE", "Escuela Iberoamericana de Postgrado",
]

COMPETITOR_CONTEXT = """
Competidores habituales en formación online en España:
- Universidades Online: UNIR, VIU, UDIMA, UOC, Universidad Europea, UEMC Online, Nebrija Online, Universidad Isabel I, UCJC, UAH Online, UAX
- Centros de Formación: IMF Smart Education, CEREM, ISEP, OBS Business School, IEBS Digital School, Campus Training, MasterD, ILERNA Online, MEDAC, Carpe Diem
- Plataformas Internacionales: Coursera, edX, Domestika, Platzi, Crehana
"""

EDUCA_BRANDS_NOTE = f"""
IMPORTANTE: Las siguientes marcas pertenecen a EDUCA EDTECH Group y NO son competencia.
Si las encuentras, etiquétalas como "Grupo EDUCA":
{', '.join(EDUCA_BRANDS)}
"""


def _get_client():
    if not ANTHROPIC_API_KEY:
        raise ValueError(
            "ANTHROPIC_API_KEY no está configurada. "
            "Establécela como variable de entorno."
        )
    return anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)


def _current_year() -> str:
    """Return current year as string for search queries."""
    return str(datetime.now().year)


async def _call_claude_with_retry(
    client,
    messages: list,
    max_tokens: int = 4096,
    tools: list = None,
    model: str = None,
) -> object:
    """
    Call Claude API with exponential backoff retry on rate limits and transient errors.
    """
    for attempt in range(MAX_RETRIES):
        try:
            kwargs = {
                "model": model or MODEL,
                "max_tokens": max_tokens,
                "messages": messages,
            }
            if tools:
                kwargs["tools"] = tools

            response = await asyncio.to_thread(
                client.messages.create, **kwargs
            )
            return response

        except anthropic.RateLimitError as e:
            delay = RETRY_BASE_DELAY * (2 ** attempt)
            logger.warning(
                f"Rate limit hit (attempt {attempt + 1}/{MAX_RETRIES}). "
                f"Waiting {delay}s before retry..."
            )
            await asyncio.sleep(delay)
            if attempt == MAX_RETRIES - 1:
                raise

        except anthropic.APIStatusError as e:
            if e.status_code >= 500:
                delay = RETRY_BASE_DELAY * (2 ** attempt)
                logger.warning(
                    f"API error {e.status_code} (attempt {attempt + 1}/{MAX_RETRIES}). "
                    f"Waiting {delay}s..."
                )
                await asyncio.sleep(delay)
                if attempt == MAX_RETRIES - 1:
                    raise
            else:
                raise

        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                raise
            delay = RETRY_BASE_DELAY * (2 ** attempt)
            logger.warning(f"Unexpected error: {e}. Retrying in {delay}s...")
            await asyncio.sleep(delay)


def _extract_json_from_text(text: str) -> dict:
    """Safely extract JSON from Claude's response text."""
    try:
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0]
        elif "```" in text:
            text = text.split("```")[1].split("```")[0]
        return json.loads(text.strip())
    except json.JSONDecodeError:
        return None


async def propose_selection(analysis_data: dict) -> dict:
    """
    Phase 2: Based on analysis, propose products for benchmark.
    Returns structured selection with stars, emerging, and at-risk products.
    """
    client = _get_client()

    # Compact data to reduce tokens
    top_products = [
        {k: p.get(k) for k in ["IDIOMA", "Producto", "Precio", "Créditos", "ventas_total", "crecimiento_pct"] if p.get(k) is not None}
        for p in analysis_data.get('top_20', [])[:15]
    ]
    emerging = [
        {k: p.get(k) for k in ["IDIOMA", "Producto", "ventas_total", "crecimiento_pct"] if p.get(k) is not None}
        for p in analysis_data.get('emerging', [])[:8]
    ]
    declining = [
        {k: p.get(k) for k in ["IDIOMA", "Producto", "ventas_total", "crecimiento_pct"] if p.get(k) is not None}
        for p in analysis_data.get('declining', [])[:8]
    ]

    prompt = f"""Analista de producto formativo. Selecciona productos para benchmark competitivo.

KPIs: {json.dumps(analysis_data.get('kpis', {}), ensure_ascii=False)}
Top productos: {json.dumps(top_products, ensure_ascii=False)}
Emergentes (+15%): {json.dumps(emerging, ensure_ascii=False)}
En declive (-15%): {json.dumps(declining, ensure_ascii=False)}

Propón:
1. Estrellas (5-10): más vendidos
2. Emergentes (3-5): mayor crecimiento
3. En riesgo (3-5): mayor caída

Para cada uno: name, type, price, hours, total_sales, growth_pct, reason.

Responde SOLO JSON:
{{"stars":[{{"name":"...","type":"...","price":0,"hours":0,"total_sales":0,"growth_pct":0,"reason":"..."}}],"emerging":[...],"at_risk":[...],"summary":"Resumen en 2 frases."}}"""

    response = await _call_claude_with_retry(
        client,
        messages=[{"role": "user", "content": prompt}],
        model=RESEARCH_MODEL,
    )

    text = response.content[0].text
    result = _extract_json_from_text(text)
    if result:
        return result
    logger.warning("Could not parse selection JSON, returning raw text")
    return {"raw_response": text, "stars": [], "emerging": [], "at_risk": []}


async def research_single_product(product: dict, category: str) -> dict:
    """
    Research a single product against competitors using Claude with web_search tool.
    """
    client = _get_client()

    product_name = product.get("name", "Producto desconocido")
    product_type = product.get("type", "Curso")
    product_price = product.get("price", "No disponible")
    product_hours = product.get("hours", "No disponible")

    search_queries = _build_search_queries(product_name, product_type)

    educa_list = ", ".join(EDUCA_BRANDS[:10])
    prompt = f"""Eres investigador de mercado de formación online. Investiga competencia para este producto de EDUCA EDTECH Group:
- Producto: {product_name}
- Tipo: {product_type}
- Precio: {product_price}€
- Horas: {product_hours}
- Categoría: {category}

INSTRUCCIONES:
1. Busca con queries como: {json.dumps(search_queries[:2], ensure_ascii=False)}
2. Encuentra 3-5 competidores reales. Prioriza: UNIR, VIU, UDIMA, UOC, IMF, CEREM, ISEP, OBS, IEBS, Campus Training, MasterD.
3. Para cada competidor extrae de su web:
   - Precio exacto (o "Bajo consulta" con rango estimado)
   - Horas lectivas y créditos ECTS
   - Tipo titulación (Oficial/Propio/Certificado)
   - Atributos de valor: oposiciones, habilitante, prácticas, becas, financiación, doble titulación, metodología
   - URL real del producto
   - Diferenciador clave en su comunicación comercial
4. Marcas EDUCA EDTECH ({educa_list}) NO son competencia externa, etiquétalas "Grupo EDUCA".

Responde SOLO con JSON válido:
{{
  "our_product": "{product_name}",
  "competitors": [
    {{
      "competitor_name": "nombre institución",
      "product_name": "nombre exacto del producto",
      "price": "precio o rango",
      "hours": "horas lectivas",
      "ects": "créditos ECTS",
      "degree_type": "Oficial/Propio/Certificado",
      "value_attributes": "oposiciones, prácticas, becas, etc.",
      "url": "URL real",
      "key_differentiator": "principal ventaja competitiva",
      "is_educa_group": false
    }}
  ],
  "market_notes": "Observaciones del mercado: rango de precios, tendencias, nivel de competencia."
}}"""

    try:
        response = await _call_claude_with_retry(
            client,
            messages=[{"role": "user", "content": prompt}],
            tools=[{"type": "web_search_20250305", "name": "web_search", "max_uses": 7}],
            model=RESEARCH_MODEL,
        )

        # Process the response — may have multiple content blocks
        text_parts = []
        for block in response.content:
            if hasattr(block, "text"):
                text_parts.append(block.text)

        full_text = "\n".join(text_parts)

        result = _extract_json_from_text(full_text)
        if result:
            result["status"] = "success"
            return result

        return {
            "our_product": product_name,
            "competitors": [],
            "status": "partial",
            "raw_response": full_text[:2000],
            "market_notes": "No se pudo parsear la respuesta estructurada.",
        }

    except Exception as e:
        logger.error(f"Error researching {product_name}: {e}")
        return {
            "our_product": product_name,
            "competitors": [],
            "status": "error",
            "error": str(e),
        }


async def research_all_products(
    products: list, category: str, progress_callback: Optional[Callable] = None
) -> list:
    """
    Research all selected products with concurrency limit.
    progress_callback(product_name, index, total) is called for each product.
    """
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    results = []

    async def _research_with_semaphore(product, idx):
        async with semaphore:
            if progress_callback:
                await progress_callback(
                    product.get("name", "?"), idx, len(products)
                )
            result = await research_single_product(product, category)
            return result

    tasks = [
        _research_with_semaphore(p, i) for i, p in enumerate(products)
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Convert exceptions to error dicts
    final_results = []
    for i, r in enumerate(results):
        if isinstance(r, Exception):
            final_results.append({
                "our_product": products[i].get("name", "?"),
                "competitors": [],
                "status": "error",
                "error": str(r),
            })
        else:
            final_results.append(r)

    return final_results


async def strategic_analysis(
    analysis_data: dict,
    research_results: list,
    selected_products: dict,
) -> dict:
    """
    Phase 4: Generate strategic analysis — competitor stars + SWOT.
    """
    client = _get_client()

    prompt = f"""Eres un estratega de producto formativo senior de EDUCA EDTECH Group.

DATOS INTERNOS DE VENTAS:
{json.dumps(analysis_data.get('kpis', {}), ensure_ascii=False)}

PRODUCTOS SELECCIONADOS:
- Estrellas: {json.dumps(selected_products.get('stars', []), ensure_ascii=False)}
- Emergentes: {json.dumps(selected_products.get('emerging', []), ensure_ascii=False)}
- En riesgo: {json.dumps(selected_products.get('at_risk', []), ensure_ascii=False)}

RESEARCH DE COMPETENCIA:
{json.dumps(research_results, ensure_ascii=False, default=str)[:8000]}

{EDUCA_BRANDS_NOTE}

Genera un análisis estratégico completo:

1. **Productos estrella de competidores**: Identifica los productos de competidores con:
   - Alta visibilidad (aparecen repetidamente)
   - Propuesta de valor atractiva
   - Atributos diferenciadores únicos
   - Nichos no cubiertos por nosotros
   Clasifica cada uno como: "amenaza_directa", "oportunidad_nicho", o "referente_calidad"

2. **DAFO profundo** (mínimo 8-10 puntos por cuadrante):
   - Fortalezas: con datos de ventas
   - Debilidades: con ejemplos de competidores
   - Oportunidades: huecos de mercado
   - Amenazas: competidores agresivos, tendencias

Responde SOLO con JSON válido:
{{
  "competitor_stars": [
    {{
      "competitor": "...",
      "product": "...",
      "classification": "amenaza_directa|oportunidad_nicho|referente_calidad",
      "reason": "...",
      "impact": "..."
    }}
  ],
  "swot": {{
    "strengths": ["punto 1", "punto 2", ...],
    "weaknesses": ["punto 1", "punto 2", ...],
    "opportunities": ["punto 1", "punto 2", ...],
    "threats": ["punto 1", "punto 2", ...]
  }},
  "strategic_summary": "Resumen estratégico en 3-4 frases."
}}"""

    response = await _call_claude_with_retry(
        client,
        messages=[{"role": "user", "content": prompt}],
        max_tokens=6000,
    )

    text = response.content[0].text
    result = _extract_json_from_text(text)
    if result:
        return result
    return {"raw_response": text}


async def generate_proposals(
    analysis_data: dict,
    research_results: list,
    strategic_data: dict,
    selected_products: dict,
) -> dict:
    """
    Phase 5: Generate improvement proposals and new product suggestions.
    """
    client = _get_client()

    prompt = f"""Eres el director de producto de EDUCA EDTECH Group.

DATOS DE VENTAS:
{json.dumps(analysis_data.get('kpis', {}), ensure_ascii=False)}

INVESTIGACIÓN COMPETITIVA (resumen):
{json.dumps(research_results, ensure_ascii=False, default=str)[:6000]}

ANÁLISIS ESTRATÉGICO:
{json.dumps(strategic_data, ensure_ascii=False, default=str)[:4000]}

PRODUCTOS ACTUALES SELECCIONADOS:
{json.dumps(selected_products, ensure_ascii=False, default=str)[:3000]}

Genera propuestas concretas:

**Bloque A — Mejora de productos existentes:**
Para productos que lo necesiten: producto actual, precio actual → propuesto (con justificación), nombre actual → propuesto (si mejora SEO/comercial), horas/ECTS ajustados, atributos a añadir, prioridad (alta/media/baja), justificación.

**Bloque B — Nuevos productos propuestos:**
Para cada hueco detectado: denominación, tipo, facultad, escuela, institución educativa sugerida, precio recomendado, horas/ECTS, atributos clave, prioridad, justificación.

Responde SOLO con JSON válido:
{{
  "improvements": [
    {{
      "current_product": "...",
      "current_price": "...",
      "proposed_price": "...",
      "price_justification": "...",
      "current_name": "...",
      "proposed_name": "...",
      "current_hours_ects": "...",
      "proposed_hours_ects": "...",
      "attributes_to_add": ["..."],
      "priority": "alta|media|baja",
      "strategic_justification": "..."
    }}
  ],
  "new_products": [
    {{
      "name": "...",
      "type": "...",
      "faculty": "...",
      "school": "...",
      "institution": "...",
      "recommended_price": "...",
      "hours_ects": "...",
      "key_attributes": ["..."],
      "priority": "alta|media|baja",
      "strategic_justification": "..."
    }}
  ],
  "executive_summary": "Resumen de las propuestas en 3-4 frases."
}}"""

    response = await _call_claude_with_retry(
        client,
        messages=[{"role": "user", "content": prompt}],
        max_tokens=6000,
    )

    text = response.content[0].text
    result = _extract_json_from_text(text)
    if result:
        return result
    return {"raw_response": text}


def _build_search_queries(product_name: str, product_type: str) -> list:
    """Build search queries based on product type."""
    topic = product_name
    for prefix in [
        "Máster en ", "Master en ", "Curso de ", "Curso en ",
        "Postgrado en ", "Especialización en ", "Experto en ",
    ]:
        if topic.lower().startswith(prefix.lower()):
            topic = topic[len(prefix):]
            break

    ptype = product_type.lower() if product_type else "curso"
    year = _current_year()

    if "máster" in ptype or "master" in ptype:
        return [
            f"máster online {topic} precio horas",
            f"mejor máster {topic} online España {year}",
            f"máster {topic} créditos ECTS online",
        ]
    elif "postgrado" in ptype or "especialización" in ptype:
        return [
            f"postgrado {topic} online España",
            f"especialización {topic} online créditos",
            f"curso postgrado {topic} precio horas",
        ]
    elif "micro" in ptype:
        return [
            f"microcredencial {topic} universidad",
            f"certificado corto {topic} online",
        ]
    elif "licenciatura" in ptype or "grado" in ptype:
        return [
            f"licenciatura {topic} online",
            f"grado {topic} universidad online {year}",
            f"carrera {topic} online precio",
        ]
    elif "maestría" in ptype:
        return [
            f"maestría {topic} online",
            f"maestría {topic} universidad online {year}",
            f"master {topic} online precio ECTS",
        ]
    else:
        return [
            f"curso online {topic} certificado",
            f"curso {topic} online precio horas",
            f"formación {topic} online acreditada España {year}",
        ]
