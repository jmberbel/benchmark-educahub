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
    Phase 2: Based on analysis, propose SPECIFIC products for benchmark.
    Returns structured selection with stars, emerging, and at-risk products.
    """
    client = _get_client()

    # Compact product data — only essential fields to stay under token limits
    name_col = analysis_data.get("name_column", "Producto")
    essential_keys = [name_col, "IDIOMA", "Producto", "Precio", "Créditos", "Horas",
                      "ventas_total", "crecimiento_pct", "importe_total"]

    def _compact(products, limit):
        return [
            {k: p[k] for k in essential_keys if k in p and p[k] is not None}
            for p in (products or [])[:limit]
        ]

    top_products = _compact(analysis_data.get("top_20", []), 15)
    emerging = _compact(analysis_data.get("emerging", []), 8)
    declining = _compact(analysis_data.get("declining", []), 8)
    dead = analysis_data.get("dead_products", [])[:8]

    prompt = f"""Eres analista de producto formativo senior de EDUCA EDTECH Group.

DATOS DE ANÁLISIS DE VENTAS:
- KPIs: {json.dumps(analysis_data.get('kpis', {}), ensure_ascii=False)}
- Top 15 productos: {json.dumps(top_products, ensure_ascii=False)}
- Emergentes (+15%): {json.dumps(emerging, ensure_ascii=False)}
- En declive (-15%): {json.dumps(declining, ensure_ascii=False)}
- Muertos: {json.dumps(dead, ensure_ascii=False)}

INSTRUCCIONES CRÍTICAS:
- Selecciona productos ESPECÍFICOS con su nombre EXACTO del Excel, precio real e información detallada
- NO agrupes por categoría genérica (ej: NO "ELE" genérico, SÍ "Curso de Profesor de Español para Extranjeros ELE - 260€ - 8 ECTS")
- Incluye el precio, horas y créditos reales de cada producto
- El campo "name" debe ser la denominación exacta del producto como aparece en el Excel
- El campo "type" debe indicar el tipo: Curso, Máster, Postgrado, Microcredencial, etc.

Propón:
1. **Productos estrella** (5-10): los más vendidos, generan el grueso del revenue. Selecciona los productos individuales concretos.
2. **Productos emergentes** (3-5): mayor crecimiento reciente, potencial de scaling.
3. **Productos en riesgo** (3-5): mayor caída sostenida, candidatos a renovar o sustituir.

Para cada producto incluye TODOS estos campos con datos reales del Excel:
- name: denominación exacta del producto
- type: tipo de producto (Curso/Máster/Postgrado/etc.)
- price: precio real en euros
- hours: horas lectivas (si disponible)
- ects: créditos ECTS (si disponible)
- total_sales: ventas totales acumuladas
- growth_pct: porcentaje de crecimiento interanual
- reason: razón de selección con datos concretos

Responde SOLO con JSON válido:
{{
  "stars": [
    {{"name": "Denominación exacta", "type": "Curso", "price": 260, "hours": 200, "ects": 8, "total_sales": 480, "growth_pct": -57, "reason": "Producto estrella en ventas con..."}}
  ],
  "emerging": [...],
  "at_risk": [...],
  "summary": "Resumen ejecutivo de la selección en 2-3 frases."
}}"""

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
    product_ects = product.get("ects", "No disponible")

    search_queries = _build_search_queries(product_name, product_type)

    educa_list = ", ".join(EDUCA_BRANDS[:10])
    prompt = f"""Eres investigador de mercado de formación online. Investiga competencia para este producto de EDUCA EDTECH Group:
- Producto: {product_name}
- Tipo: {product_type}
- Precio: {product_price}€
- Horas: {product_hours}
- ECTS: {product_ects}
- Categoría: {category}

INSTRUCCIONES:
1. Busca con queries como: {json.dumps(search_queries[:2], ensure_ascii=False)}
2. Encuentra 3-5 competidores reales. Prioriza: UNIR, VIU, UDIMA, UOC, IMF, CEREM, ISEP, OBS, IEBS, Campus Training, MasterD, Oxford House, Instituto Cervantes.
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

    # Build a richer competitor summary for better analysis
    competitor_summary = []
    for r in research_results:
        for c in (r.get("competitors") or []):
            competitor_summary.append({
                "our_product": str(r.get("our_product", "?")),
                "competitor": str(c.get("competitor_name", "?")),
                "product": str(c.get("product_name", "?")),
                "price": str(c.get("price", "?")),
                "ects": str(c.get("ects", "?")),
                "degree_type": str(c.get("degree_type", "?")),
                "value_attrs": str(c.get("value_attributes", "?")),
                "differentiator": str(c.get("key_differentiator", "?")),
            })

    # Pre-serialize data to avoid f-string issues with dicts
    kpis_json = json.dumps(analysis_data.get('kpis', {}), ensure_ascii=False, default=str)
    stars_json = json.dumps(selected_products.get('stars', []), ensure_ascii=False, default=str)
    emerging_json = json.dumps(selected_products.get('emerging', []), ensure_ascii=False, default=str)
    at_risk_json = json.dumps(selected_products.get('at_risk', []), ensure_ascii=False, default=str)
    competitor_json = json.dumps(competitor_summary, ensure_ascii=False, default=str)[:12000]

    market_notes = []
    for r in research_results:
        if r.get("market_notes"):
            market_notes.append({
                "product": str(r.get("our_product", "?")),
                "notes": str(r.get("market_notes", "")),
            })
    market_notes_json = json.dumps(market_notes, ensure_ascii=False, default=str)[:3000]

    prompt = f"""Eres un estratega de producto formativo senior de EDUCA EDTECH Group.

DATOS INTERNOS DE VENTAS:
{kpis_json}

PRODUCTOS SELECCIONADOS:
- Estrellas: {stars_json}
- Emergentes: {emerging_json}
- En riesgo: {at_risk_json}

MAPA COMPETITIVO COMPLETO ({len(competitor_summary)} competidores encontrados):
{competitor_json}

NOTAS DE MERCADO POR PRODUCTO:
{market_notes_json}

{EDUCA_BRANDS_NOTE}

Genera un análisis estratégico EXHAUSTIVO:

1. **Productos estrella de competidores** (mínimo 8-12): Identifica los MEJORES productos de cada competidor:
   - Alta visibilidad SEO (aparecen repetidamente en resultados)
   - Propuesta de valor muy atractiva (combinación precio-horas-ECTS favorable)
   - Atributos diferenciadores únicos (habilitante, oposiciones, partnerships)
   - Cubren nichos no presentes en nuestro catálogo
   Clasifica cada uno como: "amenaza_directa", "oportunidad_nicho", o "referente_calidad"

2. **DAFO profundo** (mínimo 10 puntos por cuadrante, con datos concretos):
   - Fortalezas: respaldadas por datos de ventas nuestros
   - Debilidades: con ejemplos específicos de competidores que nos superan
   - Oportunidades: huecos de mercado detectados, tendencias, nichos sin explotar
   - Amenazas: competidores agresivos con precios/atributos, tendencias tecnológicas, apps de idiomas

Responde SOLO con JSON válido:
{{
  "competitor_stars": [
    {{
      "competitor": "nombre institución",
      "product": "nombre producto con detalle",
      "price": "precio",
      "classification": "amenaza_directa|oportunidad_nicho|referente_calidad",
      "reason": "por qué es estrella (atributos, visibilidad, nicho)",
      "impact": "impacto concreto en nuestro catálogo"
    }}
  ],
  "swot": {{
    "strengths": ["punto 1 con datos", "punto 2 con datos", ...],
    "weaknesses": ["punto 1 con ejemplo competidor", ...],
    "opportunities": ["punto 1: hueco de mercado X", ...],
    "threats": ["punto 1: competidor X con precio agresivo Y", ...]
  }},
  "strategic_summary": "Resumen estratégico en 3-4 frases con conclusiones accionables."
}}"""

    response = await _call_claude_with_retry(
        client,
        messages=[{"role": "user", "content": prompt}],
        max_tokens=8000,
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

    # Build compact but complete competitor data
    competitor_data = []
    for r in research_results:
        entry = {"our_product": str(r.get("our_product", "?")), "competitors": []}
        for c in (r.get("competitors") or []):
            entry["competitors"].append({
                "name": str(c.get("competitor_name", "?")),
                "product": str(c.get("product_name", "?")),
                "price": str(c.get("price", "?")),
                "ects": str(c.get("ects", "?")),
                "attrs": str(c.get("value_attributes", "?")),
            })
        if entry["competitors"]:
            competitor_data.append(entry)

    # Pre-serialize all data to avoid f-string issues
    p5_kpis_json = json.dumps(analysis_data.get('kpis', {}), ensure_ascii=False, default=str)
    p5_catalog_json = json.dumps(analysis_data.get('top_20', [])[:20], ensure_ascii=False, default=str)[:4000]
    p5_competitor_json = json.dumps(competitor_data, ensure_ascii=False, default=str)[:8000]
    p5_strategic_json = json.dumps(strategic_data, ensure_ascii=False, default=str)[:5000]
    p5_selected_json = json.dumps(selected_products, ensure_ascii=False, default=str)[:3000]

    prompt = f"""Eres el director de producto de EDUCA EDTECH Group. Genera propuestas EXHAUSTIVAS y DETALLADAS.

DATOS DE VENTAS:
{p5_kpis_json}

TODOS LOS PRODUCTOS DEL CATÁLOGO:
{p5_catalog_json}

INVESTIGACIÓN COMPETITIVA:
{p5_competitor_json}

ANÁLISIS ESTRATÉGICO (DAFO + Estrellas competidores):
{p5_strategic_json}

PRODUCTOS SELECCIONADOS:
{p5_selected_json}

INSTRUCCIONES CRÍTICAS:
- Genera propuestas ESPECÍFICAS Y DETALLADAS, no genéricas
- CADA producto en riesgo o en declive debe tener una propuesta de mejora
- Los precios propuestos deben basarse en los precios de la competencia encontrados
- Los nuevos productos deben cubrir huecos detectados en el research

**Bloque A — Mejora de productos existentes (mínimo 5-8 propuestas):**
Para CADA producto que necesite mejora (en declive, en riesgo, o superado por competencia):
- Producto actual con nombre exacto
- Precio actual → Precio propuesto (justificado con precios de competidores encontrados)
- Nombre actual → Nombre propuesto (si mejora SEO/comercial)
- Horas/ECTS actuales → propuestos
- Atributos concretos a añadir: módulo IA, validez oposiciones, prácticas, certificación, etc.
- Prioridad alta/media/baja
- Justificación con referencia a competidores específicos

**Bloque B — Nuevos productos propuestos (mínimo 10-15 propuestas):**
Para CADA hueco de mercado detectado:
- Denominación completa del nuevo producto
- Tipo: Curso/Máster/Postgrado/Microcredencial/Experto
- Facultad y Escuela asignadas
- Institución Educativa que coexpediría (Universidad Nebrija, UTAMED, etc.)
- Precio recomendado (rango con justificación de mercado)
- Horas lectivas / créditos ECTS
- Atributos clave: ECTS, oposiciones, habilitante, prácticas, certificación, doble titulación
- Público objetivo específico
- Prioridad alta/media/baja
- Justificación estratégica con datos de competencia

Responde SOLO con JSON válido:
{{
  "improvements": [
    {{
      "current_product": "nombre exacto del producto actual",
      "current_price": "precio actual €",
      "proposed_price": "precio propuesto € (rango)",
      "price_justification": "Competidores X cobra Y€, Z cobra W€",
      "current_name": "nombre actual",
      "proposed_name": "nombre propuesto si aplica",
      "current_hours_ects": "horas/ECTS actuales",
      "proposed_hours_ects": "horas/ECTS propuestos",
      "attributes_to_add": ["atributo1", "atributo2"],
      "priority": "alta|media|baja",
      "strategic_justification": "justificación con datos de competencia"
    }}
  ],
  "new_products": [
    {{
      "name": "Denominación completa",
      "type": "Máster/Curso/Postgrado/Experto/Microcredencial",
      "faculty": "Facultad asignada",
      "school": "Escuela asignada",
      "institution": "IE que coexpide",
      "recommended_price": "rango precio €",
      "hours_ects": "horas / ECTS",
      "key_attributes": ["ECTS", "oposiciones", "prácticas", etc.],
      "target_audience": "público objetivo",
      "priority": "alta|media|baja",
      "strategic_justification": "hueco detectado + datos competencia"
    }}
  ],
  "executive_summary": "Resumen ejecutivo de las propuestas en 3-4 frases."
}}"""

    response = await _call_claude_with_retry(
        client,
        messages=[{"role": "user", "content": prompt}],
        max_tokens=8000,
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
