"""
researcher.py — Competition research module using Anthropic Claude API
with web_search_20250305 tool for real web searches.
"""

import os
import json
import asyncio
import logging
from typing import Optional
import anthropic

logger = logging.getLogger(__name__)

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
MODEL = "claude-sonnet-4-20250514"
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT_SEARCHES", "3"))

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
    return anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)


async def propose_selection(analysis_data: dict) -> dict:
    """
    Phase 2: Based on analysis, propose products for benchmark.
    Returns structured selection with stars, emerging, and at-risk products.
    """
    client = _get_client()

    prompt = f"""Eres un analista de producto formativo de EDUCA EDTECH Group.

Basándote en estos datos de análisis de ventas, propón una selección de productos para benchmark competitivo.

DATOS DE ANÁLISIS:
- KPIs: {json.dumps(analysis_data.get('kpis', {}), ensure_ascii=False)}
- Top 20 productos: {json.dumps(analysis_data.get('top_20', [])[:20], ensure_ascii=False)}
- Productos emergentes (+15% crecimiento): {json.dumps(analysis_data.get('emerging', [])[:10], ensure_ascii=False)}
- Productos en declive (-15%): {json.dumps(analysis_data.get('declining', [])[:10], ensure_ascii=False)}
- Productos muertos: {json.dumps(analysis_data.get('dead_products', [])[:10], ensure_ascii=False)}
- Distribución por facultad: {json.dumps(analysis_data.get('by_faculty', [])[:10], ensure_ascii=False)}

Propón:
1. **Productos estrella** (5-10): los más vendidos, generan el grueso del revenue
2. **Productos emergentes** (3-5): mayor crecimiento reciente
3. **Productos en riesgo** (3-5): mayor caída sostenida, candidatos a renovar

Para cada producto incluye: nombre, tipo, precio (si disponible), horas (si disponible), ventas_total, crecimiento_pct (si disponible), y razón de selección.

Responde SOLO con un JSON válido con esta estructura:
{{
  "stars": [
    {{"name": "...", "type": "...", "price": ..., "hours": ..., "total_sales": ..., "growth_pct": ..., "reason": "..."}}
  ],
  "emerging": [...],
  "at_risk": [...],
  "summary": "Resumen ejecutivo de la selección en 2-3 frases."
}}"""

    response = await asyncio.to_thread(
        client.messages.create,
        model=MODEL,
        max_tokens=4096,
        messages=[{"role": "user", "content": prompt}],
    )

    text = response.content[0].text
    # Extract JSON from response
    try:
        # Try to find JSON block
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0]
        elif "```" in text:
            text = text.split("```")[1].split("```")[0]
        return json.loads(text.strip())
    except json.JSONDecodeError:
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

    # Build search strategy based on product type
    search_queries = _build_search_queries(product_name, product_type)

    prompt = f"""Eres un investigador de mercado de formación online para EDUCA EDTECH Group.

Tu tarea: investigar la competencia para este producto nuestro:
- **Producto**: {product_name}
- **Tipo**: {product_type}
- **Precio**: {product_price}
- **Horas**: {product_hours}
- **Categoría**: {category}

{EDUCA_BRANDS_NOTE}

{COMPETITOR_CONTEXT}

INSTRUCCIONES:
1. Busca en la web productos competidores similares al nuestro.
2. Usa queries como: {json.dumps(search_queries, ensure_ascii=False)}
3. Para cada competidor encontrado, extrae:
   - Nombre del competidor (institución)
   - Nombre exacto del producto
   - Precio (o "Bajo consulta" con rango estimado)
   - Horas lectivas
   - Créditos ECTS
   - Tipo de titulación (Oficial, Propio, Certificado, etc.)
   - Atributos de valor: oposiciones, habilitante, prácticas, metodología, financiación, becas
   - URL de la página del producto
   - Diferenciador clave
4. Encuentra al menos 3-5 competidores.
5. Si encuentras marcas del grupo EDUCA, etiquétalas como "Grupo EDUCA".

Responde SOLO con un JSON válido:
{{
  "our_product": "{product_name}",
  "competitors": [
    {{
      "competitor_name": "...",
      "product_name": "...",
      "price": "...",
      "hours": "...",
      "ects": "...",
      "degree_type": "...",
      "value_attributes": "...",
      "url": "...",
      "key_differentiator": "...",
      "is_educa_group": false
    }}
  ],
  "market_notes": "Observaciones generales del mercado para este tipo de producto."
}}"""

    try:
        response = await asyncio.to_thread(
            client.messages.create,
            model=MODEL,
            max_tokens=4096,
            tools=[{"type": "web_search_20250305", "name": "web_search", "max_uses": 10}],
            messages=[{"role": "user", "content": prompt}],
        )

        # Process the response — may have multiple content blocks
        text_parts = []
        for block in response.content:
            if hasattr(block, "text"):
                text_parts.append(block.text)

        full_text = "\n".join(text_parts)

        # Extract JSON
        try:
            if "```json" in full_text:
                json_str = full_text.split("```json")[1].split("```")[0]
            elif "```" in full_text:
                json_str = full_text.split("```")[1].split("```")[0]
            else:
                json_str = full_text
            result = json.loads(json_str.strip())
            result["status"] = "success"
            return result
        except json.JSONDecodeError:
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
    products: list, category: str, progress_callback=None
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

    response = await asyncio.to_thread(
        client.messages.create,
        model=MODEL,
        max_tokens=6000,
        messages=[{"role": "user", "content": prompt}],
    )

    text = response.content[0].text
    try:
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0]
        elif "```" in text:
            text = text.split("```")[1].split("```")[0]
        return json.loads(text.strip())
    except json.JSONDecodeError:
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

    response = await asyncio.to_thread(
        client.messages.create,
        model=MODEL,
        max_tokens=6000,
        messages=[{"role": "user", "content": prompt}],
    )

    text = response.content[0].text
    try:
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0]
        elif "```" in text:
            text = text.split("```")[1].split("```")[0]
        return json.loads(text.strip())
    except json.JSONDecodeError:
        return {"raw_response": text}


def _build_search_queries(product_name: str, product_type: str) -> list:
    """Build search queries based on product type."""
    # Extract topic from product name (remove common prefixes)
    topic = product_name
    for prefix in [
        "Máster en ", "Master en ", "Curso de ", "Curso en ",
        "Postgrado en ", "Especialización en ", "Experto en ",
    ]:
        if topic.lower().startswith(prefix.lower()):
            topic = topic[len(prefix):]
            break

    ptype = product_type.lower() if product_type else "curso"
    year = "2025"

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
    else:
        return [
            f"curso online {topic} certificado",
            f"curso {topic} online precio horas",
            f"formación {topic} online acreditada España {year}",
        ]
