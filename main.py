"""
main.py — FastAPI application for BenchmarkHub.
Manages sessions, file uploads, and orchestrates the 5-phase benchmark process.
"""

import os
import uuid
import json
import asyncio
import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

from analyzer import analyze_sales
from researcher import (
    propose_selection,
    research_all_products,
    strategic_analysis,
    generate_proposals,
)
from report_generator import (
    generate_html_report,
    generate_pdf_report,
    generate_excel_report,
)

# ── Config ──
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

UPLOAD_DIR = Path(os.getenv("UPLOAD_DIR", "/app/uploads"))
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "/app/outputs"))
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ── App ──
app = FastAPI(title="BenchmarkHub", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory session store
sessions: dict = {}


# ── Health ──
@app.get("/health")
async def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}


# ── Serve frontend ──
@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    html_path = Path(__file__).parent / "static" / "index.html"
    return HTMLResponse(content=html_path.read_text(encoding="utf-8"))


# ── Session management ──
def _get_session(session_id: str) -> dict:
    if session_id not in sessions:
        raise HTTPException(status_code=404, detail="Sesion no encontrada")
    return sessions[session_id]


def _session_dir(session_id: str) -> Path:
    d = OUTPUT_DIR / session_id
    d.mkdir(parents=True, exist_ok=True)
    return d


# ─────────────── PHASE 1: Upload & Analyze ───────────────

@app.post("/api/phase1/upload")
async def phase1_upload(file: UploadFile = File(...)):
    """Upload Excel and analyze sales data."""
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Solo se aceptan archivos .xlsx o .xls")

    session_id = str(uuid.uuid4())[:8]
    upload_path = UPLOAD_DIR / f"{session_id}_{file.filename}"

    # Save file
    with open(upload_path, "wb") as f:
        content = await file.read()
        f.write(content)

    # Analyze
    try:
        analysis = analyze_sales(str(upload_path))
    except Exception as e:
        logger.error(f"Analysis error: {e}")
        raise HTTPException(status_code=500, detail=f"Error al analizar: {str(e)}")

    if "error" in analysis and not analysis.get("year_columns"):
        raise HTTPException(status_code=400, detail=analysis["error"])

    # Store session
    sessions[session_id] = {
        "id": session_id,
        "filename": file.filename,
        "upload_path": str(upload_path),
        "phase": 1,
        "analysis": analysis,
        "selection": None,
        "research": None,
        "strategic": None,
        "proposals": None,
        "faculty_name": "General",
        "created_at": datetime.utcnow().isoformat(),
    }

    return {
        "session_id": session_id,
        "phase": 1,
        "analysis": analysis,
    }


# ─────────────── PHASE 2: Product Selection ───────────────

@app.post("/api/phase2/select")
async def phase2_select(request: Request):
    """Auto-propose products for benchmark, or accept user adjustments."""
    body = await request.json()
    session_id = body.get("session_id")
    session = _get_session(session_id)
    faculty_name = body.get("faculty_name", "General")
    session["faculty_name"] = faculty_name

    # If user provides custom selection, use it
    custom_selection = body.get("custom_selection")
    if custom_selection:
        session["selection"] = custom_selection
        session["phase"] = 2
        return {"session_id": session_id, "phase": 2, "selection": custom_selection}

    # Otherwise, auto-propose with Claude
    try:
        selection = await propose_selection(session["analysis"])
    except Exception as e:
        logger.error(f"Selection error: {e}")
        raise HTTPException(status_code=500, detail=f"Error generando seleccion: {str(e)}")

    session["selection"] = selection
    session["phase"] = 2

    return {"session_id": session_id, "phase": 2, "selection": selection}


@app.post("/api/phase2/confirm")
async def phase2_confirm(request: Request):
    """Confirm the product selection (with optional adjustments)."""
    body = await request.json()
    session_id = body.get("session_id")
    session = _get_session(session_id)

    adjusted = body.get("adjusted_selection")
    if adjusted:
        session["selection"] = adjusted

    session["phase"] = 2
    return {"session_id": session_id, "phase": 2, "status": "confirmed", "selection": session["selection"]}


# ─────────────── PHASE 3: Competition Research ───────────────

@app.post("/api/phase3/research")
async def phase3_research(request: Request):
    """Research competition for all selected products."""
    body = await request.json()
    session_id = body.get("session_id")
    session = _get_session(session_id)

    if not session.get("selection"):
        raise HTTPException(status_code=400, detail="Primero completa la Fase 2")

    selection = session["selection"]
    all_products = (
        selection.get("stars", [])
        + selection.get("emerging", [])
        + selection.get("at_risk", [])
    )

    if not all_products:
        raise HTTPException(status_code=400, detail="No hay productos seleccionados")

    category = session.get("faculty_name", "General")

    try:
        results = await research_all_products(all_products, category)
    except Exception as e:
        logger.error(f"Research error: {e}")
        raise HTTPException(status_code=500, detail=f"Error en research: {str(e)}")

    session["research"] = results
    session["phase"] = 3

    return {"session_id": session_id, "phase": 3, "research": results}


# ── SSE endpoint for progress tracking ──
@app.get("/api/phase3/research/stream")
async def phase3_research_stream(session_id: str):
    """SSE endpoint to stream research progress."""
    from starlette.responses import StreamingResponse

    session = _get_session(session_id)
    selection = session.get("selection", {})
    all_products = (
        selection.get("stars", [])
        + selection.get("emerging", [])
        + selection.get("at_risk", [])
    )

    async def event_stream():
        progress = {"current": 0, "total": len(all_products), "current_product": ""}

        async def progress_cb(name, idx, total):
            progress["current"] = idx + 1
            progress["current_product"] = name

        # Start research in background
        category = session.get("faculty_name", "General")
        task = asyncio.create_task(
            research_all_products(all_products, category, progress_callback=progress_cb)
        )

        # Stream progress
        while not task.done():
            data = json.dumps(progress)
            yield f"data: {data}\n\n"
            await asyncio.sleep(2)

        # Final result
        results = await task
        session["research"] = results
        session["phase"] = 3
        yield f"data: {json.dumps({'done': True, 'total_researched': len(results)})}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


# ─────────────── PHASE 4: Strategic Analysis ───────────────

@app.post("/api/phase4/analyze")
async def phase4_analyze(request: Request):
    """Generate strategic analysis: competitor stars + SWOT."""
    body = await request.json()
    session_id = body.get("session_id")
    session = _get_session(session_id)

    if not session.get("research"):
        raise HTTPException(status_code=400, detail="Primero completa la Fase 3")

    try:
        result = await strategic_analysis(
            session["analysis"],
            session["research"],
            session["selection"],
        )
    except Exception as e:
        logger.error(f"Strategic analysis error: {e}")
        raise HTTPException(status_code=500, detail=f"Error en analisis: {str(e)}")

    session["strategic"] = result
    session["phase"] = 4

    return {"session_id": session_id, "phase": 4, "strategic": result}


# ─────────────── PHASE 5: Proposals + Reports ───────────────

@app.post("/api/phase5/generate")
async def phase5_generate(request: Request):
    """Generate proposals and all 3 deliverables."""
    body = await request.json()
    session_id = body.get("session_id")
    session = _get_session(session_id)

    if not session.get("strategic"):
        raise HTTPException(status_code=400, detail="Primero completa la Fase 4")

    # Generate proposals
    try:
        props = await generate_proposals(
            session["analysis"],
            session["research"],
            session["strategic"],
            session["selection"],
        )
        session["proposals"] = props
    except Exception as e:
        logger.error(f"Proposals error: {e}")
        raise HTTPException(status_code=500, detail=f"Error generando propuestas: {str(e)}")

    # Generate reports
    out_dir = _session_dir(session_id)
    faculty = session.get("faculty_name", "General")
    deliverables = {}

    # HTML
    try:
        html_path = str(out_dir / "informe_benchmark.html")
        generate_html_report(
            session["analysis"], session["research"],
            session["strategic"], session["proposals"],
            faculty, html_path,
        )
        deliverables["html"] = f"/api/download/{session_id}/informe_benchmark.html"
    except Exception as e:
        logger.error(f"HTML report error: {e}")
        deliverables["html_error"] = str(e)

    # Excel
    try:
        xlsx_path = str(out_dir / "benchmark_datos.xlsx")
        generate_excel_report(
            session["analysis"], session["research"],
            session["strategic"], session["proposals"],
            faculty, xlsx_path,
        )
        deliverables["excel"] = f"/api/download/{session_id}/benchmark_datos.xlsx"
    except Exception as e:
        logger.error(f"Excel report error: {e}")
        deliverables["excel_error"] = str(e)

    # PDF
    try:
        pdf_path = str(out_dir / "informe_benchmark.pdf")
        await generate_pdf_report(
            session["analysis"], session["research"],
            session["strategic"], session["proposals"],
            faculty, pdf_path,
        )
        deliverables["pdf"] = f"/api/download/{session_id}/informe_benchmark.pdf"
    except Exception as e:
        logger.error(f"PDF report error: {e}")
        deliverables["pdf_error"] = str(e)

    session["phase"] = 5
    session["deliverables"] = deliverables

    return {
        "session_id": session_id,
        "phase": 5,
        "proposals": props,
        "deliverables": deliverables,
    }


# ── Download endpoint ──
@app.get("/api/download/{session_id}/{filename}")
async def download_file(session_id: str, filename: str):
    file_path = OUTPUT_DIR / session_id / filename
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="Archivo no encontrado")
    return FileResponse(
        path=str(file_path),
        filename=filename,
        media_type="application/octet-stream",
    )


# ── Session state endpoint ──
@app.get("/api/session/{session_id}")
async def get_session(session_id: str):
    session = _get_session(session_id)
    return {
        "session_id": session_id,
        "phase": session.get("phase", 0),
        "faculty_name": session.get("faculty_name", "General"),
        "has_analysis": session.get("analysis") is not None,
        "has_selection": session.get("selection") is not None,
        "has_research": session.get("research") is not None,
        "has_strategic": session.get("strategic") is not None,
        "has_proposals": session.get("proposals") is not None,
        "deliverables": session.get("deliverables", {}),
    }


# ── Mount static files (fallback) ──
app.mount("/static", StaticFiles(directory=str(Path(__file__).parent / "static")), name="static")
