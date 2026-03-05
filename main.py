"""
main.py — FastAPI application for BenchmarkHub.
Manages sessions, file uploads, and orchestrates the 5-phase benchmark process.
Includes: basic auth, session persistence to disk, configurable CORS.
"""

import os
import uuid
import json
import asyncio
import logging
import shutil
import secrets
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, UploadFile, File, HTTPException, Request, Depends
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBasic, HTTPBasicCredentials

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
SESSION_DIR = Path(os.getenv("SESSION_DIR", "/app/sessions"))
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
SESSION_DIR.mkdir(parents=True, exist_ok=True)

# Auth config
AUTH_USERNAME = os.getenv("AUTH_USERNAME", "")
AUTH_PASSWORD = os.getenv("AUTH_PASSWORD", "")
AUTH_ENABLED = bool(AUTH_USERNAME and AUTH_PASSWORD)

# CORS config
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").split(",")

# ── App ──
app = FastAPI(title="BenchmarkHub", version="1.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

security = HTTPBasic()

# In-memory session cache (backed by JSON files)
sessions: dict = {}


# ── Auth ──
def verify_credentials(credentials: HTTPBasicCredentials = Depends(security)):
    """Verify HTTP Basic Auth credentials if auth is enabled."""
    if not AUTH_ENABLED:
        return True
    correct_user = secrets.compare_digest(credentials.username, AUTH_USERNAME)
    correct_pass = secrets.compare_digest(credentials.password, AUTH_PASSWORD)
    if not (correct_user and correct_pass):
        raise HTTPException(
            status_code=401,
            detail="Credenciales incorrectas",
            headers={"WWW-Authenticate": "Basic"},
        )
    return True


def optional_auth(request: Request):
    """If auth is enabled, require it. If not, pass through."""
    if not AUTH_ENABLED:
        return True
    # Extract credentials from Authorization header
    auth_header = request.headers.get("Authorization")
    if not auth_header:
        raise HTTPException(
            status_code=401,
            detail="Autenticación requerida",
            headers={"WWW-Authenticate": "Basic"},
        )
    return True


# ── Session persistence ──
def _save_session(session_id: str, data: dict):
    """Save session to disk as JSON."""
    path = SESSION_DIR / f"{session_id}.json"
    # Make a copy to avoid modifying the original
    save_data = {}
    for k, v in data.items():
        try:
            json.dumps(v)
            save_data[k] = v
        except (TypeError, ValueError):
            save_data[k] = str(v)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(save_data, f, ensure_ascii=False, indent=2, default=str)


def _load_session(session_id: str) -> Optional[dict]:
    """Load session from disk."""
    path = SESSION_DIR / f"{session_id}.json"
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def _load_all_sessions():
    """Load all sessions from disk on startup."""
    for path in SESSION_DIR.glob("*.json"):
        sid = path.stem
        try:
            with open(path, "r", encoding="utf-8") as f:
                sessions[sid] = json.load(f)
        except Exception as e:
            logger.warning(f"Could not load session {sid}: {e}")


# Load sessions on startup
@app.on_event("startup")
async def startup_load_sessions():
    _load_all_sessions()
    logger.info(f"Loaded {len(sessions)} sessions from disk")
    logger.info(f"Auth enabled: {AUTH_ENABLED}")


# ── Health ──
@app.get("/health")
async def health():
    return {
        "status": "ok",
        "timestamp": datetime.utcnow().isoformat(),
        "auth_enabled": AUTH_ENABLED,
    }


# ── Serve frontend ──
@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    html_path = Path(__file__).parent / "static" / "index.html"
    return HTMLResponse(content=html_path.read_text(encoding="utf-8"))


# ── Session management ──
def _get_session(session_id: str) -> dict:
    if session_id not in sessions:
        # Try loading from disk
        loaded = _load_session(session_id)
        if loaded:
            sessions[session_id] = loaded
        else:
            raise HTTPException(status_code=404, detail="Sesión no encontrada")
    return sessions[session_id]


def _session_dir(session_id: str) -> Path:
    d = OUTPUT_DIR / session_id
    d.mkdir(parents=True, exist_ok=True)
    return d


def _persist(session_id: str):
    """Convenience: save session to disk after mutation."""
    if session_id in sessions:
        _save_session(session_id, sessions[session_id])


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
    _persist(session_id)

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
        _persist(session_id)
        return {"session_id": session_id, "phase": 2, "selection": custom_selection}

    # Otherwise, auto-propose with Claude
    try:
        selection = await propose_selection(session["analysis"])
    except Exception as e:
        logger.error(f"Selection error: {e}")
        raise HTTPException(status_code=500, detail=f"Error generando selección: {str(e)}")

    session["selection"] = selection
    session["phase"] = 2
    _persist(session_id)

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
    _persist(session_id)
    return {"session_id": session_id, "phase": 2, "status": "confirmed", "selection": session["selection"]}


# ─────────────── PHASE 3: Competition Research ───────────────

@app.post("/api/phase3/research")
async def phase3_research(request: Request):
    """Research competition for all selected products.
    If research is already done, return cached results."""
    body = await request.json()
    session_id = body.get("session_id")
    session = _get_session(session_id)

    # Return cached results if already done
    if session.get("research") and session.get("phase", 0) >= 3:
        return {"session_id": session_id, "phase": 3, "research": session["research"]}

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
    _persist(session_id)

    return {"session_id": session_id, "phase": 3, "research": results}


# ── SSE endpoint for progress tracking ──
@app.get("/api/phase3/research/stream")
async def phase3_research_stream(session_id: str):
    """SSE endpoint to stream research progress."""
    from starlette.responses import StreamingResponse

    session = _get_session(session_id)

    # If already done, return immediately
    if session.get("research") and session.get("phase", 0) >= 3:
        async def done_stream():
            yield f"data: {json.dumps({'done': True, 'total_researched': len(session['research'])})}\n\n"
        return StreamingResponse(done_stream(), media_type="text/event-stream")

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
        _persist(session_id)
        yield f"data: {json.dumps({'done': True, 'total_researched': len(results)})}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


# ── Get research results (read-only, no re-execution) ──
@app.get("/api/phase3/results/{session_id}")
async def phase3_results(session_id: str):
    """Get cached research results without re-executing."""
    session = _get_session(session_id)
    if not session.get("research"):
        raise HTTPException(status_code=404, detail="Research no completado aún")
    return {"session_id": session_id, "phase": 3, "research": session["research"]}


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
        raise HTTPException(status_code=500, detail=f"Error en análisis: {str(e)}")

    session["strategic"] = result
    session["phase"] = 4
    _persist(session_id)

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
    _persist(session_id)

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
