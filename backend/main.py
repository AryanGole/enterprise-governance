"""
Enterprise Metadata & Data Lineage Management System
FastAPI Application Entry Point

Architecture: Hexagonal / Clean Architecture
Author: Data Governance Platform Team
"""

from fastapi import FastAPI, Request
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
import logging
import time
import uuid

from api.routes import (
    datasets, lineage, schemas, governance,
    dictionary, impact, audit, search, pipelines
)
from core.config import settings
from core.database import init_db
from core.neo4j_client import init_neo4j

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown lifecycle manager."""
    logger.info("Initializing Enterprise Governance Platform v2.1.0")
    await init_db()
    await init_neo4j()
    logger.info("All subsystems online. Platform ready.")
    yield
    logger.info("Shutting down platform gracefully...")


app = FastAPI(
    title="Enterprise Data Governance Platform",
    lifespan=lifespan,
    description="""
    Centralized metadata & lineage management for enterprise analytics pipelines.
    
    ## Capabilities
    - **Metadata Management**: Dataset schemas, ownership, classification
    - **Lineage Engine**: Source-to-target, column-level lineage graphs
    - **Schema Evolution**: Version history, breaking change detection
    - **Data Dictionary**: Auto-generated business & technical definitions
    - **Impact Analysis**: Downstream dependency assessment
    - **Governance Controls**: Audit logging, approval workflows
    """,
    version="2.1.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json"
)

# ── Middleware ────────────────────────────────────────────────────────────────

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(GZipMiddleware, minimum_size=1000)


@app.middleware("http")
async def request_correlation_middleware(request: Request, call_next):
    """Attach correlation ID to every request for distributed tracing."""
    correlation_id = request.headers.get("X-Correlation-ID", str(uuid.uuid4()))
    request.state.correlation_id = correlation_id
    start = time.perf_counter()

    response = await call_next(request)

    duration_ms = (time.perf_counter() - start) * 1000
    response.headers["X-Correlation-ID"] = correlation_id
    response.headers["X-Response-Time-Ms"] = f"{duration_ms:.2f}"

    logger.info(
        f"method={request.method} path={request.url.path} "
        f"status={response.status_code} duration_ms={duration_ms:.2f} "
        f"correlation_id={correlation_id}"
    )
    return response


# ── Router Registration ───────────────────────────────────────────────────────

API_V1 = "/api/v1"

app.include_router(datasets.router,    prefix=f"{API_V1}/datasets",    tags=["Metadata"])
app.include_router(lineage.router,     prefix=f"{API_V1}/lineage",     tags=["Lineage"])
app.include_router(schemas.router,     prefix=f"{API_V1}/schemas",     tags=["Schema Evolution"])
app.include_router(governance.router,  prefix=f"{API_V1}/governance",  tags=["Governance"])
app.include_router(dictionary.router,  prefix=f"{API_V1}/dictionary",  tags=["Data Dictionary"])
app.include_router(impact.router,      prefix=f"{API_V1}/impact",      tags=["Impact Analysis"])
app.include_router(audit.router,       prefix=f"{API_V1}/audit",       tags=["Audit"])
app.include_router(search.router,      prefix=f"{API_V1}/search",      tags=["Search"])
app.include_router(pipelines.router,   prefix=f"{API_V1}/pipelines",   tags=["Pipelines"])


# ── Lifecycle ─────────────────────────────────────────────────────────────────

# Lifecycle managed by lifespan context manager above


# ── Health & Diagnostics ──────────────────────────────────────────────────────

@app.get("/health", tags=["System"])
async def health_check():
    return {
        "status": "healthy",
        "version": "2.1.0",
        "platform": "Enterprise Data Governance Platform",
        "subsystems": {
            "postgres": "connected",
            "neo4j": "connected",
            "openlineage": "active"
        }
    }


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "error": "internal_server_error",
            "message": "An unexpected error occurred.",
            "correlation_id": getattr(request.state, "correlation_id", None)
        }
    )
