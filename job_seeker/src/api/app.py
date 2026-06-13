from __future__ import annotations

from src.core.tracing import setup_langsmith_tracing

setup_langsmith_tracing()

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from psycopg_pool import AsyncConnectionPool

from src.agent.graph import build_graph
from src.api.auth.router import router as auth_router
from src.api.errors import parse_validation_errors
from src.api.chat.router import router as chat_router
from src.api.openapi_meta import APP_DESCRIPTION, OPENAPI_TAGS
from src.api.saved_jobs.router import router as saved_jobs_router
from src.api.sessions.router import me_router, router as sessions_router
from src.core.config import settings
from src.core.logger import get_logger
from src.db.client import close_pool
from src.db.langgraph_checkpoint import (
    ensure_langgraph_checkpoint_schema,
    postgres_conninfo_for_psycopg,
)

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: Postgres checkpoint schema + pool, compile LangGraph once into app.state.graph.
    Shutdown: close the checkpoint pool and the app's async DB pool."""
    
    dsn = postgres_conninfo_for_psycopg(settings.database_url)

    await ensure_langgraph_checkpoint_schema(dsn)

    pool = AsyncConnectionPool(conninfo=dsn, max_size=10, open=False)
    await pool.open()

    checkpointer = AsyncPostgresSaver(pool)
    app.state.graph = build_graph(checkpointer) 
    logger.info("LangGraph compiled with Postgres checkpointer (thread_id = session_id)")

    try:
        yield
    finally:
        await pool.close()
        await close_pool()


app = FastAPI(
    title="Job Seeker Chat API",
    version="1.0.0",
    lifespan=lifespan,
    description=APP_DESCRIPTION,
    openapi_tags=OPENAPI_TAGS,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    swagger_ui_parameters={
        "persistAuthorization": True,
        "displayRequestDuration": True,
        "filter": True,
        "tryItOutEnabled": True,
    },
)
app.include_router(auth_router)
app.include_router(chat_router)
app.include_router(sessions_router)
app.include_router(me_router)
app.include_router(saved_jobs_router)
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(
    _request: Request, exc: RequestValidationError
) -> JSONResponse:
    body = parse_validation_errors(exc.errors())
    return JSONResponse(status_code=422, content={"detail": body.model_dump()})


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", include_in_schema=False)
async def root_redirect_to_docs() -> RedirectResponse:
    return RedirectResponse(url="/docs", status_code=307)


@app.get("/health", tags=["health"], summary="Health check")
async def health() -> dict[str, str]:
    return {"status": "ok"}
