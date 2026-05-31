#!/usr/bin/env python3
"""
crawler_workflow.py
===================
DBOS workflow: schedules crawling at midnight, runs ITviec and TopCV crawlers in parallel.
Uses SQLite to persist workflow state.

Usage:
    python crawler_workflow.py                        # Run the scheduler (wait until midnight)
    python crawler_workflow.py --trigger              # Trigger the crawl immediately
    python crawler_workflow.py --ui                   # Run scheduler with Web UI (http://localhost:8090)
    python crawler_workflow.py --trigger --ui         # Trigger crawl immediately with Web UI
"""

import argparse
import os
import subprocess
import sys
import threading

import psycopg2
import httpx
from datetime import datetime
from pathlib import Path
from typing import Any

# Ensure project root is on sys.path for src/ imports
_project_root = str(Path(__file__).resolve().parents[1])
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

# Load .env from project root before importing src (pydantic-settings needs env vars)
from dotenv import load_dotenv
load_dotenv(Path(_project_root) / ".env")

from bs4 import BeautifulSoup
from dbos import DBOS

DB_PATH = Path("crawler_state.sqlite").resolve()

from db import save_json, load_json

from src.ingest.json_loader import load_jobs_from_file
from src.ingest.embed import _build_embed_text
from src.models.job_schema import Job
from src.core.config import settings

from itviec import (
    get_page, wait as itv_wait,
    LOG_DIR as ITV_LOG_DIR, BASE_URL as ITV_BASE_URL,
    login as itv_login,
    parse_job_list, parse_job_detail,
)
from itviec.build_schema_itviec import build_schema_file as itv_build_schema

from topcv import (
    get_page as tcv_get, page_url, wait as tcv_wait,
    LOG_DIR as TCV_LOG_DIR, BASE_URL as TCV_BASE_URL,
    extract_job_list_data, extract_job_detail_data,
)
from topcv.build_schema_topcv import build_schema_file as tcv_build_schema

OUTPUT_DIR = Path("data_job")
ITV_EXISTING_IDS_FILE = OUTPUT_DIR / "itviec_existing_ids.json"
TCV_EXISTING_IDS_FILE = OUTPUT_DIR / "topcv_existing_ids.json"

ITV_MAX_PAGES = 1
ITV_MAX_JOBS_PER_RUN = 1
TCV_MAX_PAGES = 1
TCV_MAX_JOBS_PER_RUN = 1


def _get_output_file(site: str) -> Path:
    return OUTPUT_DIR / datetime.now().strftime("%Y%m%d") / f"{site}_jobs.json"


def _seed_ids(file: Path, existing_ids: set) -> set:
    if existing_ids:
        return existing_ids
    old = OUTPUT_DIR / file.name
    if old.exists():
        old_jobs = load_json(old)
        existing_ids.update(j["job_id"] for j in old_jobs if j.get("job_id"))
    return existing_ids


# ITviec STEPS 

@DBOS.step()
def itv_login_step():
    if not itv_login():
        raise Exception("ITviec login failed")


@DBOS.step()
def itv_fetch_step() -> list:
    pages_html = []
    for page_num in range(1, ITV_MAX_PAGES + 1):
        url = ITV_BASE_URL if page_num == 1 else f"{ITV_BASE_URL}?page={page_num}"
        soup = get_page(url)
        if not soup:
            itv_wait()
            continue
        pages_html.append(str(soup))
        itv_wait()
    return pages_html


@DBOS.step()
def itv_parse_step(pages_html: list) -> dict:
    existing_ids = _seed_ids(ITV_EXISTING_IDS_FILE, set(load_json(ITV_EXISTING_IDS_FILE)))
    jobs = []
    new_count = 0
    for html in pages_html:
        soup = BeautifulSoup(html, "lxml")
        list_jobs = parse_job_list(soup)
        for job in list_jobs:
            if ITV_MAX_JOBS_PER_RUN > 0 and new_count >= ITV_MAX_JOBS_PER_RUN:
                break
            job_id = job.get("job_id")
            if job_id in existing_ids:
                continue
            existing_ids.add(job_id)
            new_count += 1
            if job.get("url"):
                itv_wait()
                detail_soup = get_page(job["url"])
                if detail_soup:
                    detail = parse_job_detail(detail_soup)
                    job.update({k: v for k, v in detail.items() if v})
                    (ITV_LOG_DIR / f"itviec_job_{job_id}.html").write_text(str(detail_soup), encoding="utf-8")
            jobs.append(job)
        if ITV_MAX_JOBS_PER_RUN > 0 and new_count >= ITV_MAX_JOBS_PER_RUN:
            break
    return {"jobs": jobs, "ids": sorted(existing_ids)}


@DBOS.step()
def itv_save_step(jobs: list, ids_list: list):
    output_file = _get_output_file("itviec")
    all_jobs = load_json(output_file)
    all_jobs.extend(jobs)
    save_json(output_file, all_jobs)
    save_json(ITV_EXISTING_IDS_FILE, ids_list)


# TopCV STEPS 

@DBOS.step()
def tcv_fetch_step() -> list:
    pages_html = []
    for page_num in range(1, TCV_MAX_PAGES + 1):
        url = page_url(page_num)
        referer = page_url(page_num - 1) if page_num > 1 else TCV_BASE_URL
        soup = tcv_get(url, referer=referer)
        if not soup:
            tcv_wait()
            continue
        pages_html.append(str(soup))
        tcv_wait()
    return pages_html


@DBOS.step()
def tcv_parse_step(pages_html: list) -> dict:
    existing_ids = _seed_ids(TCV_EXISTING_IDS_FILE, set(load_json(TCV_EXISTING_IDS_FILE)))
    jobs = []
    new_count = 0
    for html in pages_html:
        soup = BeautifulSoup(html, "lxml")
        list_jobs = extract_job_list_data(soup)
        for job in list_jobs:
            if TCV_MAX_JOBS_PER_RUN > 0 and new_count >= TCV_MAX_JOBS_PER_RUN:
                break
            job_id = job.get("job_id")
            if job_id and job_id in existing_ids:
                continue
            existing_ids.add(job_id)
            jobs.append(job)
            new_count += 1
            if job.get("url"):
                tcv_wait()
                detail_soup = tcv_get(job["url"])
                if detail_soup:
                    detail = extract_job_detail_data(detail_soup, job["url"])
                    job.update({k: v for k, v in detail.items() if v})
                    (TCV_LOG_DIR / f"topcv_job_{job_id}.html").write_text(str(detail_soup), encoding="utf-8")
        if TCV_MAX_JOBS_PER_RUN > 0 and new_count >= TCV_MAX_JOBS_PER_RUN:
            break
    return {"jobs": jobs, "ids": sorted(existing_ids)}


@DBOS.step()
def tcv_save_step(jobs: list, ids_list: list):
    output_file = _get_output_file("topcv")
    all_jobs = []
    all_jobs.extend(jobs)
    save_json(output_file, all_jobs)
    save_json(TCV_EXISTING_IDS_FILE, ids_list)


# SCHEMA STEPS 

@DBOS.step()
def itv_build_schema_step():
    today = datetime.now().strftime("%Y%m%d")
    f = OUTPUT_DIR / today / "itviec_jobs.json"
    if f.exists():
        itv_build_schema(f)
    else:
        print(f"  No ITviec data found at {f}")


@DBOS.step()
def tcv_build_schema_step():
    today = datetime.now().strftime("%Y%m%d")
    f = OUTPUT_DIR / today / "topcv_jobs.json"
    if f.exists():
        tcv_build_schema(f)
    else:
        print(f"  No TopCV data found at {f}")


# INGEST STEPS (embed + upsert) 

def _schema_filepath(site: str) -> Path:
    today = datetime.now().strftime("%Y%m%d")
    return OUTPUT_DIR / today / f"{site}_jobs_schema.json"


@DBOS.step()
def json_loader_step(site: str) -> list[dict]:
    """Load today's schema JSON file for a given site."""
    t0 = datetime.now()
    path = _schema_filepath(site)
    if not path.exists():
        print(f"  Schema file not found: {path}")
        return []
    jobs = load_jobs_from_file(path)
    elapsed = (datetime.now() - t0).total_seconds()
    print(f"  json_loader({site}): {len(jobs)} jobs in {elapsed:.2f}s")
    return jobs


@DBOS.step()
def embed_and_upsert_step(raw_jobs: list[dict]):
    """Embed jobs and upsert to PostgreSQL (fully synchronous)."""
    if not raw_jobs:
        print("  No jobs to embed/upsert")
        return

    t0 = datetime.now()
    BGE_MODEL = "bge-m3"
    EMBED_BATCH_SIZE = 4
    OLLAMA_BASE_URL = settings.ollama_base_url

    # 1. Parse dict -> Job objects
    job_objects: list[Job] = [Job.from_json(j) for j in raw_jobs]

    # 2. Build texts to embed
    texts = [_build_embed_text(job) for job in job_objects]

    # 3. Embed synchronously using httpx.Client (no asyncio)
    all_embeddings: list[list[float]] = []
    with httpx.Client(base_url=OLLAMA_BASE_URL, timeout=300.0) as client:
        for i in range(0, len(texts), EMBED_BATCH_SIZE):
            batch = texts[i:i + EMBED_BATCH_SIZE]
            print(f"  Embedding batch {i // EMBED_BATCH_SIZE + 1}/{len(texts) // EMBED_BATCH_SIZE + 1} ({len(batch)} jobs)...")
            resp = client.post("/api/embed", json={"model": BGE_MODEL, "input": batch})
            resp.raise_for_status()
            all_embeddings.extend(resp.json()["embeddings"])

    for job, emb in zip(job_objects, all_embeddings):
        job.embedding = emb
    print(f"  Embedded {len(job_objects)} jobs in {(datetime.now() - t0).total_seconds():.1f}s")

    # 4. Clean invalid UTF-8 bytes before upsert
    for job in job_objects:
        _clean_job_utf8(job)

    # 5. Upsert to PostgreSQL (sync with psycopg2)
    dsn = _normalize_pg_dsn(settings.database_url)
    _upsert_jobs_sync(job_objects, dsn)
    print(f"  Upserted {len(job_objects)} jobs")


def _normalize_pg_dsn(dsn: str) -> str:
    for prefix in ("postgresql+asyncpg://", "postgresql+psycopg2://"):
        if dsn.startswith(prefix):
            return "postgresql://" + dsn[len(prefix):]
    return dsn


def _upsert_jobs_sync(jobs: list[Job], dsn: str):
    query = """
        INSERT INTO jobs (
            job_id, source, url, title, 
            company_name, company_url, company_id, company_size, company_industry, country,
            salary_raw, salary_min, salary_max, salary_currency, salary_negotiable,
            location_raw, locations,
            job_domains, job_level, description, requirements, skills, 
            experience_years_min, education, benefits, 
            work_mode, work_mode_days, overtime_policy, hiring_quantity, deadline,
            posted_date, crawled_date,
            created_at, updated_at,
            embedding
        )
        VALUES (
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s,
            %s, %s,
            %s::vector
        )
        ON CONFLICT (source, job_id) DO UPDATE SET
            url                  = EXCLUDED.url,
            title                = EXCLUDED.title,
            company_name         = EXCLUDED.company_name,
            company_url          = EXCLUDED.company_url,
            company_id           = EXCLUDED.company_id,
            company_size         = EXCLUDED.company_size,
            company_industry     = EXCLUDED.company_industry,
            country              = EXCLUDED.country,
            salary_raw           = EXCLUDED.salary_raw,
            salary_min           = EXCLUDED.salary_min,
            salary_max           = EXCLUDED.salary_max,
            salary_currency      = EXCLUDED.salary_currency,
            salary_negotiable    = EXCLUDED.salary_negotiable,
            location_raw         = EXCLUDED.location_raw,
            locations            = EXCLUDED.locations,
            job_domains          = EXCLUDED.job_domains,
            job_level            = EXCLUDED.job_level,
            description          = EXCLUDED.description,
            requirements         = EXCLUDED.requirements,
            skills               = EXCLUDED.skills,
            experience_years_min = EXCLUDED.experience_years_min,
            education            = EXCLUDED.education,
            benefits             = EXCLUDED.benefits,
            work_mode            = EXCLUDED.work_mode,
            work_mode_days       = EXCLUDED.work_mode_days,
            overtime_policy      = EXCLUDED.overtime_policy,
            hiring_quantity      = EXCLUDED.hiring_quantity,
            deadline             = EXCLUDED.deadline,
            posted_date          = EXCLUDED.posted_date,
            crawled_date         = EXCLUDED.crawled_date,
            updated_at           = now(),
            embedding            = EXCLUDED.embedding
    """
    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            for job in jobs:
                cur.execute(query, (
                    job.job_id, job.source, job.url, job.title,
                    job.company_name, job.company_url, job.company_id, job.company_size,
                    list(job.company_industry) if job.company_industry else [], job.country,
                    job.salary_raw, job.salary_min, job.salary_max, job.salary_currency, job.salary_negotiable,
                    job.location_raw, list(job.locations),
                    list(job.job_domains), job.job_level, job.description, job.requirements, list(job.skills),
                    job.experience_years_min, job.education, job.benefits,
                    job.work_mode, job.work_mode_days, job.overtime_policy, job.hiring_quantity, job.deadline,
                    job.posted_date, job.crawled_date,
                    job.created_at, job.updated_at,
                    _to_pgvector_literal(job.embedding)
                ))
        conn.commit()
    finally:
        conn.close()


def _to_pgvector_literal(embedding: list[float] | None) -> str | None:
    if not embedding:
        return None
    return "[" + ",".join(str(float(x)) for x in embedding) + "]"


def _clean_str(val: str | None) -> str | None:
    if val is None:
        return None
    return val.encode("utf-8", errors="replace").decode("utf-8")


def _clean_job_utf8(job: Job):
    for field in ("title", "url", "company_name", "company_url", "company_id", "company_size",
                  "country", "salary_raw", "salary_currency", "location_raw",
                  "job_level", "description", "requirements", "education", "benefits",
                  "work_mode", "work_mode_days", "overtime_policy"):
        setattr(job, field, _clean_str(getattr(job, field)))
    for lst_field in ("skills", "job_domains", "locations", "company_industry"):
        cleaned = []
        for v in getattr(job, lst_field):
            c = _clean_str(v)
            if c is not None:
                cleaned.append(c)
        setattr(job, lst_field, cleaned)


# WORKFLOWS 

@DBOS.workflow()
def itviec_workflow():
    itv_login_step()
    pages = itv_fetch_step()
    result = itv_parse_step(pages)
    itv_save_step(result["jobs"], result["ids"])
    itv_build_schema_step()
    return len(result["jobs"])


@DBOS.workflow()
def topcv_workflow():
    pages = tcv_fetch_step()
    result = tcv_parse_step(pages)
    tcv_save_step(result["jobs"], result["ids"])
    tcv_build_schema_step()
    return len(result["jobs"])


@DBOS.workflow()
def json_loader_workflow(site: str) -> list[dict]:
    """Load schema JSON for a given site."""
    return json_loader_step(site)


@DBOS.workflow()
def embed_and_upsert_workflow(raw_jobs: list[dict]):
    """Embed and upsert jobs to DB."""
    embed_and_upsert_step(raw_jobs)


@DBOS.workflow()
def start_workflow() -> tuple[int, int]:
    itv_h = DBOS.start_workflow(itviec_workflow)
    tcv_h = DBOS.start_workflow(topcv_workflow)

    # Process ITviec immediately (without waiting for TopCV)
    itv_c = itv_h.get_result()
    print(f"ITviec: {itv_c} mới")
    itv_raw = DBOS.start_workflow(json_loader_workflow, "itviec").get_result()
    DBOS.start_workflow(embed_and_upsert_workflow, itv_raw)

    # Process TopCV after it completes
    tcv_c = tcv_h.get_result()
    print(f"TopCV: {tcv_c} mới")
    tcv_raw = DBOS.start_workflow(json_loader_workflow, "topcv").get_result()
    DBOS.start_workflow(embed_and_upsert_workflow, tcv_raw)

    return itv_c, tcv_c


@DBOS.workflow()
def daily_crawl_workflow(scheduled_time: datetime, context: Any):
    return start_workflow()


# MAIN 

def start_ui():
    db_url = f"sqlite:///{DB_PATH}"
    print(f"  Starting Argus UI -> http://localhost:8090  (DB: {db_url})", flush=True)
    env = os.environ.copy()
    env.setdefault("ARGUS_DATABASE_URL", db_url)
    return subprocess.Popen(
        [sys.executable, "-m", "dbos_argus.cli", "--db-url", db_url],
        env=env,
    )


def main():
    parser = argparse.ArgumentParser(description="Job crawler DBOS workflow")
    parser.add_argument("--trigger", "-t", action="store_true",
                        help="Chạy crawl ngay lập tức rồi thoát")
    parser.add_argument("--ui", "-u", action="store_true",
                        help="Chạy kèm web UI (Argus) tại http://localhost:8090")
    args = parser.parse_args()

    if args.ui:
        ui_proc = start_ui()

    DBOS(config={
        "name": "job-crawler",
        "system_database_url": "sqlite:///crawler_state.sqlite",
    })
    DBOS.launch()

    DBOS.apply_schedules([{
        "schedule_name": "daily-crawl",
        "workflow_fn": daily_crawl_workflow,
        "schedule": "0 0 * * *",
        "cron_timezone": "Asia/Ho_Chi_Minh",
        "context": None,
    }])

    try:
        if args.trigger:
            print(f"\n{'='*50}")
            print(f"Trigger manual crawl at {datetime.now()}")
            print(f"{'='*50}\n")
            itv_c, tcv_c = DBOS.start_workflow(start_workflow).get_result()
            print(f"\n{'='*50}")
            print(f"Done: ITviec {itv_c} | TopCV {tcv_c}")
            print(f"{'='*50}")
        else:
            print("Job crawler DBOS workflow running. Scheduled daily at 00:00 Asia/Ho_Chi_Minh", flush=True)
            print("  python crawler_workflow.py --trigger", flush=True)
            if args.ui:
                print(f"  Web UI: http://localhost:8090", flush=True)
            print("Press Ctrl+C to stop.", flush=True)
            threading.Event().wait()
    finally:
        if args.ui:
            ui_proc.terminate()
            try:
                ui_proc.wait(timeout=5)
            except Exception:
                ui_proc.kill()


if __name__ == "__main__":
    main()
