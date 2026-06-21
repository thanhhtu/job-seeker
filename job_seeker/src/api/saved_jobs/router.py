from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, Query, Response

from src.api.auth.deps import get_current_user
from src.api.errors import ErrorCode, api_error
from src.api.saved_jobs.schemas import (
    SavedJobItem,
    SavedJobStatus,
    SaveJobRequest,
    UpdateSavedJobRequest,
)
from src.db.repositories.saved_job_repository import (
    SavedJobRecord,
    delete_saved_job,
    get_saved_job,
    job_exists,
    list_saved_jobs,
    update_saved_job,
    upsert_saved_job,
)
from src.db.repositories.user_repository import UserRecord

router = APIRouter(prefix="/api/me", tags=["saved-jobs"])


def _to_item(record: SavedJobRecord) -> SavedJobItem:
    return SavedJobItem(
        job_id=record.job_id,
        status=record.status,  # type: ignore[arg-type]
        note=record.note,
        applied_at=record.applied_at,
        created_at=record.created_at,
        updated_at=record.updated_at,
        title=record.title,
        company_name=record.company_name,
        url=record.url,
        locations=record.locations,
        salary_min=record.salary_min,
        salary_max=record.salary_max,
        salary_currency=record.salary_currency,
        salary_negotiable=record.salary_negotiable,
        work_mode=record.work_mode,
        job_level=record.job_level,
        posted_date=record.posted_date,
        skills=record.skills,
        experience_years_min=record.experience_years_min,
    )


@router.get(
    "/saved-jobs",
    response_model=list[SavedJobItem],
    summary="My saved jobs",
    description="JWT required. Optionally filter by status (saved/applied/interviewing/offer/rejected).",
)
async def list_my_saved_jobs(
    status: SavedJobStatus | None = Query(default=None),
    user: UserRecord = Depends(get_current_user),
) -> list[SavedJobItem]:
    rows = await list_saved_jobs(user.id, status=status)
    return [_to_item(r) for r in rows]


@router.post(
    "/saved-jobs",
    response_model=SavedJobItem,
    status_code=201,
    summary="Save a job",
    description="JWT required. Bookmarks a job (or updates its status/note if already saved).",
)
async def save_job(
    payload: SaveJobRequest,
    user: UserRecord = Depends(get_current_user),
) -> SavedJobItem:
    if not await job_exists(payload.job_id):
        raise api_error(404, ErrorCode.JOB_NOT_FOUND)

    await upsert_saved_job(
        user.id,
        payload.job_id,
        status=payload.status,
        note=payload.note,
    )
    record = await get_saved_job(user.id, payload.job_id)
    if record is None:  # pragma: no cover - just-upserted row must exist
        raise api_error(404, ErrorCode.SAVED_JOB_NOT_FOUND)
    return _to_item(record)


@router.patch(
    "/saved-jobs/{job_id}",
    response_model=SavedJobItem,
    summary="Update a saved job",
    description="JWT required. Change the status (move through the pipeline) and/or note.",
)
async def update_my_saved_job(
    job_id: UUID,
    payload: UpdateSavedJobRequest,
    user: UserRecord = Depends(get_current_user),
) -> SavedJobItem:
    updated = await update_saved_job(
        user.id,
        job_id,
        status=payload.status,
        note=payload.note,
        note_provided="note" in payload.model_fields_set,
    )
    if not updated:
        raise api_error(404, ErrorCode.SAVED_JOB_NOT_FOUND)

    record = await get_saved_job(user.id, job_id)
    if record is None:  # pragma: no cover
        raise api_error(404, ErrorCode.SAVED_JOB_NOT_FOUND)
    return _to_item(record)


@router.delete(
    "/saved-jobs/{job_id}",
    status_code=204,
    summary="Remove a saved job",
    description="JWT required. Removes the job from the user's saved list.",
)
async def delete_my_saved_job(
    job_id: UUID,
    user: UserRecord = Depends(get_current_user),
) -> Response:
    deleted = await delete_saved_job(user.id, job_id)
    if not deleted:
        raise api_error(404, ErrorCode.SAVED_JOB_NOT_FOUND)
    return Response(status_code=204)
