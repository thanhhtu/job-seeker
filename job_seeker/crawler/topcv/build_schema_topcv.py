from pathlib import Path

from db import load_json, save_json

from .schema_topcv import JobSchema


def build_schema(input_path: Path) -> list[dict]:
    raw_jobs = load_json(input_path)
    if not isinstance(raw_jobs, list):
        raw_jobs = []

    result = []
    for data in raw_jobs:
        try:
            job = JobSchema(**data)
            result.append(job.model_dump(mode="json"))
        except Exception as e:
            print(f"  Schema error job {data.get('job_id', 'N/A')}: {e}")
    return result


def build_schema_file(input_path: Path) -> Path:
    output_path = input_path.with_name("topcv_jobs_schema.json")
    data = build_schema(input_path)
    save_json(output_path, data)
    print(f"  Schema saved: {output_path} ({len(data)} jobs)")
    return output_path
