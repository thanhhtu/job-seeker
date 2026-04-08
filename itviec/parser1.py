"""
parser.py
---------
Đọc file jobs_raw.json, chuẩn hóa dữ liệu bằng Pydantic v2 và xuất jobs_clean.json.

Cấu trúc raw mỗi record:
  title, company, url, crawled_at,
  job_preview  (text thô: địa chỉ + work_mode + posted_time + skills + expertise + domain)
  job_detail   (text mô tả công việc)
  company_info (text thô: tên, rating, type, industry, size, country, working_days, ot_policy)

Mới (v2): bổ sung field `salary` với schema SalaryInfo.
"""

import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Pydantic Schemas
# ---------------------------------------------------------------------------

class SalaryInfo(BaseModel):
    """
    Salary đã được chuẩn hóa.

    Ví dụ đầu ra:
      {"min": 1000, "max": 2000, "currency": "USD", "raw": "1000-2000 USD"}
      {"min": 85000, "max": 85000, "currency": "USD", "raw": "$85,000"}
      {"min": 25000000, "max": 35000000, "currency": "VND", "raw": "25-35 triệu VND/month"}
    """
    min: Optional[float] = None
    max: Optional[float] = None
    currency: Optional[str] = None     # "USD", "VND", ...
    period: Optional[str] = None       # "month", "year", "negotiable", ...
    raw: Optional[str] = None          # chuỗi gốc trước khi parse


class CompanyInfo(BaseModel):
    name: str
    rating: Optional[float] = None
    company_type: Optional[str] = None
    industry: Optional[str] = None
    size: Optional[str] = None
    country: Optional[str] = None
    working_days: Optional[str] = None
    overtime_policy: Optional[str] = None


class JobClean(BaseModel):
    title: str
    company: str
    url: str
    crawled_at: Optional[datetime] = None

    # Trích xuất từ job_preview
    location: Optional[str] = None
    work_mode: Optional[str] = None      # At office / Hybrid / Remote
    posted_time: Optional[str] = None
    skills: list[str] = Field(default_factory=list)
    job_expertise: Optional[str] = None
    job_domain: list[str] = Field(default_factory=list)

    # Salary (MỚI)
    salary: Optional[SalaryInfo] = None

    # job_detail đã tách
    job_description: Optional[str] = None
    requirements: Optional[str] = None
    benefits: Optional[str] = None

    # Thông tin công ty
    company_info: Optional[CompanyInfo] = None

    # -----------------------------------------------------------------------
    # Validators
    # -----------------------------------------------------------------------
    @field_validator("title", "company", mode="before")
    @classmethod
    def strip_strings(cls, v):
        return v.strip() if isinstance(v, str) else v

    @field_validator("crawled_at", mode="before")
    @classmethod
    def parse_crawled_at(cls, v):
        if not v:
            return None
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(v, fmt)
            except ValueError:
                pass
        logger.warning("Cannot parse crawled_at: %s", v)
        return None

    @field_validator("url", mode="before")
    @classmethod
    def validate_url(cls, v):
        if not v or not str(v).startswith("http"):
            raise ValueError(f"Invalid URL: {v}")
        return str(v).strip()

    @model_validator(mode="after")
    def check_required_fields(self):
        if not self.title:
            raise ValueError("Missing title")
        if not self.company:
            raise ValueError("Missing company")
        return self


# ---------------------------------------------------------------------------
# Salary extraction helpers
# ---------------------------------------------------------------------------

# Các patterns theo độ ưu tiên cao → thấp
# Pattern 1: "$85,000"  hoặc  "USD 85,000"
_PAT_SINGLE_USD = re.compile(
    r"(?:upto?|up to|salary[:\s]*|income[:\s]*)?"
    r"\$\s*([\d,]+(?:\.\d+)?)"
    r"(?:\s*(?:USD|usd))?",
    re.IGNORECASE,
)

# Pattern 2: range "$1,000 - $2,000"  hoặc  "1000-2000 USD"
_PAT_RANGE_USD = re.compile(
    r"(?:upto?|up to)?\s*"
    r"\$?\s*([\d,]+(?:\.\d+)?)\s*[-–]\s*\$?\s*([\d,]+(?:\.\d+)?)\s*(?:USD|usd|\$)?",
    re.IGNORECASE,
)

# Pattern 3: "Up to $4,000"  / "Upto $1700"
_PAT_UPTO_USD = re.compile(
    r"up\s*to\s*\$\s*([\d,]+(?:\.\d+)?)",
    re.IGNORECASE,
)

# Pattern 4: VND million range: "25-35 triệu" or "25-35 million VND"
_PAT_RANGE_VND_MILLION = re.compile(
    r"([\d,.]+)\s*[-–]\s*([\d,.]+)\s*(?:tri[eệ]u|million|tr)\s*(?:VND|vnd|vnđ|đồng)?",
    re.IGNORECASE,
)

# Pattern 5: VND billion: "1-2 tỷ"
_PAT_RANGE_VND_BILLION = re.compile(
    r"([\d,.]+)\s*[-–]\s*([\d,.]+)\s*(?:t[ỷy]|billion)\s*(?:VND|vnd|đồng)?",
    re.IGNORECASE,
)

# Pattern 6: single VND amount in millions
_PAT_SINGLE_VND_MILLION = re.compile(
    r"([\d,.]+)\s*(?:tri[eệ]u|million|tr)\s*(?:VND|vnd|vnđ|đồng)",
    re.IGNORECASE,
)

# Period hints
_PERIOD_PAT = re.compile(r"(?:per\s+)?(month|year|annual|tháng|năm)", re.IGNORECASE)
_NEGOTIABLE_PAT = re.compile(r"thỏa thuận|negotiable|competitive|thương lượng", re.IGNORECASE)


def _clean_number(s: str) -> float:
    """Bỏ dấu phẩy và chuyển string thành float."""
    return float(s.replace(",", ""))


def _detect_period(text: str) -> Optional[str]:
    m = _PERIOD_PAT.search(text)
    if not m:
        return None
    w = m.group(1).lower()
    if w in ("month", "tháng"):
        return "month"
    if w in ("year", "annual", "năm"):
        return "year"
    return w


def _extract_salary(title: str, job_detail: str) -> Optional[SalaryInfo]:
    """
    Trích xuất thông tin lương từ title và job_detail.
    Ưu tiên: title trước (ngắn gọn, ít nhiễu), sau đó job_detail.

    Trả về SalaryInfo hoặc None nếu không tìm thấy.
    """
    sources = [("title", title or ""), ("detail", job_detail or "")]

    for source_name, text in sources:
        # --- USD: "Up to $4,000" ---
        m = _PAT_UPTO_USD.search(text)
        if m:
            val = _clean_number(m.group(1))
            period = _detect_period(text) or "month"
            return SalaryInfo(
                min=None, max=val,
                currency="USD", period=period,
                raw=m.group(0).strip(),
            )

        # --- USD range "$1,000 - $2,000" ---
        m = _PAT_RANGE_USD.search(text)
        if m:
            lo = _clean_number(m.group(1))
            hi = _clean_number(m.group(2))
            if lo > hi:
                lo, hi = hi, lo
            period = _detect_period(text) or "month"
            return SalaryInfo(
                min=lo, max=hi,
                currency="USD", period=period,
                raw=m.group(0).strip(),
            )

        # --- Single USD "$85,000" ---
        m = _PAT_SINGLE_USD.search(text)
        if m:
            val = _clean_number(m.group(1))
            period = _detect_period(text) or ("year" if val > 50000 else "month")
            return SalaryInfo(
                min=val, max=val,
                currency="USD", period=period,
                raw=m.group(0).strip(),
            )

        # --- VND billion range ---
        m = _PAT_RANGE_VND_BILLION.search(text)
        if m:
            lo = _clean_number(m.group(1)) * 1_000_000_000
            hi = _clean_number(m.group(2)) * 1_000_000_000
            if lo > hi:
                lo, hi = hi, lo
            period = _detect_period(text) or "month"
            return SalaryInfo(
                min=lo, max=hi,
                currency="VND", period=period,
                raw=m.group(0).strip(),
            )

        # --- VND million range ---
        m = _PAT_RANGE_VND_MILLION.search(text)
        if m:
            lo = _clean_number(m.group(1)) * 1_000_000
            hi = _clean_number(m.group(2)) * 1_000_000
            if lo > hi:
                lo, hi = hi, lo
            period = _detect_period(text) or "month"
            return SalaryInfo(
                min=lo, max=hi,
                currency="VND", period=period,
                raw=m.group(0).strip(),
            )

        # --- Single VND million ---
        m = _PAT_SINGLE_VND_MILLION.search(text)
        if m:
            val = _clean_number(m.group(1)) * 1_000_000
            period = _detect_period(text) or "month"
            return SalaryInfo(
                min=val, max=val,
                currency="VND", period=period,
                raw=m.group(0).strip(),
            )

    # --- Negotiable / thỏa thuận ---
    combined = f"{title} {job_detail}"
    if _NEGOTIABLE_PAT.search(combined):
        return SalaryInfo(
            min=None, max=None,
            currency=None, period="negotiable",
            raw="Negotiable",
        )

    return None


# ---------------------------------------------------------------------------
# job_preview parser
# ---------------------------------------------------------------------------
_WORK_MODES = re.compile(r"\b(At office|Hybrid|Remote)\b", re.IGNORECASE)
_SKILLS_HDR = re.compile(r"Skills?:\s*\n", re.IGNORECASE)
_EXPERT_HDR = re.compile(r"Job Expertise:\s*\n", re.IGNORECASE)
_DOMAIN_HDR = re.compile(r"Job Domain:\s*\n", re.IGNORECASE)


def _parse_job_preview(raw: str) -> dict:
    result = {
        "location": None,
        "work_mode": None,
        "posted_time": None,
        "skills": [],
        "job_expertise": None,
        "job_domain": [],
    }
    if not raw:
        return result

    lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]

    for ln in lines:
        m = _WORK_MODES.search(ln)
        if m:
            result["work_mode"] = m.group(1).title()
            break

    for i, ln in enumerate(lines):
        if ln.lower() == "posted" and i + 1 < len(lines):
            result["posted_time"] = lines[i + 1]
            break

    section_keywords = {
        "skills:", "job expertise:", "job domain:",
        "posted", "at office", "hybrid", "remote",
    }
    section_starts = [
        i for i, ln in enumerate(lines)
        if ln.lower().rstrip(":") in section_keywords or ln.lower() in section_keywords
    ]

    end_loc = section_starts[0] if section_starts else len(lines)
    loc_lines = [ln for ln in lines[:end_loc] if not _WORK_MODES.search(ln)]
    result["location"] = "; ".join(loc_lines) if loc_lines else None

    def _section_items(text: str, header_pat) -> list[str]:
        m = header_pat.search(text)
        if not m:
            return []
        rest = text[m.end():]
        stop = re.search(r"\nJob |\nSkills?:", rest, re.IGNORECASE)
        block = rest[: stop.start()] if stop else rest
        return [t.strip() for t in block.splitlines() if t.strip()]

    result["skills"] = _section_items(raw, _SKILLS_HDR)
    expertise_items = _section_items(raw, _EXPERT_HDR)
    result["job_expertise"] = expertise_items[0] if expertise_items else None
    result["job_domain"] = _section_items(raw, _DOMAIN_HDR)
    return result


# ---------------------------------------------------------------------------
# job_detail splitter
# ---------------------------------------------------------------------------

def _split_job_detail(raw: str) -> dict:
    if not raw:
        return {"job_description": None, "requirements": None, "benefits": None}

    text = re.sub(r"\n{3,}", "\n\n", raw.strip())

    job_desc_pat = re.compile(r"Job description", re.IGNORECASE)
    req_pat      = re.compile(r"Your skills and experience", re.IGNORECASE)
    ben_pat      = re.compile(r"Why you(?:'ll|'ll) love working here", re.IGNORECASE)

    m_desc = job_desc_pat.search(text)
    m_req  = req_pat.search(text)
    m_ben  = ben_pat.search(text)

    description = requirements = benefits = None

    if m_desc and m_req:
        description = text[m_desc.end(): m_req.start()].strip()
    elif m_desc and m_ben:
        description = text[m_desc.end(): m_ben.start()].strip()
    elif m_desc:
        description = text[m_desc.end():].strip()
    else:
        description = text.strip()

    if m_req:
        end_req = m_ben.start() if m_ben else len(text)
        requirements = text[m_req.end(): end_req].strip()

    if m_ben:
        benefits = text[m_ben.end():].strip()

    if not description:
        top3 = re.compile(r"Top 3 reasons to join us", re.IGNORECASE)
        m_top = top3.search(text)
        if m_top and m_req:
            description = text[m_top.end(): m_req.start()].strip()

    return {
        "job_description": description or None,
        "requirements": requirements or None,
        "benefits": benefits or None,
    }


# ---------------------------------------------------------------------------
# company_info parser
# ---------------------------------------------------------------------------
_RATING_PAT   = re.compile(r"(\d+\.\d+|\d+)")
_TYPE_PAT     = re.compile(r"Company type\s*\n(.+)", re.IGNORECASE)
_INDUSTRY_PAT = re.compile(r"Company industry\s*\n(.+)", re.IGNORECASE)
_SIZE_PAT     = re.compile(r"Company size\s*\n(.+?)(?:\n|employees)", re.IGNORECASE)
_COUNTRY_PAT  = re.compile(r"Country\s*\n(.+)", re.IGNORECASE)
_WDAYS_PAT    = re.compile(r"Working days\s*\n(.+)", re.IGNORECASE)
_OT_PAT       = re.compile(r"Overtime policy\s*\n(.+)", re.IGNORECASE)


def _parse_company_info(raw: str, company_name: str) -> CompanyInfo:
    lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
    rating = None
    for ln in lines[:4]:
        m = _RATING_PAT.match(ln)
        if m and 1.0 <= float(m.group()) <= 5.0:
            try:
                rating = float(m.group())
            except ValueError:
                pass
            break

    def _find(pattern, text) -> Optional[str]:
        m = pattern.search(text)
        return m.group(1).strip() if m else None

    size_raw = _find(_SIZE_PAT, raw)
    if size_raw:
        size = re.split(r"\s+", size_raw)[0]
    else:
        size = None
        for ln in lines:
            if re.match(r"^\d+[\-+]?\d*$", ln):
                size = ln
                break

    return CompanyInfo(
        name=company_name,
        rating=rating,
        company_type=_find(_TYPE_PAT, raw),
        industry=_find(_INDUSTRY_PAT, raw),
        size=size,
        country=_find(_COUNTRY_PAT, raw),
        working_days=_find(_WDAYS_PAT, raw),
        overtime_policy=_find(_OT_PAT, raw),
    )


# ---------------------------------------------------------------------------
# Main processing function
# ---------------------------------------------------------------------------

def process_jobs(
    input_path: str = "jobs_raw.json",
    output_path: str = "jobs_clean.json",
) -> None:
    """
    Đọc jobs_raw.json → validate bằng Pydantic → xuất jobs_clean.json.

    Thay đổi so với v1: thêm field `salary` (SalaryInfo).
    """
    input_file  = Path(input_path)
    output_file = Path(output_path)

    if not input_file.exists():
        logger.error("File not found: %s", input_file)
        raise FileNotFoundError(input_file)

    logger.info("Reading data from %s ...", input_file)
    with input_file.open(encoding="utf-8") as f:
        raw_data = json.load(f)

    if not isinstance(raw_data, list):
        raise ValueError("JSON file must contain a list of job objects.")

    total, cleaned, skipped = len(raw_data), [], 0

    for idx, record in enumerate(raw_data, start=1):
        try:
            title      = record.get("title", "")
            job_detail = record.get("job_detail", "")

            preview_fields = _parse_job_preview(record.get("job_preview", ""))
            detail_fields  = _split_job_detail(job_detail)
            company_info   = _parse_company_info(
                record.get("company_info", ""),
                record.get("company", ""),
            )
            salary = _extract_salary(title, job_detail)

            job = JobClean(
                title       = title,
                company     = record.get("company", ""),
                url         = record.get("url", ""),
                crawled_at  = record.get("crawled_at"),
                location    = preview_fields["location"],
                work_mode   = preview_fields["work_mode"],
                posted_time = preview_fields["posted_time"],
                skills      = preview_fields["skills"],
                job_expertise  = preview_fields["job_expertise"],
                job_domain     = preview_fields["job_domain"],
                salary         = salary,
                job_description = detail_fields["job_description"],
                requirements    = detail_fields["requirements"],
                benefits        = detail_fields["benefits"],
                company_info    = company_info,
            )

            job_dict = job.model_dump(mode="json")
            if job.crawled_at:
                job_dict["crawled_at"] = job.crawled_at.isoformat()

            cleaned.append(job_dict)

        except Exception as exc:
            skipped += 1
            logger.warning(
                "[Record %d/%d] Skipped due to error: %s | title=%s",
                idx, total, exc, record.get("title", "(unknown)"),
            )

    logger.info(
        "Result: %d/%d valid records, %d skipped.",
        len(cleaned), total, skipped,
    )

    with output_file.open("w", encoding="utf-8") as f:
        json.dump(cleaned, f, ensure_ascii=False, indent=2)

    logger.info("Exported %d records → %s", len(cleaned), output_file)

    # Thống kê salary
    with_salary = sum(1 for r in cleaned if r.get("salary") is not None)
    logger.info(
        "Salary stats: %d/%d records có thông tin lương (%.1f%%)",
        with_salary, len(cleaned),
        100 * with_salary / len(cleaned) if cleaned else 0,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(
        description="Chuẩn hóa dữ liệu jobs_raw.json → jobs_clean.json (v2, có salary)"
    )
    ap.add_argument("--input",  default="jobs_raw.json",   help="File JSON thô đầu vào")
    ap.add_argument("--output", default="jobs_clean.json", help="File JSON sạch đầu ra")
    args = ap.parse_args()

    process_jobs(input_path=args.input, output_path=args.output)
