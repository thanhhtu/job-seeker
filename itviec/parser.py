"""
parser.py
---------
Đọc file jobs_raw.json, chuẩn hóa dữ liệu bằng Pydantic v2 và xuất jobs_clean.json.

Cấu trúc raw mỗi record:
  title, url, crawled_at,
  job_preview  (text thô: địa chỉ + work_mode + posted_time + skills + expertise + domain)
  job_detail   (text mô tả công việc)
  company_info (text thô: tên, rating, type, industry, size, country, working_days, ot_policy)

Thay đổi so với v2:
  - Không còn field `company` trong JSON đầu vào; tên công ty được trích từ company_info.
  - Cải thiện logic extract salary: tránh nhầm với funding/revenue/năm/metrics.
"""

import json
import logging
import re
from datetime import datetime, timedelta
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
    posted_time: Optional[datetime] = None
    skills: list[str] = Field(default_factory=list)
    job_expertise: Optional[str] = None
    job_domain: list[str] = Field(default_factory=list)

    # Salary
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
# Company name extraction from company_info text
# ---------------------------------------------------------------------------

# Label section đã biết trong company_info — dòng ngay sau label này là VALUE, không phải tên công ty
_SECTION_LABEL_PAT = re.compile(
    r"^(?:"
    r"Company\s+type"
    r"|Company\s+industry"
    r"|Company\s+size"
    r"|Working\s+days"
    r"|Overtime\s+policy"
    r"|Country"
    r"|About\s+us"
    r"|Overview"
    r")$",
    re.IGNORECASE,
)

# Dòng trông như một số liệu (rating, size, %)
_LOOKS_LIKE_NUMBER = re.compile(r"^[\d.,\s%+\-]+$")


def _extract_company_name(company_info_raw: str) -> str:
    """
    Trích tên công ty từ text thô của company_info.

    Chiến lược:
    - Bỏ qua các dòng rỗng, số thuần, dòng quá ngắn.
    - Đánh dấu các dòng là VALUE của section label (Company type → dòng sau là giá trị,
      không phải tên công ty).
    - Dòng text đầu tiên còn lại được coi là tên công ty.

    Trả về chuỗi rỗng nếu không tìm được.
    """
    if not company_info_raw:
        return ""

    lines = [ln.strip() for ln in company_info_raw.splitlines()]
    skip_next = False   # True khi dòng hiện tại là VALUE ngay sau một section label

    for line in lines:
        if not line:
            continue

        # Dòng này là value của label trước → bỏ qua, reset flag
        if skip_next:
            skip_next = False
            continue

        # Nếu dòng này là một section label → dòng kế là value, không phải tên cty
        if _SECTION_LABEL_PAT.match(line):
            skip_next = True
            continue

        # Bỏ qua số thuần (rating, size)
        if _LOOKS_LIKE_NUMBER.match(line):
            continue

        # Bỏ qua dòng quá ngắn (artifact)
        if len(line) < 3:
            continue

        return line

    return ""


# ---------------------------------------------------------------------------
# Salary extraction helpers
# ---------------------------------------------------------------------------

# ---- Salary-positive context -----------------------------------------------
# Các từ/cụm từ chỉ ra rằng số đi kèm là thông tin lương
_SALARY_CONTEXT_PAT = re.compile(
    r"\b(?:"
    r"salary|salari"
    r"|income"
    r"|compensation|compen"
    r"|pay\b|paid"
    r"|wage"
    r"|remuneration"
    r"|package"
    r"|earn(?:ing)?"
    r"|lương|thu\s*nhập|mức\s*lương|mức\s*thu\s*nhập|thù\s*lao"
    r")\b",
    re.IGNORECASE,
)

# ---- Salary-negative context -----------------------------------------------
# Các pattern gợi ý con số KHÔNG phải lương (funding, revenue, năm, metrics…)
_NON_SALARY_CONTEXT_PAT = re.compile(
    r"\b(?:"
    r"revenue|funding|valuation|raised|investment|budget|profit|loss"
    r"|ARR|MRR|GMV|AUM|ACV|run\s*rate"
    r"|series\s*[A-Z]|round"
    r"|market\s*cap|capitali[sz]ation"
    r"|transaction|volume|turnover"
    r"|in\s+(?:19|20)\d{2}"   # "in 2024", "in 2023"
    r"|(?:19|20)\d{2}\s+(?:revenue|funding|profit)"
    r"|billion\s+(?:company|startup|unicorn)"
    r")\b",
    re.IGNORECASE,
)

# Phát hiện "$100M", "$2B", "$500K" — thường là funding/revenue, không phải lương
_LARGE_ABBREV_PAT = re.compile(
    r"\$\s*[\d,.]+\s*[KMBkmb]\b",
    re.IGNORECASE,
)

# Phát hiện năm đứng cạnh số tiền, ví dụ "$100M in 2024" hoặc "2024: $5M"
_YEAR_NEAR_AMOUNT_PAT = re.compile(
    r"(?:(?:19|20)\d{2}.*?\$|\\$.*?(?:19|20)\d{2})",
    re.IGNORECASE,
)

# ---- Period hints ----------------------------------------------------------
_PERIOD_PAT = re.compile(r"(?:per\s+)?(month|year|annual|tháng|năm)", re.IGNORECASE)
_NEGOTIABLE_PAT = re.compile(
    r"\b(?:thỏa\s*thuận|negotiable|competitive\s*(?:salary|package)?|thương\s*lượng)\b",
    re.IGNORECASE,
)

# ---- Salary patterns (theo thứ tự ưu tiên) ---------------------------------

# P1: "Up to $4,000" / "Upto $1700"
_PAT_UPTO_USD = re.compile(
    r"\bup\s*to\s*\$\s*([\d,]+(?:\.\d+)?)\b",
    re.IGNORECASE,
)

# P2: USD range "$1,000 - $2,000" / "1000-2000 USD" / "$1,000 – $2,000"
_PAT_RANGE_USD = re.compile(
    r"\$\s*([\d,]+(?:\.\d+)?)\s*[-–]\s*\$?\s*([\d,]+(?:\.\d+)?)\s*(?:USD|usd)?"
    r"|\b([\d,]+(?:\.\d+)?)\s*[-–]\s*([\d,]+(?:\.\d+)?)\s*(?:USD|usd)\b",
    re.IGNORECASE,
)

# P3: single "$85,000" — chỉ khi có salary context rõ ràng (xử lý riêng bên dưới)
_PAT_SINGLE_USD = re.compile(
    r"\$\s*([\d,]+(?:\.\d+)?)\b",
    re.IGNORECASE,
)

# P4: VND billion range "1-2 tỷ"
_PAT_RANGE_VND_BILLION = re.compile(
    r"([\d,.]+)\s*[-–]\s*([\d,.]+)\s*(?:t[ỷy]|billion)\s*(?:VND|vnd|đồng)?",
    re.IGNORECASE,
)

# P5: VND million range "25-35 triệu" / "25-35 million VND"
_PAT_RANGE_VND_MILLION = re.compile(
    r"([\d,.]+)\s*[-–]\s*([\d,.]+)\s*(?:tri[eệ]u|million|tr)\s*(?:VND|vnd|vnđ|đồng)?",
    re.IGNORECASE,
)

# P6: single VND million "10 triệu VND"
_PAT_SINGLE_VND_MILLION = re.compile(
    r"([\d,.]+)\s*(?:tri[eệ]u|million|tr)\s*(?:VND|vnd|vnđ|đồng)",
    re.IGNORECASE,
)


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


# ---------------------------------------------------------------------------
# Context window helpers
# ---------------------------------------------------------------------------

def _window(text: str, match: re.Match, radius: int = 120) -> str:
    """Trả về đoạn text xung quanh match (±radius ký tự) để kiểm tra context."""
    start = max(0, match.start() - radius)
    end   = min(len(text), match.end() + radius)
    return text[start:end]


def _has_salary_context(ctx: str) -> bool:
    """True nếu context xung quanh match có từ khoá chỉ lương."""
    return bool(_SALARY_CONTEXT_PAT.search(ctx))


def _has_non_salary_context(ctx: str) -> bool:
    """
    True nếu context xung quanh match có dấu hiệu KHÔNG phải lương.
    Ưu tiên loại trừ: funding, revenue, năm, abbrev lớn (M/B/K).
    """
    if _NON_SALARY_CONTEXT_PAT.search(ctx):
        return True
    if _LARGE_ABBREV_PAT.search(ctx):
        return True
    if _YEAR_NEAR_AMOUNT_PAT.search(ctx):
        return True
    return False


def _is_plausible_salary_usd(value: float, period: Optional[str]) -> bool:
    """
    Kiểm tra giá trị USD có nằm trong dải lương hợp lý không.

    - Monthly: 200 – 50,000 USD/month  (loại trừ số cực lớn như $1M)
    - Yearly : 1,000 – 1,000,000 USD/year
    - Unknown: chấp nhận nếu ≤ 500,000
    """
    if period == "month":
        return 200 <= value <= 50_000
    if period in ("year", "annual"):
        return 1_000 <= value <= 1_000_000
    # period chưa xác định: loại trừ số quá lớn (thường là revenue/funding)
    return value <= 500_000


def _is_plausible_salary_vnd(value: float) -> bool:
    """
    VND: lương hợp lý 1 triệu – 500 triệu / tháng.
    Số > 500M thường là revenue hoặc contract value.
    """
    return 1_000_000 <= value <= 500_000_000


# ---------------------------------------------------------------------------
# Core salary extractor
# ---------------------------------------------------------------------------

def _try_extract_from_text(text: str) -> Optional[SalaryInfo]:
    """
    Thử trích salary từ một đoạn text.
    Trả về SalaryInfo nếu tìm được, None nếu không.

    Chiến lược:
    1. Quét các pattern theo độ ưu tiên.
    2. Với mỗi match, kiểm tra context window:
       - Nếu có non-salary signal → bỏ qua.
       - Nếu pattern ít ambiguous (range, up-to) → chấp nhận ngay.
       - Nếu pattern ambiguous (single number) → yêu cầu có salary context rõ ràng.
    3. Kiểm tra plausibility (dải giá trị hợp lý).
    """

    # --- P1: Up to $X ---
    for m in _PAT_UPTO_USD.finditer(text):
        ctx = _window(text, m)
        if _has_non_salary_context(ctx):
            continue
        val = _clean_number(m.group(1))
        period = _detect_period(ctx) or "month"
        if not _is_plausible_salary_usd(val, period):
            continue
        return SalaryInfo(min=None, max=val, currency="USD", period=period, raw=m.group(0).strip())

    # --- P2: USD range ---
    for m in _PAT_RANGE_USD.finditer(text):
        ctx = _window(text, m)
        if _has_non_salary_context(ctx):
            continue
        # group(1,2) cho dạng "$X - $Y", group(3,4) cho dạng "X - Y USD"
        if m.group(1) and m.group(2):
            lo, hi = _clean_number(m.group(1)), _clean_number(m.group(2))
        elif m.group(3) and m.group(4):
            lo, hi = _clean_number(m.group(3)), _clean_number(m.group(4))
        else:
            continue
        if lo > hi:
            lo, hi = hi, lo
        period = _detect_period(ctx) or "month"
        if not (_is_plausible_salary_usd(lo, period) and _is_plausible_salary_usd(hi, period)):
            continue
        return SalaryInfo(min=lo, max=hi, currency="USD", period=period, raw=m.group(0).strip())

    # --- P3: Single USD — chỉ lấy khi có salary context rõ ràng ---
    for m in _PAT_SINGLE_USD.finditer(text):
        ctx = _window(text, m)
        if _has_non_salary_context(ctx):
            continue
        if not _has_salary_context(ctx):
            # Single USD số không có từ khoá lương → quá ambiguous, bỏ qua
            continue
        val = _clean_number(m.group(1))
        period = _detect_period(ctx)
        if period is None:
            period = "year" if val > 50_000 else "month"
        if not _is_plausible_salary_usd(val, period):
            continue
        return SalaryInfo(min=val, max=val, currency="USD", period=period, raw=m.group(0).strip())

    # --- P4: VND billion range ---
    for m in _PAT_RANGE_VND_BILLION.finditer(text):
        ctx = _window(text, m)
        if _has_non_salary_context(ctx):
            continue
        lo = _clean_number(m.group(1)) * 1_000_000_000
        hi = _clean_number(m.group(2)) * 1_000_000_000
        if lo > hi:
            lo, hi = hi, lo
        if not (_is_plausible_salary_vnd(lo) and _is_plausible_salary_vnd(hi)):
            continue
        period = _detect_period(ctx) or "month"
        return SalaryInfo(min=lo, max=hi, currency="VND", period=period, raw=m.group(0).strip())

    # --- P5: VND million range ---
    for m in _PAT_RANGE_VND_MILLION.finditer(text):
        ctx = _window(text, m)
        if _has_non_salary_context(ctx):
            continue
        lo = _clean_number(m.group(1)) * 1_000_000
        hi = _clean_number(m.group(2)) * 1_000_000
        if lo > hi:
            lo, hi = hi, lo
        if not (_is_plausible_salary_vnd(lo) and _is_plausible_salary_vnd(hi)):
            continue
        period = _detect_period(ctx) or "month"
        return SalaryInfo(min=lo, max=hi, currency="VND", period=period, raw=m.group(0).strip())

    # --- P6: Single VND million — chỉ lấy khi có salary context ---
    for m in _PAT_SINGLE_VND_MILLION.finditer(text):
        ctx = _window(text, m)
        if _has_non_salary_context(ctx):
            continue
        if not _has_salary_context(ctx):
            continue
        val = _clean_number(m.group(1)) * 1_000_000
        if not _is_plausible_salary_vnd(val):
            continue
        period = _detect_period(ctx) or "month"
        return SalaryInfo(min=val, max=val, currency="VND", period=period, raw=m.group(0).strip())

    return None


def _extract_salary(title: str, job_detail: str) -> Optional[SalaryInfo]:
    """
    Trích xuất thông tin lương từ title và job_detail.
    Ưu tiên: title trước (ngắn gọn, ít nhiễu), sau đó job_detail.

    Xử lý đặc biệt:
    - "Negotiable / thỏa thuận" → SalaryInfo(period="negotiable") nếu không tìm được số cụ thể.
    - Tránh nhầm với funding/revenue/năm/metrics bằng context window.
    """
    for text in (title or "", job_detail or ""):
        result = _try_extract_from_text(text)
        if result:
            return result

    # Fallback: negotiable
    combined = f"{title} {job_detail}"
    if _NEGOTIABLE_PAT.search(combined):
        return SalaryInfo(min=None, max=None, currency=None, period="negotiable", raw="Negotiable")

    return None


# ---------------------------------------------------------------------------
# posted_time computation
# ---------------------------------------------------------------------------

# Pattern: "X unit ago" (EN) hoặc "X đơn_vị trước" (VI)
# Hỗ trợ:
#   EN: minute(s), hour(s), day(s), week(s), month(s)
#   VI: phút, giờ, ngày, tuần, tháng
_POSTED_AGO_PAT = re.compile(
    r"^\s*(\d+)\s+"
    r"(?:"
    r"minutes?\s+ago"
    r"|hours?\s+ago"
    r"|days?\s+ago"
    r"|weeks?\s+ago"
    r"|months?\s+ago"
    r"|ph[uú]t\s+tr[uưước]c"
    r"|gi[oờ]\s+tr[uưước]c"
    r"|ng[aày]\s+tr[uưước]c"
    r"|tu[aầ]n\s+tr[uưước]c"
    r"|th[aá]ng\s+tr[uưước]c"
    r")\s*$",
    re.IGNORECASE,
)

# Map từ khoá đơn vị → số giây
_UNIT_TO_SECONDS: dict[str, int] = {
    "minute":  60,
    "minutes": 60,
    "hour":    3600,
    "hours":   3600,
    "day":     86400,
    "days":    86400,
    "week":    604800,
    "weeks":   604800,
    "month":   2592000,   # 30 ngày
    "months":  2592000,
    # Vietnamese (ASCII-folded từ pattern)
    "phut":    60,
    "gio":     3600,
    "ngay":    86400,
    "tuan":    604800,
    "thang":   2592000,
}


def _vi_ascii_fold(s: str) -> str:
    """Chuyển ký tự có dấu tiếng Việt thường gặp về ASCII để tra bảng."""
    replacements = {
        "ú": "u", "ù": "u", "ư": "u", "ứ": "u", "ừ": "u",
        "ờ": "o", "ở": "o", "ỡ": "o", "ộ": "o", "ọ": "o",
        "ò": "o", "ó": "o", "ô": "o", "ố": "o", "ổ": "o",
        "à": "a", "á": "a", "ầ": "a", "ả": "a", "ắ": "a", "ặ": "a",
        "ế": "e", "ề": "e", "ể": "e", "ệ": "e",
        "đ": "d",
        "ướ": "uoc",
    }
    result = s.lower()
    for vi, asc in replacements.items():
        result = result.replace(vi, asc)
    return result


def _compute_posted_time(
    raw_text: str,
    crawled_at: Optional[datetime],
) -> Optional[datetime]:
    """
    Tính thời gian đăng bài từ chuỗi tương đối + crawled_at.

    ``raw_text`` là chuỗi ngay sau label "Posted" trong job_preview, ví dụ:
      - "11 hours ago"    → crawled_at - 11 giờ
      - "2 days ago"      → crawled_at - 2 ngày
      - "30 phút trước"  → crawled_at - 30 phút
      - "3 ngày trước"   → crawled_at - 3 ngày

    Trả về datetime (ISO-serializable) hoặc None nếu không parse được.
    """
    if not raw_text or not crawled_at:
        return None

    text = raw_text.strip()
    m = _POSTED_AGO_PAT.match(text)
    if not m:
        return None

    quantity = int(m.group(1))

    # Tách phần đơn vị: bỏ số đầu và bỏ "ago"/"trước"
    unit_part = text
    unit_part = re.sub(r"^\d+\s*", "", unit_part)
    unit_part = re.sub(r"\s*(ago|tr[uưước][cớ]?)\s*$", "", unit_part, flags=re.IGNORECASE)
    unit_key  = _vi_ascii_fold(unit_part.strip())

    seconds = _UNIT_TO_SECONDS.get(unit_key)

    # Prefix/suffix fallback
    if seconds is None:
        for k, v in _UNIT_TO_SECONDS.items():
            if unit_key.startswith(k) or k.startswith(unit_key):
                seconds = v
                break

    if seconds is None:
        logger.debug("Cannot map posted_time unit: %r from %r", unit_key, raw_text)
        return None

    return crawled_at - timedelta(seconds=seconds * quantity)


# ---------------------------------------------------------------------------
# job_preview parser
# ---------------------------------------------------------------------------
_WORK_MODES = re.compile(r"\b(At office|Hybrid|Remote)\b", re.IGNORECASE)
_SKILLS_HDR = re.compile(r"Skills?:\s*\n", re.IGNORECASE)
_EXPERT_HDR = re.compile(r"Job Expertise:\s*\n", re.IGNORECASE)
_DOMAIN_HDR = re.compile(r"Job Domain:\s*\n", re.IGNORECASE)


def _parse_job_preview(raw: str, crawled_at: Optional[datetime] = None) -> dict:
    """
    Parse job_preview text thô.

    ``crawled_at`` được dùng để tính ``posted_time`` thực từ chuỗi
    tương đối ("11 hours ago", "2 ngày trước"…).
    Nếu không parse được → posted_time = None.
    """
    result = {
        "location": None,
        "work_mode": None,
        "posted_time": None,   # Optional[datetime]
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

    # Tìm chuỗi tương đối ngay sau từ "Posted"
    for i, ln in enumerate(lines):
        if ln.lower() == "posted" and i + 1 < len(lines):
            raw_posted = lines[i + 1]
            result["posted_time"] = _compute_posted_time(raw_posted, crawled_at)
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


def _parse_company_info(raw: str) -> CompanyInfo:
    """
    Parse company_info text thô thành CompanyInfo.

    Thay đổi: không nhận company_name từ bên ngoài nữa.
    Tên công ty được trích xuất trực tiếp từ text bằng _extract_company_name().
    """
    company_name = _extract_company_name(raw)

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

    Thay đổi so với v2:
    - Field `company` được trích từ company_info (không còn trong JSON đầu vào).
    - Salary extraction thông minh hơn: loại bỏ false-positive funding/revenue/năm.
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
            title            = record.get("title", "")
            job_detail       = record.get("job_detail", "")
            company_info_raw = record.get("company_info", "")

            # Trích tên công ty từ company_info (không còn field "company" trong JSON)
            company_name = _extract_company_name(company_info_raw)
            if not company_name:
                logger.warning(
                    "[Record %d/%d] Cannot extract company name from company_info | title=%s",
                    idx, total, title,
                )

            # Parse crawled_at trước để truyền vào _parse_job_preview
            crawled_at_raw = record.get("crawled_at")
            crawled_at_dt: Optional[datetime] = None
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
                try:
                    crawled_at_dt = datetime.strptime(crawled_at_raw, fmt)
                    break
                except (ValueError, TypeError):
                    pass

            preview_fields = _parse_job_preview(
                record.get("job_preview", ""),
                crawled_at=crawled_at_dt,
            )
            detail_fields  = _split_job_detail(job_detail)
            company_info   = _parse_company_info(company_info_raw)
            salary         = _extract_salary(title, job_detail)

            job = JobClean(
                title        = title,
                company      = company_name,
                url          = record.get("url", ""),
                crawled_at   = record.get("crawled_at"),
                location     = preview_fields["location"],
                work_mode    = preview_fields["work_mode"],
                posted_time  = preview_fields["posted_time"],
                skills       = preview_fields["skills"],
                job_expertise   = preview_fields["job_expertise"],
                job_domain      = preview_fields["job_domain"],
                salary          = salary,
                job_description = detail_fields["job_description"],
                requirements    = detail_fields["requirements"],
                benefits        = detail_fields["benefits"],
                company_info    = company_info,
            )

            job_dict = job.model_dump(mode="json")
            # Serialize datetime fields → ISO string
            if job.crawled_at:
                job_dict["crawled_at"] = job.crawled_at.isoformat()
            if job.posted_time:
                job_dict["posted_time"] = job.posted_time.isoformat()

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

    # Thống kê posted_time
    with_posted = sum(1 for r in cleaned if r.get("posted_time") is not None)
    logger.info(
        "Posted-time stats: %d/%d records có posted_time (%.1f%%)",
        with_posted, len(cleaned),
        100 * with_posted / len(cleaned) if cleaned else 0,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(
        description="Chuẩn hóa dữ liệu jobs_raw.json → jobs_clean.json (v3)"
    )
    ap.add_argument("--input",  default="jobs_raw.json",   help="File JSON thô đầu vào")
    ap.add_argument("--output", default="jobs_clean.json", help="File JSON sạch đầu ra")
    args = ap.parse_args()

    process_jobs(input_path=args.input, output_path=args.output)
