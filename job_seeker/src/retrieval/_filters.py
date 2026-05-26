from __future__ import annotations

import re
import unicodedata

from src.core.logger import get_logger

logger = get_logger(__name__)

# SQL fragments
TEXT_SEARCH_BLOB_SQL = (
    "lower("
    "coalesce(title,'') || ' ' || "
    "coalesce(description,'') || ' ' || "
    "coalesce(requirements,'') || ' ' || "
    "coalesce(benefits,'')"
    ")"
)
_FTS_DOCUMENT_SQL = (
    "coalesce(title,'') || ' ' || "
    "coalesce(description,'') || ' ' || "
    "coalesce(requirements,'') || ' ' || "
    "coalesce(benefits,'') || ' ' || "
    "coalesce(company_name,'') || ' ' || "
    "array_to_string(coalesce(skills, '{}'::text[]), ' ') || ' ' || "
    "array_to_string(coalesce(job_domains, '{}'::text[]), ' ')"
)
TSVECTOR_SQL = f"to_tsvector('public.vietnamese_unaccent', {_FTS_DOCUMENT_SQL})"
JOB_SELECT_COLUMNS = (
    "id, job_id, source, url, title, "
    "company_name, company_url, company_id, company_size, company_industry, country, "
    "salary_raw, salary_min, salary_max, salary_currency, salary_negotiable, "
    "location_raw, locations, job_domains, job_level, description, requirements, skills, "
    "experience_years_min, education, benefits, work_mode, work_mode_days, "
    "overtime_policy, hiring_quantity, deadline, posted_date, crawled_date, "
    "created_at, updated_at"
)

# Work-mode patterns
_HYBRID_PATTERNS = (
    r"\bhybrid\b",
    r"\blinh\s*hoat\b",
    r"\bban\s*thoi\s*gian\b",
    r"\bpart\s*time\b",
    r"\bpart-time\b",
    r"\bflexible\b",
    r"\bket\s*hop\b",
    r"\blam\s*viec\s*linh\s*hoat\b",
    r"\bco\s*the\s*remote\b",
    r"\bmot\s*phan\b",
    r"\bnua\s*thoi\s*gian\b",
    r"\bpartly\s*remote\b",
    r"\bpartial\s*remote\b",
    r"\bpartially\s*remote\b",
    r"\bmot\s*phan\s*remote\b",
    r"\bremote\s*mot\s*phan\b",
)
_REMOTE_PATTERNS = (
    r"\bremote\b",
    r"\bwfh\b",
    r"\bwork\s*from\s*home\b",
    r"\bwork-from-home\b",
    r"\btu\s*xa\b",
    r"\bo\s*nha\b",
    r"\blam\s*o\s*nha\b",
    r"\blam\s*viec\s*o\s*nha\b",
    r"\bfull\s*remote\b",
    r"\b100%\s*remote\b",
    r"\bfully\s*remote\b",
    r"\bdistributed\b",
    r"\btelework\b",
    r"\btelecommut\w+\b",
)
_ONSITE_PATTERNS = (
    r"\bonsite\b",
    r"\bon-site\b",
    r"\bon\s*site\b",
    r"\bin\s*office\b",
    r"\bin-office\b",
    r"\btai\s*van\s*phong\b",
    r"\bvan\s*phong\b",
    r"\bfull\s*time\b",
    r"\bfull-time\b",
    r"\btoan\s*thoi\s*gian\b",
    r"\bpresential\b",
    r"\bin\s*person\b",
)

# Location aliases
_LOCATION_KEY_ALIASES: dict[str, tuple[str, ...]] = {
    "ha_noi": ("ha noi", "hanoi"),
    "tuyen_quang": ("tuyen quang", "ha giang"),
    "lao_cai": ("lao cai", "yen bai"),
    "thai_nguyen": ("thai nguyen", "bac kan"),
    "phu_tho": ("phu tho", "vinh phuc", "hoa binh"),
    "bac_ninh": ("bac ninh", "bac giang"),
    "hung_yen": ("hung yen", "thai binh"),
    "hai_phong": ("hai phong", "hai duong"),
    "ninh_binh": ("ninh binh", "nam dinh", "ha nam"),
    "quang_tri": ("quang tri", "quang binh"),
    "da_nang": ("da nang", "danang", "quang nam"),
    "quang_ngai": ("quang ngai", "kon tum", "kontum"),
    "gia_lai": ("gia lai", "binh dinh"),
    "khanh_hoa": ("khanh hoa", "ninh thuan"),
    "lam_dong": ("lam dong", "dak nong"),
    "dak_lak": ("dak lak", "phu yen"),
    "ho_chi_minh": (
        "ho chi minh",
        "tp ho chi minh",
        "tphcm",
        "tp hcm",
        "sai gon",
        "saigon",
        "binh duong",
        "ba ria vung tau",
        "brvt",
        "vung tau",
    ),
    "dong_nai": ("dong nai", "binh phuoc"),
    "tay_ninh": ("tay ninh", "long an"),
    "can_tho": ("can tho", "soc trang", "hau giang"),
    "vinh_long": ("vinh long", "ben tre", "tra vinh"),
    "dong_thap": ("dong thap", "tien giang"),
    "ca_mau": ("ca mau", "bac lieu"),
    "an_giang": ("an giang", "kien giang"),
}

# Normalizers
def clean_phrases(values: object) -> list[str]:
    """Return lowercased, trimmed, non-empty phrases from a list-like value."""
    if not isinstance(values, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for raw in values:
        text = str(raw or "").strip().lower()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _fold_ascii(text: str) -> str:
    normalized = unicodedata.normalize("NFD", text)
    return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn").lower()


def normalize_location_key(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    folded = _fold_ascii(text).replace("đ", "d")
    normalized = (
        folded.replace("/", " ")
        .replace("-", " ")
        .replace(".", " ")
        .replace(",", " ")
    )
    normalized = " ".join(part for part in normalized.split() if part)
    if not normalized:
        return ""

    padded = f" {normalized} "
    for canonical_key, aliases in _LOCATION_KEY_ALIASES.items():
        if normalized == canonical_key.replace("_", " "):
            return canonical_key
        for alias in aliases:
            if f" {alias} " in padded:
                return canonical_key

    return normalized.replace(" ", "_")


def normalize_work_mode(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""

    folded = _fold_ascii(text).replace("đ", "d")
    normalized = " ".join(
        folded.replace("-", " ").replace("_", " ").replace("/", " ").split()
    )
    if not normalized:
        return ""

    if normalized in {"hybrid", "remote", "onsite"}:
        return normalized

    if any(re.search(p, normalized) for p in _HYBRID_PATTERNS):
        return "hybrid"
    if any(re.search(p, normalized) for p in _REMOTE_PATTERNS):
        return "remote"
    if any(re.search(p, normalized) for p in _ONSITE_PATTERNS):
        return "onsite"
    return ""


# SQL condition builders
def append_location_conditions(
    *,
    parsed_query: dict,
    conditions: list[str],
    params: list,
    idx: int,
) -> int:
    raw_location = str(parsed_query.get("location") or "").strip()
    if not raw_location:
        return idx

    location_clauses: list[str] = []
    normalized_key = normalize_location_key(raw_location)
    if normalized_key:
        location_clauses.append(f"${idx} = ANY(locations)")
        params.append(normalized_key)
        idx += 1

    location_clauses.append(f"lower(coalesce(location_raw, '')) LIKE ${idx}")
    params.append(f"%{raw_location.lower()}%")
    idx += 1

    conditions.append("(" + " OR ".join(location_clauses) + ")")
    return idx


def append_skills_conditions(
    *,
    parsed_query: dict,
    conditions: list[str],
    params: list,
    idx: int,
) -> int:
    skills = clean_phrases(parsed_query.get("skills"))
    for skill in skills:
        conditions.append(
            f"EXISTS (SELECT 1 FROM unnest(skills) s WHERE lower(s) LIKE ${idx})"
        )
        params.append(f"%{skill}%")
        idx += 1
    return idx


def append_industry_conditions(
    *,
    parsed_query: dict,
    conditions: list[str],
    params: list,
    idx: int,
) -> int:
    """Filter by job/company domains. job_domains uses GIN overlap for speed."""
    job_domains = clean_phrases(parsed_query.get("job_domains"))
    if not job_domains:
        return idx

    conditions.append(
        "("
        f"job_domains && ${idx}::text[] "
        "OR EXISTS ("
        f"SELECT 1 FROM unnest(company_industry) ci WHERE lower(ci) = ANY(${idx}::text[])"
        ")"
        ")"
    )
    params.append(job_domains)
    idx += 1
    return idx


def append_keyword_match_conditions(
    *,
    parsed_query: dict,
    conditions: list[str],
    params: list,
    idx: int,
    slot: str,
    negate: bool,
) -> int:
    """Apply ILIKE / NOT ILIKE filters against the job text blob."""
    phrases = clean_phrases(parsed_query.get(slot))
    operator = "NOT LIKE" if negate else "LIKE"
    for phrase in phrases:
        conditions.append(f"{TEXT_SEARCH_BLOB_SQL} {operator} ${idx}")
        params.append(f"%{phrase}%")
        idx += 1
    return idx


def append_extra_filters(
    *,
    parsed_query: dict,
    conditions: list[str],
    params: list,
    idx: int,
) -> int:
    idx = append_industry_conditions(
        parsed_query=parsed_query, conditions=conditions, params=params, idx=idx
    )
    idx = append_keyword_match_conditions(
        parsed_query=parsed_query,
        conditions=conditions,
        params=params,
        idx=idx,
        slot="must_include_keywords",
        negate=False,
    )
    idx = append_keyword_match_conditions(
        parsed_query=parsed_query,
        conditions=conditions,
        params=params,
        idx=idx,
        slot="must_exclude_keywords",
        negate=True,
    )
    return idx


def append_salary_conditions(
    *,
    parsed_query: dict,
    conditions: list[str],
    params: list,
    idx: int,
    query_name: str,
) -> int:
    sal_min = parsed_query.get("salary_min")
    sal_max = parsed_query.get("salary_max")
    if sal_min is None and sal_max is None:
        return idx

    salary_currency = str(parsed_query.get("salary_currency") or "").strip().upper()
    if not salary_currency:
        logger.info(
            "%s: salary filter skipped because salary_currency is missing",
            query_name,
        )
        return idx

    conditions.append(f"salary_currency = ${idx}")
    params.append(salary_currency)
    idx += 1

    if sal_min is not None:
        conditions.append(f"(salary_max IS NULL OR salary_max >= ${idx})")
        params.append(float(sal_min))
        idx += 1

    if sal_max is not None:
        conditions.append(f"(salary_min IS NULL OR salary_min <= ${idx})")
        params.append(float(sal_max))
        idx += 1

    return idx


def append_experience_conditions(
    *,
    parsed_query: dict,
    conditions: list[str],
    params: list,
    idx: int,
) -> int:
    """Apply candidate- and job-side experience constraints.

    - candidate_experience_years: match jobs whose minimum requirement is <= candidate years.
    - job_experience_min/max: constrain job requirement range directly.
    """
    candidate_exp = parsed_query.get("candidate_experience_years")
    if candidate_exp is not None:
        conditions.append(f"experience_years_min <= ${idx}")
        params.append(int(candidate_exp))
        idx += 1

    if (job_exp_min := parsed_query.get("job_experience_min")) is not None:
        conditions.append(f"experience_years_min >= ${idx}")
        params.append(int(job_exp_min))
        idx += 1

    if (job_exp_max := parsed_query.get("job_experience_max")) is not None:
        conditions.append(f"experience_years_min <= ${idx}")
        params.append(int(job_exp_max))
        idx += 1

    return idx
