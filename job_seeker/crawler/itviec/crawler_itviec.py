#!/usr/bin/env python3
"""
crawler_itviec.py
======================
Pure Python ITviec crawler - bypass Cloudflare with curl_cffi (impersonate Chrome).
No browser and no Playwright/Puppeteer required.

Install:
    pip install curl_cffi beautifulsoup4 lxml

Run:
    python crawler_itviec.py
"""

import json
import re
import time
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from curl_cffi import requests as cf
from bs4 import BeautifulSoup

# ─── Config ───────────────────────────────────────────────────────────────────
EMAIL       = "nguyenminhnhat474@gmail.com"
PASSWORD    = "CuM@ng1971982"
MAX_PAGES   = 2
WAIT_MIN    = 1.5   # seconds, random delay between requests
WAIT_MAX    = 3.5
OUTPUT_DIR  = Path("data_job")
OUTPUT_FILE = OUTPUT_DIR / "itviec_jobs.json"
LOG_DIR     = Path("itviec/logs")

SITE_URL    = "https://itviec.com"
BASE_URL    = "https://itviec.com/it-jobs"
LOGIN_URL   = "https://itviec.com/sign_in"

# curl_cffi impersonate - mimic Chrome TLS/HTTP2 fingerprint
IMPERSONATE = "chrome124"

# ─── Session (keep cookies across requests) ───────────────────────────────────
session = cf.Session(impersonate=IMPERSONATE)
session.headers.update({
    "Accept":           "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language":  "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding":  "gzip, deflate, br",
    "Cache-Control":    "no-cache",
    "Pragma":           "no-cache",
})


# ─── Helpers ──────────────────────────────────────────────────────────────────

def wait():
    time.sleep(random.uniform(WAIT_MIN, WAIT_MAX))


def save_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def load_json(path: Path) -> list:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []


def extract_job_id(url: str) -> Optional[str]:
    if not url:
        return None
    clean = url.split("?")[0].split("#")[0].rstrip("/")
    slug  = clean.split("/")[-1]
    m     = re.search(r"-([A-Za-z]?\d+)$", slug)
    return f"{m.group(1)}" if m else f"{slug}"


def parse_relative_time(text: str) -> Optional[datetime]:
    now  = datetime.now()
    text = (text or "").lower()
    if "just now" in text or "today" in text or "hôm nay" in text:
        return now
    patterns = [
        (r"(\d+)\s+giây",   timedelta(seconds=1)),
        (r"(\d+)\s+phút",   timedelta(minutes=1)),
        (r"(\d+)\s+giờ",    timedelta(hours=1)),
        (r"(\d+)\s+ngày",   timedelta(days=1)),
        (r"(\d+)\s+tuần",   timedelta(weeks=1)),
        (r"(\d+)\s+tháng",  timedelta(days=30)),
        (r"(\d+)\s+năm",    timedelta(days=365)),
        (r"(\d+)\s+hour",   timedelta(hours=1)),
        (r"(\d+)\s+day",    timedelta(days=1)),
        (r"(\d+)\s+week",   timedelta(weeks=1)),
        (r"(\d+)\s+month",  timedelta(days=30)),
    ]
    for pattern, delta in patterns:
        m = re.search(pattern, text)
        if m:
            return now - int(m.group(1)) * delta
    return None


def compute_posted_date(posted_at_text: str, crawled_dt: datetime) -> Optional[str]:
    """Compute posted datetime using the same rules as test_itviec.py."""
    if not crawled_dt:
        return None
    text = (posted_at_text or "").lower()
    if "just now" in text or "today" in text:
        return crawled_dt.strftime("%Y-%m-%d %H:%M:%S")

    patterns = [
        (re.compile(r"(\d+)\s+minute", re.I), "minutes", 0),
        (re.compile(r"(\d+)\s+minutes", re.I), "minutes", 0),
        (re.compile(r"(\d+)\s+hour", re.I), "hours", 0),
        (re.compile(r"(\d+)\s+day", re.I), "days", 1),
        (re.compile(r"(\d+)\s+week", re.I), "weeks", 7),
        (re.compile(r"(\d+)\s+month", re.I), "months", 30),
    ]
    for pattern, unit, days_per_unit in patterns:
        m = pattern.search(text)
        if m:
            n = int(m.group(1))
            if unit == "minutes":
                delta = timedelta(minutes=n)
            elif unit == "hours":
                delta = timedelta(hours=n)
            else:
                delta = timedelta(days=n * days_per_unit)
            target_dt = crawled_dt - delta
            return target_dt.strftime("%Y-%m-%d %H:%M:%S")
    return None


def get_page(url: str, referer: str = BASE_URL) -> Optional[BeautifulSoup]:
    """Send a GET request and return BeautifulSoup, or None on failure."""
    try:
        session.headers.update({"Referer": referer})
        r = session.get(url, timeout=30)
        print(f"  GET {url} → {r.status_code}")
        if r.status_code != 200:
            print(f"  WARN: Status {r.status_code}")
            return None
        return BeautifulSoup(r.text, "lxml")
    except Exception as e:
        print(f"  ERROR: {e}")
        return None


# ─── Login ────────────────────────────────────────────────────────────────────

def get_csrf_token(soup: BeautifulSoup) -> Optional[str]:
    """Extract CSRF token from the login form."""
    # Try meta tag first
    meta = soup.find("meta", {"name": "csrf-token"})
    if meta and meta.get("content"):
        return meta["content"]
    # Try hidden input
    hidden = soup.find("input", {"name": "authenticity_token"})
    if hidden and hidden.get("value"):
        return hidden["value"]
    return None


def login(email: str, password: str) -> bool:
    """Login to ITviec and store cookies in the shared session."""
    print("Login...")

    # Step 1: GET login page to obtain CSRF token + cookies
    r = session.get(LOGIN_URL, timeout=30)
    print(f"  Login page status: {r.status_code}")

    if r.status_code != 200:
        print("  ERROR: Cannot load login page")
        return False

    soup = BeautifulSoup(r.text, "lxml")

    # Check for Cloudflare challenge
    if "challenge" in r.text.lower() or "xác minh" in r.text.lower():
        print("  WARN: Cloudflare challenge! curl_cffi usually bypasses this - retrying...")
        time.sleep(3)
        r = session.get(LOGIN_URL, timeout=30)
        soup = BeautifulSoup(r.text, "lxml")

    csrf = get_csrf_token(soup)
    print(f"  CSRF token: {csrf[:20]}..." if csrf else "  WARN: CSRF token not found")

    # Step 2: POST login form
    payload = {
        "authenticity_token": csrf or "",
        "user[email]":        email,
        "user[password]":     password,
        "user[remember_me]":  "1",
        "commit":             "Sign in",
    }

    session.headers.update({
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin":       SITE_URL,
        "Referer":      LOGIN_URL,
    })

    r2 = session.post(
        LOGIN_URL,
        data=payload,
        timeout=30,
        allow_redirects=True,
    )

    print(f"  POST login status: {r2.status_code} | URL: {r2.url}")

    # Validate login result
    if "sign_in" in r2.url and r2.status_code == 200:
        soup2 = BeautifulSoup(r2.text, "lxml")
        error = soup2.select_one(".alert-danger, .alert.alert-error, [class*='error']")
        if error:
            print(f"  ERROR: Login error: {error.get_text(strip=True)}")
        else:
            print("  WARN: Still on sign_in page - credentials may be wrong or request is blocked")
        return False

    # Check cookies
    cookies = dict(session.cookies)
    has_session = any(k in cookies for k in ["_itviec_session", "_session_id", "remember_user_token"])
    print(f"  Cookies: {list(cookies.keys())}")

    if has_session or "sign_in" not in r2.url:
        print("  OK: Login successful!")
        return True

    print("  ERROR: Login failed")
    return False


# ─── Parse job list ───────────────────────────────────────────────────────────

def parse_job_list(soup: BeautifulSoup) -> list[dict]:
    jobs = []
    cards = soup.select('div.job-card')

    for idx, card in enumerate(cards):
        try:
            job = {}

            # Title + URL
            h3 = card.find("h3")
            if h3:
                job["title"] = re.sub(r"\s+", " ", h3.get_text(strip=True))
                url_attr = h3.get("data-url") or h3.get("data-search--job-selection-job-url-value", "")
                if url_attr:
                    job_path = url_attr.split("?")[0].split("/content")[0]
                    job["url"] = job_path if job_path.startswith("http") else f"{SITE_URL}{job_path}"
                    job["job_id"] = extract_job_id(job["url"])
                else:
                    # Fallback: find link in h3
                    a = h3.find("a")
                    if a and a.get("href"):
                        href = a["href"].split("?")[0]
                        job["url"] = href if href.startswith("http") else f"{SITE_URL}{href}"
                        job["job_id"] = extract_job_id(job["url"])

            # Company
            company_el = card.select_one("a.text-rich-grey")
            if company_el:
                job["company_name"] = company_el.get_text(strip=True)
                co_href            = company_el.get("href", "").split("?")[0]
                job["company_url"] = co_href if co_href.startswith("http") else f"{SITE_URL}{co_href}"
                job["company_id"]  = co_href.rstrip("/").split("/")[-1]

            # Salary
            salary_el = card.select_one("div.salary span, .salary > span")
            if salary_el:
                # Remove icon SVGs before extracting plain text
                for svg in salary_el.find_all("svg"):
                    svg.decompose()
                job["salary"] = salary_el.get_text(strip=True) or None

            # Location
            loc_el = card.select_one("svg use[href*='map-pin']")
            if loc_el:
                loc_div = loc_el.find_parent("svg")
                if loc_div:
                    next_div = loc_div.find_next_sibling()
                    if next_div:
                        job["location"] = next_div.get_text(strip=True)

            # Posted date
            posted_el = card.select_one(".small-text.text-dark-grey")
            if posted_el:
                posted_text = re.sub(r"\s+", " ", posted_el.get_text(strip=True))
                posted_raw = posted_text.replace("Posted", "").strip()
                posted_date = compute_posted_date(posted_raw, datetime.utcnow())
                if posted_date:
                    job["posted_date"] = posted_date

            job["crawled_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            jobs.append(job)

        except Exception as e:
            print(f"  WARN: Error parsing job #{idx+1}: {e}")

    return jobs


# ─── Parse job detail ─────────────────────────────────────────────────────────

def parse_job_detail(soup: BeautifulSoup) -> dict:
    detail = {}

    def norm(text: str) -> str:
        return (text or "").strip().lower().rstrip(":")

    def get_labeled_links(container, label: str) -> list[str]:
        if not container:
            return []
        label_key = norm(label)
        for div in container.find_all("div"):
            if norm(div.get_text(" ", strip=True)) == label_key:
                sibling = div.find_next_sibling("div")
                if not sibling:
                    return []
                return [a.get_text(strip=True) for a in sibling.find_all("a") if a.get_text(strip=True)]
        return []

    def get_labeled_itags(container, label: str) -> list[str]:
        if not container:
            return []
        label_key = norm(label)
        for div in container.find_all("div"):
            if norm(div.get_text(" ", strip=True)) == label_key:
                sibling = div.find_next_sibling("div")
                if not sibling:
                    return []
                return [
                    t.get_text(strip=True)
                    for t in sibling.select(".itag")
                    if t.get_text(strip=True)
                ]
        return []

    def find_heading_block(container, keywords: list[str]):
        if not container:
            return None
        keys = [k.lower() for k in keywords]
        for h2 in container.find_all("h2"):
            h2_text = h2.get_text(" ", strip=True).lower()
            if any(k in h2_text for k in keys):
                return h2.find_parent("div") or h2.parent
        return None

    def block_text_without_h2(block) -> Optional[str]:
        if not block:
            return None
        texts = []
        for child in block.children:
            if getattr(child, "name", None) == "h2":
                continue
            if hasattr(child, "get_text"):
                text = child.get_text(separator="\n", strip=True)
            else:
                text = str(child).strip()
            if text:
                texts.append(text)
        return "\n".join(texts) if texts else None

    def get_employer_value(container, label: str) -> Optional[str]:
        if not container:
            return None
        label_key = norm(label)
        for div in container.find_all("div"):
            if norm(div.get_text(" ", strip=True)) == label_key:
                row = div.parent
                if not row:
                    return None
                value_el = row.find("div", class_=lambda c: c and "text-end" in c)
                if value_el:
                    value = " ".join(value_el.stripped_strings)
                    return value or None
                span_values = [s.get_text(strip=True) for s in row.find_all("span") if s.get_text(strip=True)]
                if span_values:
                    return " ".join(span_values)
        return None

    job_header = soup.select_one("div.job-show-info")
    info_container = job_header.select_one("div.imb-3") if job_header else None
    content_section = soup.select_one("section.job-content")
    employer_section = soup.select_one("section.job-show-employer-info")

    salary_raw = soup.select_one(".job-header-info .salary")
    if salary_raw:
        salary_text = salary_raw.get_text(" ", strip=True)
        detail["salary"] = salary_text if salary_text and "sign in" not in salary_text.lower() else "unavailable"

    if info_container:
        posted_text = ""
        clock_use = info_container.select_one("svg use[href*='clock']")
        if clock_use:
            clock_svg = clock_use.find_parent("svg")
            if clock_svg:
                posted_span = clock_svg.find_next_sibling("span")
                if posted_span:
                    posted_text = posted_span.get_text(strip=True).replace("Posted", "").strip()

        posted_date = compute_posted_date(posted_text, datetime.utcnow())
        if posted_date:
            detail["posted_date"] = posted_date

        map_pin_use = info_container.select_one("svg use[href*='map-pin']")
        if map_pin_use:
            map_svg = map_pin_use.find_parent("svg")
            if map_svg:
                loc_span = map_svg.find_next_sibling("span")
                if loc_span:
                    detail["location"] = loc_span.get_text(strip=True)

        work_mode_items = [
            el.get_text(strip=True)
            for el in info_container.select(".preview-header-item span")
            if el.get_text(strip=True) and "posted" not in el.get_text(strip=True).lower()
        ]
        if work_mode_items:
            detail["work_mode"] = ", ".join(work_mode_items)

        skills = get_labeled_links(info_container, "Skills")
        if skills:
            detail["skills"] = skills

        job_expertise = get_labeled_links(info_container, "Job Expertise")
        if job_expertise:
            detail["job_expertise"] = job_expertise

        job_domains = get_labeled_itags(info_container, "Job Domain")
        if job_domains:
            detail["job_domains"] = job_domains

    reasons_block = find_heading_block(content_section, ["reasons"])
    if reasons_block:
        top_reasons = [li.get_text(strip=True) for li in reasons_block.find_all("li") if li.get_text(strip=True)]
        if top_reasons:
            detail["top_reasons"] = top_reasons

    description_block = find_heading_block(content_section, ["job description", "mô tả công việc"])
    requirements_block = find_heading_block(content_section, ["skills and experience", "yêu cầu"])
    benefits_block = find_heading_block(content_section, ["love working here", "quyền lợi"])

    description = block_text_without_h2(description_block)
    requirements = block_text_without_h2(requirements_block)
    benefits = block_text_without_h2(benefits_block)

    if description:
        detail["description"] = description
    elif content_section:
        detail["description"] = content_section.get_text(separator="\n", strip=True)

    if requirements:
        detail["requirements"] = requirements
    if benefits:
        detail["benefits"] = benefits

    if employer_section:
        full_name_el = employer_section.select_one(".imt-5 p")
        if full_name_el:
            detail["company_full_name"] = full_name_el.get_text(strip=True)

        company_type = get_employer_value(employer_section, "Company type")
        company_industry = get_employer_value(employer_section, "Company industry")
        company_size = get_employer_value(employer_section, "Company size")
        country = get_employer_value(employer_section, "Country")
        working_days = get_employer_value(employer_section, "Working days")
        overtime_policy = get_employer_value(employer_section, "Overtime policy")

        if company_type:
            detail["company_type"] = company_type
        if company_industry:
            detail["company_industry"] = company_industry
        if company_size:
            detail["company_size"] = company_size
        if country:
            detail["country"] = country
        if working_days:
            detail["working_days"] = working_days
        if overtime_policy:
            detail["overtime_policy"] = overtime_policy

    return detail


# ─── Main crawl ───────────────────────────────────────────────────────────────

def crawl():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    # Load existing data
    all_jobs = load_json(OUTPUT_FILE)
    existing_ids = {j["job_id"] for j in all_jobs if j.get("job_id")}
    print(f"Loaded {len(all_jobs)} existing jobs")

    # Login
    if not login(EMAIL, PASSWORD):
        print("ERROR: Login failed. Stopping.")
        return

    new_count = 0

    for page_num in range(1, MAX_PAGES + 1):
        url = BASE_URL if page_num == 1 else f"{BASE_URL}?page={page_num}"
        print(f"\nPage {page_num}: {url}")

        soup = get_page(url, referer=f"{BASE_URL}?page={page_num - 1}" if page_num > 1 else BASE_URL)
        if not soup:
            print("  ERROR: Cannot load page, skipping")
            wait()
            continue

        jobs = parse_job_list(soup)
        print(f"  OK: Found {len(jobs)} jobs")

        if not jobs:
            print("  END: No more jobs, stopping.")
            break

        for job in jobs:
            job_id = job.get("job_id")

            if job_id in existing_ids:
                print(f"  SKIP: {job_id}")
                continue

            # Crawl detail
            if job.get("url"):
                print(f"  DETAIL: {(job.get('title') or '')[:55]}...")
                wait()
                detail_soup = get_page(job["url"], referer=url)
                if detail_soup:
                    detail = parse_job_detail(detail_soup)
                    job.update({k: v for k, v in detail.items() if v})

                    # Lưu HTML log
                    log_path = LOG_DIR / f"itviec_job_{job_id}.html"
                    try:
                        log_path.write_text(str(detail_soup), encoding="utf-8")
                    except Exception:
                        pass

            all_jobs.append(job)
            existing_ids.add(job_id)
            new_count += 1

        # Persist after each page
        save_json(OUTPUT_FILE, all_jobs)
        print(f"  SAVED: {len(all_jobs)} total jobs")
        wait()

    save_json(OUTPUT_FILE, all_jobs)
    print(f"\nDone! {new_count} new jobs. Total: {len(all_jobs)}")


if __name__ == "__main__":
    crawl()