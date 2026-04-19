#!/usr/bin/env python3
"""
crawler_topcv.py
================
Refactored from Playwright to curl_cffi + BeautifulSoup.
Bypasses Cloudflare by impersonating Chrome, no browser and no login required.

Install:
    pip install curl_cffi beautifulsoup4 lxml

Run:
    python crawler_topcv.py
"""

import json
import re
import time
import random
import os
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

from curl_cffi import requests as cf
from bs4 import BeautifulSoup

# ─── Config ───────────────────────────────────────────────────────────────────
BASE_URL         = "https://www.topcv.vn/tim-viec-lam-cong-nghe-thong-tin-cr257"
SITE_URL         = "https://www.topcv.vn"
MAX_PAGES        = 1
MAX_JOBS_PER_RUN = 2     # 0 = crawl all
CRAWL_DETAIL     = True
WAIT_MIN         = 1.5   # seconds, random delay between requests
WAIT_MAX         = 3.5
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
OUTPUT_FILE      = "data_job/topcv_jobs.json"
LOG_DIR          = Path("topcv/logs")

# curl_cffi impersonate - mimic Chrome TLS/HTTP2 fingerprint
IMPERSONATE = "chrome124"

# ─── Selectors for LIST page ──────────────────────────────────────────────────
LIST_SELECTORS = {
    "container":  ".job-list-search-result",
    "job_item":   ".job-item-search-result",
    "title":      "h3.title a",
    "company":    "a.company span.company-name",
    "salary":     "label.salary span, label.title-salary",
    "location":   "label.address span.city-text",
    "experience": "label.exp span",
}

# ─── Selectors for DETAIL page ────────────────────────────────────────────────
# All fields use CSS selectors (equivalent to original XPath).
# Only update this section when TopCV changes HTML structure.
DETAIL_SELECTORS = {
    # Simple CSS selector
    "deadline": "div.job-detail__info--deadline-date",

    # Fields mapped from original XPath equivalents
    "experience":         ".section-experience .content-value",

    "requirements_tags":  ".job-tags__group:has(.job-tags__group-name) .job-tags__group-list-tag-scroll a.item",
    "benefits_tags":      None,   # see note below - filtered by group name in code
    "job_expertise":      None,   # see note below - filtered by group name in code

    "description":        ".job-description__item .job-description__item--content",
    "detail_requirements": ".job-detail-section.requirement .job-description__item--content",
    "detail_salary":      None,   # filtered by heading "Thu nhập" in code
    "detail_benefit":     ".job-detail-section.benefit .job-description__item--content",
    "other_benefits":     ".custom-form-job .custom-form-job__item",
    "detail_location":    None,   # filtered by heading "Địa điểm làm việc" in code
    "working_days":       None,   # filtered by heading "Thời gian làm việc" in code

    "job_level":          ".box-general-group-info .box-general-group-info-value",
    "education":          None,   # filtered by title in code
    "hiring_quantity":    None,   # filtered by title in code
    "work_mode":          None,   # filtered by title in code

    "company_name":       ".job-detail__company--information-item.company-name a.name",
    "company_size":       ".company-scale .company-value",
    "company_industry":   ".company-field .company-value",
}


# ─── Session (keep cookies across requests) ───────────────────────────────────
session = cf.Session(impersonate=IMPERSONATE)
session.headers.update({
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Cache-Control":   "no-cache",
    "Pragma":          "no-cache",
})


# ─── Helpers ──────────────────────────────────────────────────────────────────

def wait():
    time.sleep(random.uniform(WAIT_MIN, WAIT_MAX))


def save_json(jobs: list, filename: str) -> None:
    if not jobs:
        print("No data to save.")
        return
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(jobs, f, ensure_ascii=False, indent=2)
    print(f"Saved {len(jobs)} jobs to {filename}")


def load_json(filename: str) -> list:
    try:
        with open(filename, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []


def page_url(page_num: int) -> str:
    if page_num == 1:
        return BASE_URL
    return f"{BASE_URL}?page={page_num}"


def get_page(url: str, referer: str = BASE_URL) -> Optional[BeautifulSoup]:
    """Send GET request and return BeautifulSoup, or None on error."""
    try:
        session.headers.update({"Referer": referer})
        r = session.get(url, timeout=30)
        print(f"  GET {url} -> {r.status_code}")
        if r.status_code != 200:
            print(f"  WARN: Status {r.status_code}")
            return None
        return BeautifulSoup(r.text, "lxml")
    except Exception as e:
        print(f"  ERROR: {e}")
        return None


def css_text(soup: BeautifulSoup, selector: str) -> Optional[str]:
    """Get text from the first element matching a CSS selector."""
    el = soup.select_one(selector)
    if not el:
        return None
    return el.get_text(separator="\n", strip=True) or None


def css_attr(soup: BeautifulSoup, selector: str, attr_name: str) -> Optional[str]:
    """Get an attribute from the first element matching a CSS selector."""
    el = soup.select_one(selector)
    if not el:
        return None
    value = el.get(attr_name)
    return value.strip() if isinstance(value, str) and value.strip() else None


def to_absolute_url(raw_url: str) -> Optional[str]:
    if not raw_url:
        return None
    return raw_url if raw_url.startswith("http") else f"{SITE_URL}{raw_url}"


def get_id_from_url(url: str) -> Optional[str]:
    if not url:
        return None
    path_parts = urlparse(url).path.rstrip("/").split("/")
    return path_parts[-1].replace(".html", "") if path_parts else None


def css_texts(soup: BeautifulSoup, selector: str) -> list[str]:
    """Get text list from all elements matching a CSS selector."""
    return [
        el.get_text(strip=True)
        for el in soup.select(selector)
        if el.get_text(strip=True)
    ]


def get_job_description_item(soup: BeautifulSoup, heading_text: str) -> Optional[str]:
    """
    XPath equivalent:
        .//div[@class='job-description__item']
            [.//h3[contains(text(), '{heading_text}')]]
            //div[contains(@class, 'job-description__item--content')]
    Find .job-description__item block with matching heading and return content.
    """
    for block in soup.select(".job-description__item"):
        h3 = block.find("h3")
        if h3 and heading_text.lower() in h3.get_text().lower():
            content = block.select_one(".job-description__item--content")
            if content:
                return content.get_text(separator="\n", strip=True) or None
    return None


def get_general_info(soup: BeautifulSoup, title_text: str) -> Optional[str]:
    """
    XPath equivalent:
        .//div[@class='box-general-group-info']
            [.//div[@class='box-general-group-info-title'][contains(text(), '{title_text}')]]
            //div[@class='box-general-group-info-value']
    """
    for block in soup.select(".box-general-group-info"):
        title = block.select_one(".box-general-group-info-title")
        if title and title_text.lower() in title.get_text().lower():
            value = block.select_one(".box-general-group-info-value")
            if value:
                return value.get_text(strip=True) or None
    return None


def get_tags_by_group(soup: BeautifulSoup, group_name: str) -> list[str]:
    """
    XPath equivalent:
        .//div[@class='job-tags__group']
            [.//div[@class='job-tags__group-name'][contains(text(), '{group_name}')]]
            //div[contains(@class,'job-tags__group-list-tag-scroll')]//a[contains(@class,'item')]
    """
    for group in soup.select(".job-tags__group"):
        name_el = group.select_one(".job-tags__group-name")
        if name_el and group_name.lower() in name_el.get_text().lower():
            return [
                a.get_text(strip=True)
                for a in group.select(".job-tags__group-list-tag-scroll a.item")
                if a.get_text(strip=True)
            ]
    return []


# ─── Extract job list ─────────────────────────────────────────────────────────

def extract_job_list_data(soup: BeautifulSoup) -> list[dict]:
    jobs = []

    cards = soup.select(LIST_SELECTORS["job_item"])
    print(f"  DEBUG: Found {len(cards)} job cards in DOM")

    for idx, card in enumerate(cards):
        try:
            job = {"position": idx + 1}

            # ── Title & URL ──
            title_link = card.select_one(LIST_SELECTORS["title"])
            if title_link:
                job["title"] = (
                    title_link.get("data-original-title")
                    or title_link.get("title")
                    or title_link.get_text(strip=True)
                )
                raw_url = title_link.get("href", "")
                job["url"] = to_absolute_url(raw_url)
                job["job_id"] = get_id_from_url(job["url"]) if job.get("url") else None

                # job_id from card data attribute (higher priority when present)
            job_id_attr = card.get("data-job-id")
            if job_id_attr:
                job["job_id"] = job_id_attr

            # ── Company ──
            company_el = card.select_one(LIST_SELECTORS["company"])
            company_anchor = card.select_one("a.company")
            job["company"] = company_el.get_text(strip=True) if company_el else None

            company_href = company_anchor.get("href", "") if company_anchor else ""
            if company_href:
                job["company_url"] = to_absolute_url(company_href)
                job["company_id"] = get_id_from_url(job["company_url"]) if job.get("company_url") else None

            # ── Salary (remove icons before extracting text) ──
            salary_el = card.select_one(LIST_SELECTORS["salary"])
            if salary_el:
                for icon in salary_el.find_all(["i", "svg"]):
                    icon.decompose()
                job["salary"] = salary_el.get_text(strip=True) or None
            else:
                job["salary"] = None

            # ── Location ──
            loc_el = card.select_one(LIST_SELECTORS["location"])
            job["location"] = loc_el.get_text(strip=True) if loc_el else None

            # ── Experience ──
            exp_el = card.select_one(LIST_SELECTORS["experience"])
            job["experience"] = exp_el.get_text(strip=True) if exp_el else None

            jobs.append(job)

        except Exception as e:
            print(f"  Error extracting job #{idx + 1}: {e}")

    return jobs


# ─── Extract job detail ───────────────────────────────────────────────────────

def extract_job_detail_data(soup: BeautifulSoup, job_url: str) -> dict:
    detail = {"url": job_url}

    # ── Meta description ──
    meta = soup.find("meta", {"name": "description"})
    if meta and meta.get("content"):
        detail["meta_description"] = meta["content"]

    # ── Direct CSS selector fields ──
    detail["deadline"]         = css_text(soup, DETAIL_SELECTORS["deadline"])
    detail["experience"]       = css_text(soup, DETAIL_SELECTORS["experience"])
    detail["main_experience"]  = detail["experience"]
    detail["description"]      = css_text(soup, DETAIL_SELECTORS["description"])
    detail["detail_requirements"] = css_text(soup, DETAIL_SELECTORS["detail_requirements"])
    detail["detail_benefit"]   = css_text(soup, DETAIL_SELECTORS["detail_benefit"])
    detail["other_benefits"]   = css_texts(soup, DETAIL_SELECTORS["other_benefits"]) or None
    detail["company_name"]     = css_text(soup, DETAIL_SELECTORS["company_name"])
    detail["company_url"]      = to_absolute_url(css_attr(soup, DETAIL_SELECTORS["company_name"], "href") or "")
    detail["company_id"]       = get_id_from_url(detail["company_url"]) if detail.get("company_url") else None
    detail["company_size"]     = css_text(soup, DETAIL_SELECTORS["company_size"])
    detail["company_industry"] = css_text(soup, DETAIL_SELECTORS["company_industry"])

    # ── Tag groups (filtered by group name) ──
    detail["requirements_tags"] = get_tags_by_group(soup, "Yêu cầu") or None
    detail["benefits_tags"]     = get_tags_by_group(soup, "Quyền lợi") or None
    detail["job_expertise"]     = get_tags_by_group(soup, "Chuyên môn") or None

    # ── job-description__item (filtered by h3 heading) ──
    detail["detail_salary"]   = get_job_description_item(soup, "Thu nhập")
    detail["detail_location"] = get_job_description_item(soup, "Địa điểm làm việc")
    detail["working_days"]    = get_job_description_item(soup, "Thời gian làm việc")

    # ── box-general-group-info (filtered by title) ──
    detail["job_level"]        = get_general_info(soup, "Cấp bậc")
    detail["education"]        = get_general_info(soup, "Học vấn")
    detail["hiring_quantity"]  = get_general_info(soup, "Số lượng tuyển")
    detail["work_mode"]        = get_general_info(soup, "Hình thức làm việc")

    return detail


# ─── Main crawl ───────────────────────────────────────────────────────────────

def crawl_topcv() -> list:
    all_jobs = []
    jobs_crawled = 0

    for page_num in range(1, MAX_PAGES + 1):
        url = page_url(page_num)
        print(f"\nLoading list page #{page_num}: {url}")

        referer = page_url(page_num - 1) if page_num > 1 else BASE_URL
        soup = get_page(url, referer=referer)
        if not soup:
            print("  ERROR: Cannot load page, skipping")
            wait()
            continue

        list_jobs = extract_job_list_data(soup)
        print(f"Found {len(list_jobs)} jobs on page {page_num}")

        if not list_jobs:
            print("No jobs found on this page, stopping.")
            break

        for job in list_jobs:
            if MAX_JOBS_PER_RUN > 0 and jobs_crawled >= MAX_JOBS_PER_RUN:
                break

            # Record crawl time in local timestamp format for schema compatibility.
            job["crawled_date"] = datetime.now().strftime(TIMESTAMP_FORMAT)

            all_jobs.append(job)
            jobs_crawled += 1

            if CRAWL_DETAIL and job.get("url"):
                print(f"  Crawling detail: {job['title'][:60]}...")
                wait()

                detail_soup = get_page(job["url"], referer=url)
                if detail_soup:
                    detail = extract_job_detail_data(detail_soup, job["url"])
                    job.update({k: v for k, v in detail.items() if v})

                    # Save HTML log for selector debugging
                    LOG_DIR.mkdir(parents=True, exist_ok=True)
                    log_path = LOG_DIR / f"topcv_job_{job.get('job_id')}.html"
                    try:
                        log_path.write_text(str(detail_soup), encoding="utf-8")
                    except Exception:
                        pass

                    print(f"  [{job.get('job_id')}] {job.get('title')}")
                    print(f"    @ {job.get('company')} | {job.get('salary')} | {job.get('location')}")
                    print(f"    {job.get('url')}\n")

        if MAX_JOBS_PER_RUN > 0 and jobs_crawled >= MAX_JOBS_PER_RUN:
            print(f"Reached MAX_JOBS_PER_RUN ({MAX_JOBS_PER_RUN}), stopping.")
            break

        wait()

    return all_jobs


def main():
    print("Starting TopCV crawler (list + detail)...")
    jobs = crawl_topcv()

    if jobs:
        save_json(jobs, OUTPUT_FILE)
        print(f"\nDone! Crawled {len(jobs)} jobs.")
    else:
        print("\nNo data. Check LOG_DIR for debug HTML.")


if __name__ == "__main__":
    main()