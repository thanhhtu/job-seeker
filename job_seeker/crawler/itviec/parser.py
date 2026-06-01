import re
from typing import Optional

from bs4 import BeautifulSoup

import sys
from pathlib import Path

_root = Path(__file__).resolve().parents[2]
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from src.utils.datetime_utils import now_str

from .fetcher import SITE_URL, compute_posted_date, extract_job_id


def parse_job_list(soup: BeautifulSoup) -> list[dict]:
    jobs = []
    cards = soup.select("div.job-card")

    for idx, card in enumerate(cards):
        try:
            job = {}

            h3 = card.find("h3")
            if h3:
                job["title"] = re.sub(r"\s+", " ", h3.get_text(strip=True))
                url_attr = h3.get("data-url") or h3.get(
                    "data-search--job-selection-job-url-value", ""
                )
                if url_attr:
                    job_path = url_attr.split("?")[0].split("/content")[0]
                    job["url"] = (
                        job_path
                        if job_path.startswith("http")
                        else f"{SITE_URL}{job_path}"
                    )
                    job["job_id"] = extract_job_id(job["url"])
                else:
                    a = h3.find("a")
                    if a and a.get("href"):
                        href = a["href"].split("?")[0]
                        job["url"] = (
                            href if href.startswith("http") else f"{SITE_URL}{href}"
                        )
                        job["job_id"] = extract_job_id(job["url"])

            company_el = card.select_one("a.text-rich-grey")
            if company_el:
                job["company_name"] = company_el.get_text(strip=True)
                co_href = company_el.get("href", "").split("?")[0]
                job["company_url"] = (
                    co_href if co_href.startswith("http") else f"{SITE_URL}{co_href}"
                )
                job["company_id"] = co_href.rstrip("/").split("/")[-1]

            salary_el = card.select_one("div.salary span, .salary > span")
            if salary_el:
                for svg in salary_el.find_all("svg"):
                    svg.decompose()
                job["salary"] = salary_el.get_text(strip=True) or None

            loc_el = card.select_one("svg use[href*='map-pin']")
            if loc_el:
                loc_div = loc_el.find_parent("svg")
                if loc_div:
                    next_div = loc_div.find_next_sibling()
                    if next_div:
                        job["location"] = next_div.get_text(strip=True)

            posted_el = card.select_one(".small-text.text-dark-grey")
            if posted_el:
                posted_text = re.sub(r"\s+", " ", posted_el.get_text(strip=True))
                posted_raw = posted_text.replace("Posted", "").strip()
                posted_date = compute_posted_date(posted_raw)
                if posted_date:
                    job["posted_date"] = posted_date

            job["crawled_date"] = now_str()
            jobs.append(job)

        except Exception as e:
            print(f"  Error parsing job #{idx + 1}: {e}")

    return jobs


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
                return [
                    a.get_text(strip=True)
                    for a in sibling.find_all("a")
                    if a.get_text(strip=True)
                ]
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
                span_values = [
                    s.get_text(strip=True)
                    for s in row.find_all("span")
                    if s.get_text(strip=True)
                ]
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
        detail["salary"] = (
            salary_text
            if salary_text and "sign in" not in salary_text.lower()
            else "unavailable"
        )

    if info_container:
        posted_text = ""
        clock_use = info_container.select_one("svg use[href*='clock']")
        if clock_use:
            clock_svg = clock_use.find_parent("svg")
            if clock_svg:
                posted_span = clock_svg.find_next_sibling("span")
                if posted_span:
                    posted_text = posted_span.get_text(strip=True).replace("Posted", "").strip()

        posted_date = compute_posted_date(posted_text)
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
        top_reasons = [
            li.get_text(strip=True)
            for li in reasons_block.find_all("li")
            if li.get_text(strip=True)
        ]
        if top_reasons:
            detail["top_reasons"] = top_reasons

    description_block = find_heading_block(
        content_section, ["job description", "mô tả công việc"]
    )
    requirements_block = find_heading_block(
        content_section, ["skills and experience", "yêu cầu"]
    )
    benefits_block = find_heading_block(
        content_section, ["love working here", "quyền lợi"]
    )

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
