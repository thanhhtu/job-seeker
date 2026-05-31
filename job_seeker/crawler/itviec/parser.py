import re
import time

from bs4 import BeautifulSoup

from .fetcher import SITE_URL, extract_job_id, parse_relative_time


def parse_job_list(soup: BeautifulSoup) -> list[dict]:
    jobs = []
    cards = soup.select('div.job-card')

    for idx, card in enumerate(cards):
        try:
            job = {}

            h3 = card.find("h3")
            if h3:
                job["title"] = re.sub(r"\s+", " ", h3.get_text(strip=True))
                url_attr = h3.get("data-url") or h3.get("data-search--job-selection-job-url-value", "")
                if url_attr:
                    job_path = url_attr.split("?")[0].split("/content")[0]
                    job["url"] = job_path if job_path.startswith("http") else f"{SITE_URL}{job_path}"
                    job["job_id"] = extract_job_id(job["url"])
                else:
                    a = h3.find("a")
                    if a and a.get("href"):
                        href = a["href"].split("?")[0]
                        job["url"] = href if href.startswith("http") else f"{SITE_URL}{href}"
                        job["job_id"] = extract_job_id(job["url"])

            company_el = card.select_one("a.text-rich-grey")
            if company_el:
                job["company"]     = company_el.get_text(strip=True)
                co_href            = company_el.get("href", "").split("?")[0]
                job["company_url"] = co_href
                job["company_id"]  = co_href.rstrip("/").split("/")[-1]

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
                job["posted_at"] = posted_text.replace("Posted", "").strip()
                dt = parse_relative_time(posted_text)
                if dt:
                    job["posted_date"] = dt.strftime("%Y-%m-%d")

            job["crawled_date"] = int(time.time() * 1000)
            jobs.append(job)

        except Exception as e:
            print(f"  Error parsing job #{idx+1}: {e}")

    return jobs


def parse_job_detail(soup: BeautifulSoup) -> dict:
    detail = {}

    salary_el = soup.select_one(".salary > span, span.salary-text")
    if salary_el:
        for svg in salary_el.find_all("svg"):
            svg.decompose()
        detail["salary"] = salary_el.get_text(strip=True) or None

    skills = []
    for tag in soup.select("a.itag, span.itag, .tag-list a"):
        t = tag.get_text(strip=True)
        if t:
            skills.append(t)
    if skills:
        detail["skills"] = skills

    for section_id, keywords in [
        ("description",  ["job description", "mô tả công việc"]),
        ("requirements", ["skills and experience", "yêu cầu"]),
        ("benefits",     ["love working here", "quyền lợi"]),
    ]:
        for h2 in soup.find_all("h2"):
            if any(kw in h2.get_text().lower() for kw in keywords):
                parent = h2.find_parent()
                if parent:
                    texts = []
                    for el in parent.children:
                        if el == h2:
                            continue
                        text = el.get_text(separator="\n", strip=True) if hasattr(el, "get_text") else str(el).strip()
                        if text:
                            texts.append(text)
                    if texts:
                        detail[section_id] = "\n".join(texts)
                break

    if "description" not in detail:
        content_el = soup.select_one(".job-content, #job-content")
        if content_el:
            detail["description"] = content_el.get_text(separator="\n", strip=True)

    loc_el = soup.select_one("svg use[href*='map-pin']")
    if loc_el:
        parent_svg = loc_el.find_parent("svg")
        if parent_svg:
            next_el = parent_svg.find_next_sibling()
            if next_el:
                detail["location"] = next_el.get_text(strip=True)

    header_items = soup.select(".preview-header-item span")
    if header_items:
        detail["work_mode"] = ", ".join(
            el.get_text(strip=True) for el in header_items
            if el.get_text(strip=True) and "posted" not in el.get_text().lower()
        )

    return detail
