from typing import Optional
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from .fetcher import SITE_URL

LIST_SELECTORS = {
    "container":  ".job-list-search-result",
    "job_item":   ".job-item-search-result",
    "title":      "h3.title a",
    "company":    "a.company span.company-name",
    "salary":     "label.salary span, label.title-salary",
    "location":   "label.address span.city-text",
    "experience": "label.exp span",
}

DETAIL_SELECTORS = {
    "deadline":           "div.job-detail__info--deadline-date",
    "main_experience":    ".section-experience .content-value",
    "requirements_tags":  ".job-tags__group:has(.job-tags__group-name) .job-tags__group-list-tag-scroll a.item",
    "benefits_tags":      None,
    "job_expertise":      None,
    "description":        ".job-description__item .job-description__item--content",
    "detail_requirements": ".job-detail-section.requirement .job-description__item--content",
    "detail_salary":      None,
    "detail_benefit":     ".job-detail-section.benefit .job-description__item--content",
    "other_benefits":     ".custom-form-job .custom-form-job__item",
    "detail_location":    None,
    "working_days":       None,
    "job_level":          ".box-general-group-info .box-general-group-info-value",
    "education":          None,
    "hiring_quantity":    None,
    "work_mode":          None,
    "company_name":       ".job-detail__company--information-item.company-name a.name",
    "company_size":       ".company-scale .company-value",
    "company_industry":   ".company-field .company-value",
}


def css_text(soup: BeautifulSoup, selector: str) -> Optional[str]:
    el = soup.select_one(selector)
    if not el:
        return None
    return el.get_text(separator="\n", strip=True) or None


def css_texts(soup: BeautifulSoup, selector: str) -> list[str]:
    return [
        el.get_text(strip=True)
        for el in soup.select(selector)
        if el.get_text(strip=True)
    ]


def get_job_description_item(soup: BeautifulSoup, heading_text: str) -> Optional[str]:
    for block in soup.select(".job-description__item"):
        h3 = block.find("h3")
        if h3 and heading_text.lower() in h3.get_text().lower():
            content = block.select_one(".job-description__item--content")
            if content:
                return content.get_text(separator="\n", strip=True) or None
    return None


def get_general_info(soup: BeautifulSoup, title_text: str) -> Optional[str]:
    for block in soup.select(".box-general-group-info"):
        title = block.select_one(".box-general-group-info-title")
        if title and title_text.lower() in title.get_text().lower():
            value = block.select_one(".box-general-group-info-value")
            if value:
                return value.get_text(strip=True) or None
    return None


def get_tags_by_group(soup: BeautifulSoup, group_name: str) -> list[str]:
    for group in soup.select(".job-tags__group"):
        name_el = group.select_one(".job-tags__group-name")
        if name_el and group_name.lower() in name_el.get_text().lower():
            return [
                a.get_text(strip=True)
                for a in group.select(".job-tags__group-list-tag-scroll a.item")
                if a.get_text(strip=True)
            ]
    return []


def extract_job_list_data(soup: BeautifulSoup) -> list[dict]:
    jobs = []
    cards = soup.select(LIST_SELECTORS["job_item"])
    print(f"  Found {len(cards)} job cards")

    for idx, card in enumerate(cards):
        try:
            job = {"position": idx + 1}

            title_link = card.select_one(LIST_SELECTORS["title"])
            if title_link:
                job["title"] = (
                    title_link.get("data-original-title")
                    or title_link.get("title")
                    or title_link.get_text(strip=True)
                )
                raw_url = title_link.get("href", "")
                job["url"] = raw_url if raw_url.startswith("http") else f"{SITE_URL}{raw_url}"

                path_parts = urlparse(job["url"]).path.rstrip("/").split("/")
                job["job_id"] = path_parts[-1].replace(".html", "") if path_parts else None

            job_id_attr = card.get("data-job-id")
            if job_id_attr:
                job["job_id"] = job_id_attr

            company_el = card.select_one(LIST_SELECTORS["company"])
            job["company"] = company_el.get_text(strip=True) if company_el else None

            salary_el = card.select_one(LIST_SELECTORS["salary"])
            if salary_el:
                for icon in salary_el.find_all(["i", "svg"]):
                    icon.decompose()
                job["salary"] = salary_el.get_text(strip=True) or None
            else:
                job["salary"] = None

            loc_el = card.select_one(LIST_SELECTORS["location"])
            job["location"] = loc_el.get_text(strip=True) if loc_el else None

            exp_el = card.select_one(LIST_SELECTORS["experience"])
            job["experience"] = exp_el.get_text(strip=True) if exp_el else None

            jobs.append(job)

        except Exception as e:
            print(f"  Error extracting job #{idx + 1}: {e}")

    return jobs


def extract_job_detail_data(soup: BeautifulSoup, job_url: str) -> dict:
    detail = {"url": job_url}

    meta = soup.find("meta", {"name": "description"})
    if meta and meta.get("content"):
        detail["meta_description"] = meta["content"]

    detail["deadline"]         = css_text(soup, DETAIL_SELECTORS["deadline"])
    detail["main_experience"]  = css_text(soup, DETAIL_SELECTORS["main_experience"])
    detail["description"]      = css_text(soup, DETAIL_SELECTORS["description"])
    detail["detail_requirements"] = css_text(soup, DETAIL_SELECTORS["detail_requirements"])
    detail["detail_benefit"]   = css_text(soup, DETAIL_SELECTORS["detail_benefit"])
    detail["other_benefits"]   = css_texts(soup, DETAIL_SELECTORS["other_benefits"]) or None
    detail["company_name"]     = css_text(soup, DETAIL_SELECTORS["company_name"])
    detail["company_size"]     = css_text(soup, DETAIL_SELECTORS["company_size"])
    detail["company_industry"] = css_text(soup, DETAIL_SELECTORS["company_industry"])

    detail["requirements_tags"] = get_tags_by_group(soup, "Yêu cầu") or None
    detail["benefits_tags"]     = get_tags_by_group(soup, "Quyền lợi") or None
    detail["job_expertise"]     = get_tags_by_group(soup, "Chuyên môn") or None

    detail["detail_salary"]   = get_job_description_item(soup, "Thu nhập")
    detail["detail_location"] = get_job_description_item(soup, "Địa điểm làm việc")
    detail["working_days"]    = get_job_description_item(soup, "Thời gian làm việc")

    detail["job_level"]        = get_general_info(soup, "Cấp bậc")
    detail["education"]        = get_general_info(soup, "Học vấn")
    detail["hiring_quantity"]  = get_general_info(soup, "Số lượng tuyển")
    detail["work_mode"]        = get_general_info(soup, "Hình thức làm việc")

    return detail
