import asyncio
import json
import random
from typing import List, Dict, Optional, Union
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
from urllib.parse import urlparse
import os

BASE_URL = "https://www.topcv.vn/tim-viec-lam-cong-nghe-thong-tin-cr257"
HEADLESS = False
MAX_PAGES = 1           # Number of listing pages to crawl
MAX_JOBS_PER_RUN = 2    # Number of jobs to crawl (0 = all)
CRAWL_DETAIL = True     # Whether to crawl job detail pages or not
TIMEOUT_MS = 60000
OUTPUT_FILE = "data/test_topcv_jobs.json"

LIST_SELECTORS = {
    "container": ".job-list-search-result",
    "job_item": ".job-item-search-result",
    "title": "h3.title a",
    "company": "a.company span.company-name",
    "salary": "label.salary span, label.title-salary",
    "location": "label.address span.city-text",
    "experience": "label.exp span",
}

DETAIL_XPATH = {
    "deadline": ".job-detail__info--deadline-date",
    "main_experience": "//div[contains(@class,'section-experience')]//div[contains(@class,'content-value')]",

    "requirements_tags": "//div[contains(@class,'job-tags__group')][descendant::*[contains(.,'Yêu cầu')]]//a",
    "benefits_tags": "//div[contains(@class,'job-tags__group')][descendant::*[contains(.,'Quyền lợi')]]//a",
    "job_expertise": "//div[contains(@class,'job-tags__group')][descendant::*[contains(.,'Chuyên môn')]]//a",

    "description": "//div[contains(@class,'job-description__item')][descendant::h3[contains(.,'Mô tả')]]//div[contains(@class,'content')]",
    "detail_requirements": "//div[contains(@class,'job-description__item')][descendant::h3[contains(.,'Yêu cầu')]]//div[contains(@class,'content')]",
    "detail_salary": "//div[contains(@class,'job-description__item')][descendant::h3[contains(.,'Thu nhập')]]//div[contains(@class,'content')]",
    "detail_benefit": "//div[contains(@class,'job-description__item')][descendant::h3[contains(.,'Phúc lợi') or contains(.,'Quyền lợi')]]//div[contains(@class,'content')]",

    "other_benefits": ".custom-form-job__item",

    "detail_location": "//div[contains(@class,'job-description__item')][descendant::h3[contains(.,'Địa điểm')]]//div[contains(@class,'content')]",
    "working_days": "//div[contains(@class,'job-description__item')][descendant::h3[contains(.,'Thời gian')]]//div[contains(@class,'content')]",

    "job_level": "//div[contains(@class,'box-general-group-info')][descendant::*[contains(.,'Cấp bậc')]]//div[contains(@class,'value')]",
    "education": "//div[contains(@class,'box-general-group-info')][descendant::*[contains(.,'Học vấn')]]//div[contains(@class,'value')]",
    "hiring_quantity": "//div[contains(@class,'box-general-group-info')][descendant::*[contains(.,'Số lượng')]]//div[contains(@class,'value')]",
    "work_mode": "//div[contains(@class,'box-general-group-info')][descendant::*[contains(.,'Hình thức')]]//div[contains(@class,'value')]",

    "company_name": "//div[contains(@class,'company-name')]//a[contains(@class,'name')]",
    "company_size": ".company-scale .company-value",
    "company_industry": ".company-field .company-value",
}

CSS_FIELDS = {"deadline", "company_size", "company_industry", "other_benefits"}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

STEALTH_SCRIPT = """
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
Object.defineProperty(navigator, 'languages', { get: () => ['vi-VN','vi','en-US','en'] });
window.chrome = { runtime: {} };
"""

def _rand(lo=2.0, hi=4.0):
    return random.uniform(lo, hi)

async def _scroll(page):
    try:
        max_height = await page.evaluate("document.body.scrollHeight")
        curr_pos = 0
        while curr_pos < max_height:
            step = random.randint(300, 600)
            curr_pos += step
            await page.evaluate(f"window.scrollTo(0, {curr_pos})")
            await page.wait_for_timeout(random.randint(500, 1000))
            max_height = await page.evaluate("document.body.scrollHeight")
            if curr_pos > 10000: break 
    except Exception:
        pass

async def _xpath_text(page, xpath: str) -> Optional[str]:
    try:
        el = page.locator(f"xpath={xpath}").first
        if await el.is_visible(timeout=3000):
            return (await el.inner_text()).strip()
        return None
    except Exception: return None

async def _xpath_texts(page, xpath: str) -> List[str]:
    try:
        els = await page.locator(f"xpath={xpath}").all()
        results = []
        for el in els:
            t = (await el.inner_text()).strip()
            if t:
                results.append(t)
        return results
    except Exception: return []

async def _wait_not_blocked(page, timeout=30):
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        try:
            title = (await page.title()).lower()
            if not any(x in title for x in ["cloudflare", "just a moment", "attention required"]):
                return True
        except: pass
        await page.wait_for_timeout(2000)
    return False

async def extract_detail(page, url: str) -> dict:
    detail = {"url": url}
    print(f"  → Detail: {url}")
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=TIMEOUT_MS)
        if not await _wait_not_blocked(page): 
            return {"url": url, "error": "blocked_by_cloudflare"}
        
        await _scroll(page) 
        
        for field, selector in DETAIL_XPATH.items():
            if field in CSS_FIELDS:
                if field == "other_benefits":
                    els = await page.query_selector_all(selector)
                    val = [((await el.inner_text()).strip()) for el in els]
                    val = [v for v in val if v]
                else:
                    el = await page.query_selector(selector)
                    val = (await el.inner_text()).strip() if el else None
            elif "tags" in field:
                val = await _xpath_texts(page, selector)
            else:
                val = await _xpath_text(page, selector)
            
            if val:
                detail[field] = val
    except PlaywrightTimeout:
        print(f"The details page timed out, but it still tried to retrieve the current data...")
    except Exception as e:
        detail["error"] = str(e)
    return detail

async def extract_list(page) -> list:
    jobs = []

    try:
        await page.wait_for_selector(LIST_SELECTORS["container"], timeout=15_000)
        items = await page.query_selector_all(LIST_SELECTORS["job_item"])
        for idx, el in enumerate(items):
            try:
                job = {"position": idx + 1}

                # Title + URL + job_id
                title_link = await el.query_selector(LIST_SELECTORS["title"])
                if title_link:
                    job["title"] = (await title_link.get_attribute("data-original-title") or await title_link.inner_text()).strip()
                    raw_url = await title_link.get_attribute("href") or ""
                    job["url"] = raw_url if raw_url.startswith("http") else f"https://www.topcv.vn{raw_url}"
                    job["job_id"] = urlparse(job["url"]).path.split("/")[-1].replace(".html", "")

                # Company
                company_el = await el.query_selector(LIST_SELECTORS["company"])
                if company_el:
                    job["company"] = (await company_el.inner_text()).strip()

                # Salary
                salary_el = await el.query_selector(LIST_SELECTORS["salary"])
                if salary_el:
                    job["salary"] = (await salary_el.inner_text()).strip()

                # Location
                location_el = await el.query_selector(LIST_SELECTORS["location"])
                if location_el:
                    job["location"] = (await location_el.inner_text()).strip()

                # Experience
                exp_el = await el.query_selector(LIST_SELECTORS["experience"])
                if exp_el:
                    job["experience"] = (await exp_el.inner_text()).strip()

                jobs.append(job)
            except:
                continue
    except Exception as e:
        print(f"Job listings not found: {e}")
    return jobs

async def crawl_topcv():
    all_jobs = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS, args=["--disable-blink-features=AutomationControlled"])
        context = await browser.new_context(user_agent=random.choice(USER_AGENTS))
        await context.add_init_script(STEALTH_SCRIPT)
        page = await context.new_page()

        current_page = 1
        jobs_count = 0

        try:
            while current_page <= MAX_PAGES:
                url = BASE_URL if current_page == 1 else f"{BASE_URL}?page={current_page}"
                print(f"\nLoading list page #{current_page}: {url}")
                
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=TIMEOUT_MS)
                except PlaywrightTimeout:
                    print("List page timeout, try again by waiting for the selector...")

                if not await _wait_not_blocked(page): 
                    print("Cloudiflare block detected on list page, stopping crawler.")
                    break
                
                await _scroll(page)
                list_jobs = await extract_list(page)
                print(f"  Found {len(list_jobs)} jobs.")

                for job in list_jobs:
                    if 0 < MAX_JOBS_PER_RUN <= jobs_count: break
                    
                    if CRAWL_DETAIL and "url" in job:
                        detail_data = await extract_detail(page, job["url"])
                        job.update(detail_data)
                        await asyncio.sleep(_rand(2, 4))
                    
                    all_jobs.append(job)
                    jobs_count += 1

                current_page += 1
                if current_page <= MAX_PAGES:
                    await asyncio.sleep(_rand(5, 8))

        finally:
            await browser.close()
    return all_jobs

def save_json(jobs, filename):
    if not jobs: return
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(jobs, f, ensure_ascii=False, indent=2)

async def main():
    print("Starting TopCV crawler (list + detail)...")
    jobs = await crawl_topcv()
    if jobs:
        save_json(jobs, OUTPUT_FILE)
        print(f"\nCompleted! Saved {len(jobs)} jobs to {OUTPUT_FILE}")
    else:
        print("\nNo data. Try setting HEADLESS=False to debug.")


if __name__ == "__main__":
    asyncio.run(main())
