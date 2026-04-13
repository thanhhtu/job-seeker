import asyncio
import json
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
from urllib.parse import urlparse

BASE_URL = "https://www.topcv.vn/tim-viec-lam-cong-nghe-thong-tin-cr257"
HEADLESS = True
MAX_PAGES = 6           # Number of listing pages to crawl
MAX_JOBS_PER_RUN = 0   # Number of jobs to crawl (0 = all)
CRAWL_DETAIL = True     # Whether to crawl job detail pages or not
TIMEOUT_MS = 60000

LIST_SELECTORS = {
    "container":    ".job-list-search-result",
    "job_item":     ".job-item-search-result",
    "title":        "h3.title a",
    "company":      "a.company span.company-name",
    "salary":       "label.salary span, label.title-salary",
    "location":     "label.address span.city-text",
    "experience":   "label.exp span",
    "next_button":  "a[rel='next'], .pagination a[title='Trang sau']",
}

DETAIL_SELECTORS = {
    "job_id": "[data-job-id], .job-id",
    "deadline": "div.job-detail__info--deadline-date",

    "main_experience": "//div[contains(@class, 'section-experience')]//div[contains(@class, 'content-value')]",

    "requirements_tags": (
        ".//div[@class='job-tags__group']"
        "[.//div[@class='job-tags__group-name'][contains(text(), 'Yêu cầu')]]"
        "//div[contains(@class, 'job-tags__group-list-tag-scroll')]//a[contains(@class, 'item')]"
    ),

    "benefits_tags": (
        ".//div[@class='job-tags__group']"
        "[.//div[@class='job-tags__group-name'][contains(text(), 'Quyền lợi')]]"
        "//div[contains(@class, 'job-tags__group-list-tag-scroll')]//a[contains(@class, 'item')]"
    ),

    "job_expertise": (
        ".//div[@class='job-tags__group']"
        "[.//div[@class='job-tags__group-name'][contains(text(), 'Chuyên môn')]]"
        "//div[contains(@class, 'job-tags__group-list-tag-scroll')]//a[contains(@class, 'item')]"
    ),

    "description": (
        ".//div[@class='job-description__item']"
        "[.//h3[contains(text(), 'Mô tả công việc')]]"
        "//div[contains(@class, 'job-description__item--content')]"
    ),

    "detail_requirements": (
        ".//div[contains(@class, 'job-detail-section') and contains(@class, 'requirement')]"
        "//div[contains(@class, 'job-description__item--content')]"
    ),

    "detail_salary": (
        ".//div[@class='job-description__item']"
        "[.//h3[contains(text(), 'Thu nhập')]]"
        "//div[contains(@class, 'job-description__item--content')]"
    ),

    "detail_benefit": (
        ".//div[contains(@class, 'job-detail-section') and contains(@class, 'benefit')]"
        "//div[contains(@class, 'job-description__item--content')]"
    ),

    "other_benefits": (
        ".//div[contains(@class, 'custom-form-job')]"
        "//div[contains(@class, 'custom-form-job__item')]"
    ),

    "detail_location": (
        ".//div[@class='job-description__item']"
        "[.//h3[contains(text(), 'Địa điểm làm việc')]]"
        "//div[contains(@class, 'job-description__item--content')]"
    ),

    "working_days": (
        ".//div[@class='job-description__item']"
        "[.//h3[contains(text(), 'Thời gian làm việc')]]"
        "//div[contains(@class, 'job-description__item--content')]"
    ),

    "job_level": (
        ".//div[@class='box-general-group-info']"
        "[.//div[@class='box-general-group-info-title'][contains(text(), 'Cấp bậc')]]"
        "//div[@class='box-general-group-info-value']"
    ),

    "education": (
        ".//div[@class='box-general-group-info']"
        "[.//div[@class='box-general-group-info-title'][contains(text(), 'Học vấn')]]"
        "//div[@class='box-general-group-info-value']"
    ),
    
    "hiring_quantity": (
        ".//div[@class='box-general-group-info']"
        "[.//div[@class='box-general-group-info-title'][contains(text(), 'Số lượng tuyển')]]"
        "//div[@class='box-general-group-info-value']"
    ),

    "work_mode": (
        ".//div[@class='box-general-group-info']"
        "[.//div[@class='box-general-group-info-title'][contains(text(), 'Hình thức làm việc')]]"
        "//div[@class='box-general-group-info-value']"
    ),

    "company_name": (
        ".//div[@class='job-detail__company--information-item company-name']"
        "//a[contains(@class, 'name')]"
    ),

    "company_size": (
        ".//div[contains(@class, 'company-scale')]//div[@class='company-value']"
    ),

    "company_industry": (
        ".//div[contains(@class, 'company-field')]//div[@class='company-value']"
    ),
}

async def _xpath_texts(page, xpath: str) -> list:
    els = await page.locator(f"xpath={xpath}").all()
    results = []
    for el in els:
        t = (await el.inner_text()).strip()
        if t:
            results.append(t)
    return results


async def _xpath_text(page, xpath: str):
    texts = await _xpath_texts(page, xpath)
    return texts[0] if texts else None


async def _css_text(page, selector: str):
    el = await page.query_selector(selector)
    if not el:
        return None
    t = (await el.inner_text()).strip()
    return t or None

async def extract_job_list_data(page) -> list:
    jobs = []

    try:
        await page.wait_for_selector(LIST_SELECTORS["container"], timeout=20000)
    except PlaywrightTimeout:
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(2000)

    job_elements = await page.query_selector_all(LIST_SELECTORS["job_item"])

    for idx, job_el in enumerate(job_elements):
        try:
            job = {"position": idx + 1}

            title_link = await job_el.query_selector(LIST_SELECTORS["title"])
            if title_link:
                job["title"] = (
                    await title_link.get_attribute("data-original-title")
                    or (await title_link.inner_text()).strip()
                )
                raw_url = await title_link.get_attribute("href") or ""
                job["url"] = raw_url if raw_url.startswith("http") else f"https://www.topcv.vn{raw_url}"

                path_parts = urlparse(job["url"]).path.rstrip("/").split("/")
                job["job_id"] = path_parts[-1].replace(".html", "") if path_parts else None

            company_el = await job_el.query_selector(LIST_SELECTORS["company"])
            job["company"] = (await company_el.inner_text()).strip() if company_el else None

            salary_el = await job_el.query_selector(LIST_SELECTORS["salary"])
            if salary_el:
                raw_salary = await salary_el.evaluate("""
                    el => {
                        const clone = el.cloneNode(true);
                        clone.querySelectorAll('i, svg').forEach(i => i.remove());
                        return clone.textContent.trim();
                    }
                """)
                job["salary"] = raw_salary.strip() or None
            else:
                job["salary"] = None

            loc_el = await job_el.query_selector(LIST_SELECTORS["location"])
            job["location"] = (await loc_el.inner_text()).strip() if loc_el else None

            exp_el = await job_el.query_selector(LIST_SELECTORS["experience"])
            job["experience"] = (await exp_el.inner_text()).strip() if exp_el else None

            job_id_attr = await job_el.get_attribute("data-job-id")
            if job_id_attr:
                job["job_id"] = job_id_attr

            jobs.append(job)

        except Exception as e:
            print(f"Error extract job #{idx + 1}: {e}")

    return jobs


async def extract_job_detail_data(page, job_url: str) -> dict:
    detail = {"url": job_url}

    try:
        await page.goto(job_url, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(2000)
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(1000)

        try:
            meta_desc = await page.get_attribute("meta[name='description']", "content")
            if meta_desc:
                detail["meta_description"] = meta_desc
        except Exception:
            pass

        detail["deadline"] = await _css_text(page, DETAIL_SELECTORS["deadline"])

        main_experience = await _xpath_text(page, DETAIL_SELECTORS["main_experience"])
        if main_experience:
            detail["experience"] = main_experience

        detail["requirements_tags"] = await _xpath_texts(page, DETAIL_SELECTORS["requirements_tags"])
        detail["benefits_tags"] = await _xpath_texts(page, DETAIL_SELECTORS["benefits_tags"])
        detail["job_expertise"] = await _xpath_texts(page, DETAIL_SELECTORS["job_expertise"])
        detail["description"] = await _xpath_text(page, DETAIL_SELECTORS["description"])
        detail["detail_requirements"] = await _xpath_text(page, DETAIL_SELECTORS["detail_requirements"])
        detail["detail_salary"] = await _xpath_text(page, DETAIL_SELECTORS["detail_salary"])
        detail["detail_benefit"] = await _xpath_text(page, DETAIL_SELECTORS["detail_benefit"])
        detail["other_benefits"] = await _xpath_texts(page, DETAIL_SELECTORS["other_benefits"])
        detail["detail_location"] = await _xpath_text(page, DETAIL_SELECTORS["detail_location"])
        detail["working_days"] = await _xpath_text(page, DETAIL_SELECTORS["working_days"])
        detail["job_level"] = await _xpath_text(page, DETAIL_SELECTORS["job_level"])
        detail["education"] = await _xpath_text(page, DETAIL_SELECTORS["education"])
        detail["hiring_quantity"] = await _xpath_text(page, DETAIL_SELECTORS["hiring_quantity"])
        detail["work_mode"] = await _xpath_text(page, DETAIL_SELECTORS["work_mode"])
        detail["company_name"] = await _xpath_text(page, DETAIL_SELECTORS["company_name"])
        detail["company_size"] = await _xpath_text(page, DETAIL_SELECTORS["company_size"])
        detail["company_industry"] = await _xpath_text(page, DETAIL_SELECTORS["company_industry"])

        return detail

    except PlaywrightTimeout:
        print(f"Timeout khi crawl detail: {job_url}")
        return {"url": job_url, "error": "timeout"}
    except Exception as e:
        print(f"Lỗi crawl detail {job_url}: {e}")
        return {"url": job_url, "error": str(e)}

async def crawl_topcv() -> list:
    all_jobs = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=HEADLESS,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            viewport={"width": 1920, "height": 1080},
        )
        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        )

        page = await context.new_page()
        page.set_default_timeout(TIMEOUT_MS)

        current_page = 1
        jobs_crawled = 0

        while True:
            print(f"\nLoading list page #{current_page}...")

            await page.goto(
                BASE_URL if current_page == 1 else page.url,
                wait_until="domcontentloaded",
                timeout=TIMEOUT_MS,
            )
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(3000)

            list_jobs = await extract_job_list_data(page)
            print(f"Found {len(list_jobs)} jobs on page {current_page}")

            for job in list_jobs:
                if MAX_JOBS_PER_RUN > 0 and jobs_crawled >= MAX_JOBS_PER_RUN:
                    break

                all_jobs.append(job)
                jobs_crawled += 1

                if CRAWL_DETAIL and job.get("url"):
                    print(f"Crawling detail: {job['title'][:60]}...")
                    detail = await extract_job_detail_data(page, job["url"])

                    job.update({k: v for k, v in detail.items() if v})

                    print(f"  • [{job.get('job_id')}] {job.get('title')}")
                    print(f"    @ {job.get('company')} | {job.get('salary')} | {job.get('location')}")
                    print(f"    {job.get('url')}\n")

                    await asyncio.sleep(2)

                if MAX_JOBS_PER_RUN > 0 and jobs_crawled >= MAX_JOBS_PER_RUN:
                    break

            if MAX_JOBS_PER_RUN > 0 and jobs_crawled >= MAX_JOBS_PER_RUN:
                break
            if MAX_PAGES > 0 and current_page >= MAX_PAGES:
                break

            next_btn = await page.query_selector(LIST_SELECTORS["next_button"])
            if not next_btn:
                print("No more pages")
                break

            await next_btn.scroll_into_view_if_needed()
            await page.wait_for_timeout(1000)
            await next_btn.click()
            current_page += 1
            await asyncio.sleep(2)

        await browser.close()

    return all_jobs

def save_to_json(jobs: list, filename: str = "topcv_jobs.json") -> None:
    if not jobs:
        print("No data to save")
        return
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(jobs, f, ensure_ascii=False, indent=2)
    print(f"Saved {len(jobs)} jobs → {filename}")

async def main():
    print("Starting TopCV crawler (list + detail)...")
    jobs = await crawl_topcv()

    if jobs:
        save_to_json(jobs)
        print(f"\nDone! Crawled {len(jobs)} jobs")
    else:
        print("\nNo data. Try HEADLESS=False to debug")


if __name__ == "__main__":
    asyncio.run(main())
