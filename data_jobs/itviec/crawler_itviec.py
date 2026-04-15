import scrapy
import json
import re
import asyncio
from datetime import datetime, timedelta
from typing import Optional
from scrapy.crawler import CrawlerProcess
from scrapy_playwright.page import PageMethod
import os
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://itviec.com/it-jobs"
HEADLESS = True
MAX_PAGES = 2
MAX_JOBS_PER_RUN = 0
OUTPUT_FILE = "data/itviec_jobs.json"

ITVIEC_EMAIL = os.getenv("ITVIEC_EMAIL")
ITVIEC_PASSWORD = os.getenv("ITVIEC_PASSWORD")

_JOB_ID_RE = re.compile(r"-([A-Za-z]?\d+)$")

def _extract_job_id(url: str) -> Optional[str]:
    if not url: return None
    clean = url.split("?")[0].split("#")[0].rstrip("/")
    slug = clean.split("/")[-1] if "/" in clean else clean
    m = _JOB_ID_RE.search(slug)
    return f"{m.group(1)}" if m else f"itviec-{slug}"

def _compute_posted_time(posted_at_text: str, crawled_dt: datetime) -> Optional[str]:
    if not crawled_dt: return None
    text = (posted_at_text or "").lower()
    if "just now" in text or "today" in text:
        return crawled_dt.strftime("%Y-%m-%d %H:%M:%S")
        
    patterns = [
        (re.compile(r"(\d+)\s+hour", re.I), "hours", 0),
        (re.compile(r"(\d+)\s+day", re.I), "days", 1),
        (re.compile(r"(\d+)\s+week", re.I), "weeks", 7),
        (re.compile(r"(\d+)\s+month", re.I), "months", 30),
    ]
    for pattern, unit, days_per_unit in patterns:
        m = pattern.search(text)
        if m:
            n = int(m.group(1))
            delta = timedelta(hours=n) if unit == "hours" else timedelta(days=n * days_per_unit)
            target_dt = crawled_dt - delta
            return target_dt.strftime("%Y-%m-%d %H:%M:%S")
    return None

raw_jobs_result = []

class ItViecSpider(scrapy.Spider):
    name = "itviec_spider"
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spawned_count = 0

    custom_settings = {
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        "DOWNLOAD_HANDLERS": {
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        "PLAYWRIGHT_LAUNCH_OPTIONS": {
            "headless": HEADLESS,
            "args": ["--disable-blink-features=AutomationControlled"]
        },
        
        "PLAYWRIGHT_BROWSER_TYPE": "chromium",
        "PLAYWRIGHT_CONTEXTS": {
            "default": {
                "ignore_https_errors": True,
            }
        },
        "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "CONCURRENT_REQUESTS": 2,
        "DOWNLOAD_DELAY": 2,
        "CLOSESPIDER_ITEMCOUNT": MAX_JOBS_PER_RUN if MAX_JOBS_PER_RUN > 0 else 0,
        "LOG_LEVEL": "INFO",
    }
    
    def start_requests(self):
        yield scrapy.Request(
            url="https://itviec.com/sign_in",
            meta={
                "playwright": True,
                "playwright_include_page": True,
                "playwright_context": "default",        
                "playwright_page_methods": [
                    PageMethod("wait_for_selector", "input[name='user[email]']"),
                ],
            },
            callback=self.do_login,
            dont_filter=True,
        )

    async def do_login(self, response):
        page = response.meta["playwright_page"]

        await page.fill("input[name='user[email]']", ITVIEC_EMAIL)
        await page.fill("input[name='user[password]']", ITVIEC_PASSWORD)
        await page.click("button.ibtn-primary[type='submit']")

        # Chờ load xong (không dùng networkidle)
        await page.wait_for_load_state("load", timeout=15000)

        current_url = page.url
        self.logger.info(f"After login URL: {current_url}")

        if "sign_in" in current_url:
            self.logger.error("Login failed! Check credentials or CAPTCHA.")
            return

        self.logger.info("Login successful!")

        for p in range(1, MAX_PAGES + 1):
            url = f"{BASE_URL}?page={p}"
            yield scrapy.Request(
                url=url,
                meta={
                    "playwright": True,
                    "playwright_context": "default",
                    "playwright_page_methods": [
                        PageMethod("wait_for_selector", "h3[data-search--job-selection-target='jobTitle']"),
                    ],
                },
                callback=self.parse_list,
                dont_filter=True,
            )

    async def parse_list(self, response):
        job_elements = response.css("h3[data-search--job-selection-target='jobTitle']")
        for el in job_elements:
            if MAX_JOBS_PER_RUN > 0 and self.spawned_count >= MAX_JOBS_PER_RUN:
                break

            title = el.css("::text").get(default="").strip()
            job_url = el.css("::attr(data-url)").get()

            if job_url:
                self.spawned_count += 1 
                full_url = response.urljoin(job_url)
                yield scrapy.Request(
                    url=full_url,
                    meta={
                        "playwright": True,
                        "playwright_context": "default", 
                        "playwright_page_methods": [
                            PageMethod("evaluate", "window.scrollTo(0, 500)"),
                            PageMethod("wait_for_selector", ".job-show-info", timeout=20000),
                        ],
                        "job_title": title
                    },
                    callback=self.parse_detail
                )

    async def parse_detail(self, response):
        job_header = response.xpath("//div[contains(@class, 'job-show-info')]")
        info_container = job_header.xpath("./div[contains(@class, 'imb-3')]")
        content_section = response.xpath("//section[contains(@class, 'job-content')]")
        employer_section = response.xpath("//section[contains(@class, 'job-show-employer-info')]")
        
        raw_posted_at = info_container.xpath(".//svg[use[contains(@href, 'clock')]]/following-sibling::span/text()").get(default="").strip().replace("Posted", "").strip()
        crawled_now = datetime.utcnow()

        salary_raw = (
            response.xpath("string(//div[@class='job-header-info']//div[contains(@class,'salary')])")
            .get(default="").strip()
        )
        salary = salary_raw if salary_raw and "sign in" not in salary_raw.lower() else "unavailable"

        item = {
            "job_id": _extract_job_id(response.url),
            "job_url": response.url,
            "title": response.meta.get("job_title", "N/A"),
            "salary": response.xpath("string(//div[@class='job-header-info']//div[contains(@class, 'salary')])").get(default="").strip(),
            "company_name": response.css("div.employer-name::text").get(default="").strip(),
            "location": info_container.xpath(".//svg[use[contains(@href, 'map-pin')]]/following-sibling::span/text()").get(default="").strip(),
            "work_mode": info_container.xpath(".//div[contains(@class, 'preview-header-item')]//span[not(contains(text(), 'Posted'))]/text()").get(default="").strip(),
            "posted_datetime": _compute_posted_time(raw_posted_at, crawled_now),
            "skills": [s.strip() for s in info_container.xpath(".//div[contains(text(), 'Skills:')]/following-sibling::div/a/text()").getall()],
            "job_expertise": [e.strip() for e in info_container.xpath(".//div[contains(text(), 'Job Expertise:')]/following-sibling::div/a/text()").getall()],
            "job_domains": [d.strip() for d in info_container.xpath(".//div[contains(text(), 'Job Domain:')]/following-sibling::div//div[contains(@class, 'itag')]/text()").getall() if d.strip()],
            "top_reasons": [r.strip() for r in content_section.xpath(".//div[h2[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'reasons')]]//li/text()").getall()],
            "description": "\n".join([t.strip() for t in content_section.xpath(".//div[h2[contains(., 'Job description')]]//*[not(self::h2)]//text()").getall() if t.strip()]),
            "requirements": "\n".join([t.strip() for t in content_section.xpath(".//div[h2[contains(., 'skills and experience')]]//*[not(self::h2)]//text()").getall() if t.strip()]),
            "benefits": "\n".join([t.strip() for t in content_section.xpath(".//div[h2[contains(., 'love working here')]]//*[not(self::h2)]//text()").getall() if t.strip()]),
            "company_full_name": employer_section.xpath(".//div[contains(@class, 'imt-5')]/p/text()").get(default="").strip(),
            "company_type": employer_section.xpath(".//div[div[contains(text(), 'Company type')]]/div[contains(@class, 'text-end')]/text()").get(default="").strip(),
            "company_industry": "".join(employer_section.xpath(".//div[div[contains(text(), 'Company industry')]]/div[contains(@class, 'text-end')]//text()").getall()).strip(),
            "company_size": " ".join(employer_section.xpath(".//div[div[contains(text(), 'Company size')]]/div[contains(@class, 'text-end')]//text()").getall()).strip(),
            "country": employer_section.xpath(".//div[div[contains(text(), 'Country')]]//span/text()").get(default="").strip(),
            "working_days": employer_section.xpath(".//div[div[contains(text(), 'Working days')]]/div[contains(@class, 'text-end')]/text()").get(default="").strip(),
            "overtime_policy": employer_section.xpath(".//div[div[contains(text(), 'Overtime policy')]]/div[contains(@class, 'text-end')]/text()").get(default="").strip(),
        }
        
        raw_jobs_result.append(item)
        yield item

if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(ItViecSpider)
    process.start()
    
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(raw_jobs_result, f, ensure_ascii=False, indent=2)
    print(f"\nDone! Saved {len(raw_jobs_result)} jobs to {OUTPUT_FILE}")
