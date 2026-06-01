import random
import re
import time
from datetime import timedelta
from pathlib import Path
from typing import Optional

from curl_cffi import requests as cf
from bs4 import BeautifulSoup

import sys

_root = Path(__file__).resolve().parents[2]
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from src.utils.datetime_utils import now

SITE_URL    = "https://itviec.com"
BASE_URL    = "https://itviec.com/it-jobs"
LOGIN_URL   = "https://itviec.com/sign_in"
IMPERSONATE = "chrome124"

WAIT_MIN = 1.5
WAIT_MAX = 3.5
LOG_DIR = Path("log")
TIMESTAMP_FORMAT = "ISO8601_UTC"

session = cf.Session(impersonate=IMPERSONATE)
session.headers.update({
    "Accept":           "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language":  "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding":  "gzip, deflate, br",
    "Cache-Control":    "no-cache",
    "Pragma":           "no-cache",
})


def wait():
    time.sleep(random.uniform(WAIT_MIN, WAIT_MAX))


def extract_job_id(url: str) -> Optional[str]:
    if not url:
        return None
    clean = url.split("?")[0].split("#")[0].rstrip("/")
    slug  = clean.split("/")[-1]
    m     = re.search(r"-([A-Za-z]?\d+)$", slug)
    return f"itviec-{m.group(1)}" if m else f"itviec-{slug}"


def list_page_url(page_num: int) -> str:
    if page_num <= 1:
        return f"{BASE_URL}?sort=new"
    return f"{BASE_URL}?sort=new&page={page_num}"


def compute_posted_date(posted_at_text: str) -> Optional[str]:
    dt = parse_relative_time(posted_at_text)
    return dt.isoformat(timespec="seconds") if dt else None


def parse_relative_time(text: str):
    ref = now()
    text = (text or "").lower()
    if "just now" in text or "today" in text or "hôm nay" in text:
        return ref
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
            return ref - int(m.group(1)) * delta
    return None


def get_page(url: str, referer: str = BASE_URL) -> Optional[BeautifulSoup]:
    try:
        session.headers.update({"Referer": referer})
        r = session.get(url, timeout=30)
        print(f"  GET {url} → {r.status_code}")
        if r.status_code != 200:
            print(f"  Status {r.status_code}")
            return None
        return BeautifulSoup(r.text, "lxml")
    except Exception as e:
        print(f"  Error: {e}")
        return None
