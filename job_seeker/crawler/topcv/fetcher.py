import random
import time
from pathlib import Path
from typing import Optional

from curl_cffi import requests as cf
from bs4 import BeautifulSoup

SITE_URL    = "https://www.topcv.vn"
BASE_URL    = "https://www.topcv.vn/tim-viec-lam-cong-nghe-thong-tin-cr257"
IMPERSONATE = "chrome124"

WAIT_MIN = 1.5
WAIT_MAX = 3.5
LOG_DIR  = Path("log")

session = cf.Session(impersonate=IMPERSONATE)
session.headers.update({
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Cache-Control":   "no-cache",
    "Pragma":          "no-cache",
})


def wait():
    time.sleep(random.uniform(WAIT_MIN, WAIT_MAX))


def page_url(page_num: int) -> str:
    if page_num == 1:
        return BASE_URL
    return f"{BASE_URL}?page={page_num}"


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
