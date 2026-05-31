import time
from typing import Optional

from bs4 import BeautifulSoup

from .fetcher import session, LOGIN_URL, SITE_URL

EMAIL    = "nguyenminhnhat474@gmail.com"
PASSWORD = "CuM@ng1971982"


def get_csrf_token(soup: BeautifulSoup) -> Optional[str]:
    meta = soup.find("meta", {"name": "csrf-token"})
    if meta and meta.get("content"):
        return meta["content"]
    hidden = soup.find("input", {"name": "authenticity_token"})
    if hidden and hidden.get("value"):
        return hidden["value"]
    return None


def login() -> bool:
    print("Login...")

    r = session.get(LOGIN_URL, timeout=30)
    print(f"  Login page status: {r.status_code}")

    if r.status_code != 200:
        print("  Cannot load login page")
        return False

    soup = BeautifulSoup(r.text, "lxml")

    if "challenge" in r.text.lower() or "xác minh" in r.text.lower():
        print("  Cloudflare challenge — retrying...")
        time.sleep(3)
        r = session.get(LOGIN_URL, timeout=30)
        soup = BeautifulSoup(r.text, "lxml")

    csrf = get_csrf_token(soup)
    print(f"  CSRF token: {csrf[:20]}..." if csrf else "  No CSRF token found")

    payload = {
        "authenticity_token": csrf or "",
        "user[email]":        EMAIL,
        "user[password]":     PASSWORD,
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

    if "sign_in" in r2.url and r2.status_code == 200:
        soup2 = BeautifulSoup(r2.text, "lxml")
        error = soup2.select_one(".alert-danger, .alert.alert-error, [class*='error']")
        if error:
            print(f"  Login error: {error.get_text(strip=True)}")
        else:
            print("  Still on sign_in — possibly wrong credentials or blocked")
        return False

    cookies = dict(session.cookies)
    has_session = any(k in cookies for k in ["_itviec_session", "_session_id", "remember_user_token"])
    print(f"  Cookies: {list(cookies.keys())}")

    if has_session or "sign_in" not in r2.url:
        print("  Login successful!")
        return True

    print("  Login failed")
    return False
