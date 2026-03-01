from typing import Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; CompanyDataBot/1.0; +https://example.com/bot)"
}


class Fetcher:
    def __init__(self, timeout: int = 4, max_retries: int = 0):
        self.timeout = timeout
        self.session = requests.Session()
        retry = Retry(
            total=max_retries,
            read=max_retries,
            connect=max_retries,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET", "HEAD"]),
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def get(self, url: str) -> Tuple[Optional[str], Optional[str]]:
        try:
            response = self.session.get(
                url,
                timeout=self.timeout,
                headers=DEFAULT_HEADERS,
                allow_redirects=True,
            )
            response.raise_for_status()
            content_type = response.headers.get("Content-Type", "")
            if "text/html" not in content_type and "application/xhtml+xml" not in content_type:
                return None, f"unsupported_content_type:{content_type}"
            return response.text, None
        except Exception as exc:
            return None, str(exc)
