from typing import Dict

from .extractor import Extractor, merge_best
from .fetcher import Fetcher
from .utils import (
    clean_text,
    normalize_address,
    normalize_person_name,
    normalize_phone,
    normalize_url,
    normalize_year,
)


class CompanyScrapper:
    def __init__(self):
        self.fetcher = Fetcher()
        self.extractor = Extractor()

    def scrape_company(self, company_name: str, website: str) -> Dict[str, str]:
        website = normalize_url(website)
        row = {
            "company_name": company_name or "",
            "website": website,
            "full_address": "",
            "director_or_founder": "",
            "founded_year": "",
            "email": "",
            "phone": "",
            "source_page": "",
            "scrape_status": "",
        }

        if not website:
            row["scrape_status"] = "invalid_website"
            return row

        home_html, home_err = self.fetcher.get(website)
        if home_err:
            row["scrape_status"] = f"fetch_failed:{self._sanitize_status(home_err)}"
            return row

        best = self.extractor.extract_fields(website, home_html)
        links = self.extractor.discover_relevant_links(website, home_html)

        for link in links:
            html, err = self.fetcher.get(link)
            if err or not html:
                continue
            candidate = self.extractor.extract_fields(link, html)
            best, _ = merge_best(best, candidate)

        row.update(best)
        row["full_address"] = normalize_address(row.get("full_address", ""))
        row["director_or_founder"] = normalize_person_name(row.get("director_or_founder", ""))
        row["founded_year"] = normalize_year(row.get("founded_year", ""))
        row["phone"] = normalize_phone(row.get("phone", ""))
        row["email"] = clean_text(row.get("email", "")).lower()
        if "@" not in row["email"]:
            row["email"] = ""
        for key in row:
            row[key] = clean_text(str(row[key] or ""))

        non_empty = sum(
            1
            for k in ["full_address", "director_or_founder", "founded_year", "email", "phone"]
            if row.get(k)
        )
        row["scrape_status"] = "ok" if non_empty > 0 else "no_data_found"
        return row

    @staticmethod
    def _sanitize_status(msg: str) -> str:
        cleaned = (msg or "").replace("\n", " ").replace("\r", " ").replace(",", ";")
        return cleaned[:140]
