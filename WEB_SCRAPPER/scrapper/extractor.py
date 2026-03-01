import json
from typing import Dict, List, Tuple
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from .utils import (
    clean_text,
    find_director_or_founder,
    find_email,
    find_founded_year,
    find_phone,
    normalize_address,
    normalize_person_name,
    normalize_phone,
    normalize_year,
    root_domain,
)

CONTACT_KEYWORDS = ("contact", "about", "team", "leadership", "company")


class Extractor:
    def __init__(self, max_extra_pages: int = 3):
        self.max_extra_pages = max_extra_pages

    def discover_relevant_links(self, base_url: str, html: str) -> List[str]:
        soup = BeautifulSoup(html, "html.parser")
        links = []
        base_domain = root_domain(base_url)

        for tag in soup.find_all("a", href=True):
            href = tag.get("href", "").strip()
            anchor = clean_text(tag.get_text(" ")).lower()
            if not href:
                continue
            full = urljoin(base_url, href)
            parsed = urlparse(full)
            if parsed.scheme not in ("http", "https"):
                continue
            if root_domain(full) != base_domain:
                continue
            content = f"{full.lower()} {anchor}"
            if any(k in content for k in CONTACT_KEYWORDS):
                if full not in links:
                    links.append(full)
            if len(links) >= self.max_extra_pages:
                break

        return links

    def extract_fields(self, page_url: str, html: str) -> Dict[str, str]:
        soup = BeautifulSoup(html, "html.parser")
        visible_text = clean_text(soup.get_text(" ", strip=True))
        json_ld = self._extract_json_ld(soup)

        address = self._extract_address(soup, visible_text, json_ld)
        founder = self._extract_director_or_founder(visible_text, json_ld)
        year = self._extract_founded_year(visible_text, json_ld)
        email = self._extract_email(soup, visible_text)
        phone = self._extract_phone(soup, visible_text)

        return {
            "full_address": address,
            "director_or_founder": founder,
            "founded_year": year,
            "email": email,
            "phone": phone,
            "source_page": page_url,
        }

    def _extract_json_ld(self, soup: BeautifulSoup) -> List[dict]:
        items: List[dict] = []
        for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
            raw = (script.string or script.get_text() or "").strip()
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except Exception:
                continue
            if isinstance(data, list):
                items.extend([x for x in data if isinstance(x, dict)])
            elif isinstance(data, dict):
                if isinstance(data.get("@graph"), list):
                    items.extend([x for x in data["@graph"] if isinstance(x, dict)])
                else:
                    items.append(data)
        return items

    def _extract_email(self, soup: BeautifulSoup, visible_text: str) -> str:
        for tag in soup.select("a[href^='mailto:']"):
            href = (tag.get("href") or "").strip()
            email = href.replace("mailto:", "").split("?")[0].strip()
            email = find_email(email)
            if email:
                return email
        return find_email(visible_text)

    def _extract_phone(self, soup: BeautifulSoup, visible_text: str) -> str:
        for tag in soup.select("a[href^='tel:']"):
            href = (tag.get("href") or "").strip()
            phone = normalize_phone(href.replace("tel:", ""))
            if phone:
                return phone
        return find_phone(visible_text)

    def _extract_founded_year(self, visible_text: str, json_ld: List[dict]) -> str:
        for item in json_ld:
            for key in ("foundingDate", "foundingdate"):
                if key in item:
                    year = normalize_year(clean_text(str(item[key]))[:4])
                    if year:
                        return year
        return normalize_year(find_founded_year(visible_text))

    def _extract_director_or_founder(self, visible_text: str, json_ld: List[dict]) -> str:
        for item in json_ld:
            for key in ("founder", "employee", "founders"):
                value = item.get(key)
                if isinstance(value, dict):
                    name = normalize_person_name(clean_text(str(value.get("name", ""))))
                    if name:
                        return name
                if isinstance(value, list):
                    for entry in value:
                        if isinstance(entry, dict):
                            name = normalize_person_name(clean_text(str(entry.get("name", ""))))
                            if name:
                                return name
                        elif isinstance(entry, str):
                            name = normalize_person_name(entry)
                            if name:
                                return name
                if isinstance(value, str):
                    name = normalize_person_name(value)
                    if name:
                        return name
        return normalize_person_name(find_director_or_founder(visible_text))

    def _extract_address(self, soup: BeautifulSoup, visible_text: str, json_ld: List[dict]) -> str:
        for item in json_ld:
            addr = item.get("address")
            if isinstance(addr, dict):
                parts = [
                    clean_text(str(addr.get("streetAddress", ""))),
                    clean_text(str(addr.get("addressLocality", ""))),
                    clean_text(str(addr.get("addressRegion", ""))),
                    clean_text(str(addr.get("postalCode", ""))),
                    clean_text(str(addr.get("addressCountry", ""))),
                ]
                candidate = ", ".join([p for p in parts if p])
                candidate = normalize_address(candidate)
                if candidate:
                    return candidate
            if isinstance(addr, str):
                candidate = normalize_address(addr)
                if candidate:
                    return candidate

        address_tag = soup.find("address")
        if address_tag:
            candidate = normalize_address(address_tag.get_text(" ", strip=True))
            if candidate:
                return candidate

        lines = [clean_text(x) for x in soup.get_text("\n").splitlines() if clean_text(x)]
        keywords = (
            "street",
            "st",
            "road",
            "rd",
            "avenue",
            "ave",
            "blvd",
            "lane",
            "city",
            "india",
            "usa",
            "uk",
            "suite",
            "floor",
            "zip",
            "postal",
        )
        for line in lines:
            if any(k in line.lower() for k in keywords) and any(ch.isdigit() for ch in line):
                candidate = normalize_address(line)
                if candidate:
                    return candidate

        return ""


def merge_best(primary: Dict[str, str], secondary: Dict[str, str]) -> Tuple[Dict[str, str], int]:
    score = 0
    merged = dict(primary)
    for key in ["full_address", "director_or_founder", "founded_year", "email", "phone"]:
        if not merged.get(key) and secondary.get(key):
            merged[key] = secondary[key]
            merged["source_page"] = secondary.get("source_page", merged.get("source_page", ""))
            score += 1
    return merged, score
