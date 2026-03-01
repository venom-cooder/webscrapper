import re
from typing import Dict, List, Optional
from urllib.parse import parse_qs, quote_plus, urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup

from .extractor import Extractor
from .fetcher import Fetcher
from .utils import COLUMNS, clean_text, normalize_phone, normalize_url

WIKIDATA_API = "https://www.wikidata.org/w/api.php"
INDIA_QID = "Q668"
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_RE = re.compile(r"(?:\+?\d[\d\s().-]{7,}\d)")
CONTACT_LINK_KEYWORDS = (
    "contact",
    "contact-us",
    "support",
    "help",
    "about",
    "about-us",
    "reach",
    "grievance",
    "customer",
    "legal",
)
BLOCKED_WEBSITE_DOMAINS = (
    "wikipedia.org",
    "wikidata.org",
    "linkedin.com",
    "facebook.com",
    "instagram.com",
    "twitter.com",
    "x.com",
    "youtube.com",
    "crunchbase.com",
)


def _ua_headers() -> Dict[str, str]:
    return {"User-Agent": "Mozilla/5.0 (CompanyDataBot/1.0)"}


def _safe_text(value) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    out = clean_text(str(value))
    return "" if out.lower() == "nan" else out


def _domain(url: str) -> str:
    try:
        d = urlparse(url).netloc.lower().replace("www.", "")
        return d
    except Exception:
        return ""


def _is_blocked_website(url: str) -> bool:
    d = _domain(url)
    return any(d.endswith(x) for x in BLOCKED_WEBSITE_DOMAINS)


def _split_founders(value: str) -> str:
    value = _safe_text(value)
    if not value:
        return ""
    parts = [clean_text(x) for x in re.split(r",|/| and ", value) if clean_text(x)]
    # Keep at most first two founders to keep cell clean.
    return ", ".join(parts[:2])


def _wikidata_search(session: requests.Session, query: str) -> Optional[dict]:
    params = {
        "action": "wbsearchentities",
        "search": query,
        "language": "en",
        "format": "json",
        "limit": 8,
    }
    data = session.get(WIKIDATA_API, params=params, headers=_ua_headers(), timeout=20).json()
    hits = data.get("search", [])
    if not hits:
        return None

    def score(hit: dict) -> int:
        label = _safe_text(hit.get("label")).lower()
        desc = _safe_text(hit.get("description")).lower()
        s = 0
        q = query.lower()
        if label == q:
            s += 6
        if q in label:
            s += 4
        if "indian" in desc:
            s += 4
        if any(k in desc for k in ("startup", "company", "technology", "fintech", "commerce")):
            s += 2
        return s

    hits = sorted(hits, key=score, reverse=True)
    best = hits[0]
    best_score = score(best)
    if best_score < 4:
        return None
    desc = _safe_text(best.get("description")).lower()
    # Guard against obvious non-company entities (films, songs, places, persons).
    if any(x in desc for x in ("film", "television", "tv series", "song", "village", "district", "cricketer", "actor")):
        return None
    if not any(k in desc for k in ("company", "startup", "app", "platform", "service", "business", "technology", "fintech")):
        return None
    return best


def _entity(session: requests.Session, qid: str) -> Optional[dict]:
    url = f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json"
    data = session.get(url, headers=_ua_headers(), timeout=20).json()
    return data.get("entities", {}).get(qid)


def _claim_entity_ids(entity: dict, prop: str) -> List[str]:
    out: List[str] = []
    for claim in entity.get("claims", {}).get(prop, []):
        dv = claim.get("mainsnak", {}).get("datavalue", {})
        if dv.get("type") == "wikibase-entityid":
            out.append("Q" + str(dv["value"]["numeric-id"]))
    return out


def _claim_strings(entity: dict, prop: str) -> List[str]:
    out: List[str] = []
    for claim in entity.get("claims", {}).get(prop, []):
        dv = claim.get("mainsnak", {}).get("datavalue", {})
        t = dv.get("type")
        if t == "string":
            out.append(str(dv.get("value", "")))
        elif t == "time":
            out.append(str(dv.get("value", {}).get("time", "")))
    return out


def _qid_labels(session: requests.Session, qids: List[str]) -> Dict[str, str]:
    if not qids:
        return {}
    params = {
        "action": "wbgetentities",
        "ids": "|".join(sorted(set(qids))),
        "languages": "en",
        "format": "json",
    }
    data = session.get(WIKIDATA_API, params=params, headers=_ua_headers(), timeout=20).json()
    labels: Dict[str, str] = {}
    for qid, ent in data.get("entities", {}).items():
        labels[qid] = _safe_text(ent.get("labels", {}).get("en", {}).get("value", ""))
    return labels


def _year_from_time(values: List[str]) -> str:
    for v in values:
        m = re.search(r"([12][0-9]{3})", v or "")
        if m:
            year = int(m.group(1))
            if 1800 <= year <= 2025:
                return str(year)
    return ""


def _country_is_india(entity: dict) -> bool:
    countries = _claim_entity_ids(entity, "P17")
    return INDIA_QID in countries


def _extract_contact_from_website(url: str) -> Dict[str, str]:
    if not url:
        return {"email": "", "phone": "", "source_page": ""}
    fetcher = Fetcher(timeout=4, max_retries=0)
    extractor = Extractor(max_extra_pages=2)
    html, err = fetcher.get(url)
    if err or not html:
        return {"email": "", "phone": "", "source_page": ""}

    def parse_contacts(page_url: str, page_html: str) -> Dict[str, str]:
        soup = BeautifulSoup(page_html, "html.parser")
        emails: List[str] = []
        phones: List[str] = []

        for tag in soup.select("a[href^='mailto:']"):
            href = _safe_text(tag.get("href", ""))
            e = _safe_text(href.replace("mailto:", "").split("?")[0]).lower()
            if EMAIL_RE.fullmatch(e):
                emails.append(e)

        for tag in soup.select("a[href^='tel:']"):
            href = _safe_text(tag.get("href", ""))
            p = normalize_phone(href.replace("tel:", ""))
            if p:
                phones.append(p)

        visible = clean_text(soup.get_text(" ", strip=True))
        emails.extend([x.lower() for x in EMAIL_RE.findall(visible)])
        for cand in PHONE_RE.findall(visible):
            p = normalize_phone(cand)
            if p:
                phones.append(p)

        def best_email(values: List[str]) -> str:
            uniq = []
            for v in values:
                if v not in uniq:
                    uniq.append(v)
            if not uniq:
                return ""
            preferred = [v for v in uniq if not any(x in v for x in ("noreply", "no-reply", "donotreply", "example"))]
            return preferred[0] if preferred else uniq[0]

        def best_phone(values: List[str]) -> str:
            uniq = []
            for v in values:
                if v not in uniq:
                    uniq.append(v)
            if not uniq:
                return ""
            india_pref = [v for v in uniq if v.startswith("+91") or (v.isdigit() and len(v) == 10 and v[0] in ("6", "7", "8", "9"))]
            return india_pref[0] if india_pref else uniq[0]

        return {"email": best_email(emails), "phone": best_phone(phones), "source_page": page_url}

    fields = parse_contacts(url, html)
    links = extractor.discover_relevant_links(url, html)
    soup_home = BeautifulSoup(html, "html.parser")
    for a in soup_home.find_all("a", href=True):
        href = _safe_text(a.get("href"))
        text = _safe_text(a.get_text(" ")).lower()
        full = urljoin(url, href)
        if _domain(full) != _domain(url):
            continue
        if any(k in full.lower() or k in text for k in CONTACT_LINK_KEYWORDS):
            if full not in links:
                links.append(full)
    for guessed in ("/contact", "/contact-us", "/support", "/about"):
        links.append(urljoin(url, guessed))

    seen = set()
    for link in links[:5]:
        if link in seen:
            continue
        seen.add(link)
        page_html, page_err = fetcher.get(link)
        if page_err or not page_html:
            continue
        cand = parse_contacts(link, page_html)
        if not fields.get("email") and cand.get("email"):
            fields["email"] = cand["email"]
            fields["source_page"] = cand.get("source_page", fields.get("source_page", url))
        if not fields.get("phone") and cand.get("phone"):
            fields["phone"] = cand["phone"]
            fields["source_page"] = cand.get("source_page", fields.get("source_page", url))
        if fields.get("email") and fields.get("phone"):
            break

    return {
        "email": _safe_text(fields.get("email", "")).lower(),
        "phone": normalize_phone(fields.get("phone", "")),
        "source_page": _safe_text(fields.get("source_page", url)) or url,
    }


def _website_from_wikipedia(session: requests.Session, company_name: str) -> str:
    try:
        params = {
            "action": "query",
            "list": "search",
            "srsearch": f"{company_name} Indian startup company",
            "format": "json",
            "srlimit": 3,
        }
        data = session.get(
            "https://en.wikipedia.org/w/api.php",
            params=params,
            headers=_ua_headers(),
            timeout=20,
        ).json()
    except Exception:
        return ""
    hits = data.get("query", {}).get("search", [])
    for hit in hits:
        title = _safe_text(hit.get("title"))
        if not title:
            continue
        try:
            page_url = f"https://en.wikipedia.org/wiki/{title.replace(' ', '_')}"
            html = session.get(page_url, headers=_ua_headers(), timeout=20).text
            soup = BeautifulSoup(html, "html.parser")
            infobox = soup.select_one("table.infobox")
            if not infobox:
                continue
            page_text = clean_text(soup.get_text(" ", strip=True)).lower()
            if "india" not in page_text and "indian" not in page_text:
                continue
            for tr in infobox.select("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if not th or not td:
                    continue
                if "website" not in _safe_text(th.get_text(" ", strip=True)).lower():
                    continue
                a = td.find("a", href=True)
                if a and a.get("href"):
                    website = normalize_url(_safe_text(a.get("href")))
                    if website and not _is_blocked_website(website):
                        return website
                txt = _safe_text(td.get_text(" ", strip=True))
                m = re.search(r"(https?://[^\s]+|www\.[^\s]+)", txt)
                if m:
                    website = normalize_url(m.group(1))
                    if website and not _is_blocked_website(website):
                        return website
        except Exception:
            continue
    return ""


def _website_from_duckduckgo(session: requests.Session, company_name: str) -> str:
    try:
        q = quote_plus(f"{company_name} official website")
        html = session.get(
            f"https://duckduckgo.com/html/?q={q}",
            headers=_ua_headers(),
            timeout=6,
        ).text
    except Exception:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.select("a.result__a, a[href]"):
        href = _safe_text(a.get("href"))
        if not href:
            continue
        # DuckDuckGo often wraps URL in uddg query param.
        if "duckduckgo.com/l/?" in href:
            parsed = urlparse(href)
            uddg = parse_qs(parsed.query).get("uddg", [""])[0]
            href = uddg or href
        website = normalize_url(href)
        if not website:
            continue
        if _is_blocked_website(website):
            continue
        # Basic sanity: should not be a deep article URL as first pick.
        d = _domain(website)
        if not d or "." not in d:
            continue
        return website
    return ""


def generate_indian_startups_df(limit: int = 100) -> pd.DataFrame:
    session = requests.Session()
    html = session.get(
        "https://en.wikipedia.org/wiki/List_of_unicorn_startup_companies",
        headers=_ua_headers(),
        timeout=30,
    ).text
    tables = pd.read_html(html)
    unicorns = tables[2]
    india = unicorns[
        unicorns["Country/ countries"].astype(str).str.contains("India", case=False, na=False)
    ].copy()

    rows: List[dict] = []
    for _, rec in india.iterrows():
        company_name = _safe_text(rec.get("Company", ""))
        founder_fallback = _split_founders(rec.get("Founder(s)", ""))
        if not company_name:
            continue

        qid = ""
        website = ""
        founded_year = ""
        director_or_founder = founder_fallback
        full_address = ""
        source_page = ""
        email = ""
        phone = ""

        hit = _wikidata_search(session, company_name)
        if hit and hit.get("id"):
            qid = hit["id"]
            ent = _entity(session, qid)
            if ent:
                desc = _safe_text(hit.get("description")).lower()
                if _country_is_india(ent) or "indian" in desc:
                    website = normalize_url((_claim_strings(ent, "P856") or [""])[0])
                    if website and _is_blocked_website(website):
                        website = ""
                    founded_year = _year_from_time(_claim_strings(ent, "P571"))

                    founder_qids = _claim_entity_ids(ent, "P112")
                    founder_labels = _qid_labels(session, founder_qids)
                    founders = [founder_labels.get(q, "") for q in founder_qids if founder_labels.get(q, "")]
                    if founders:
                        director_or_founder = _safe_text(", ".join(founders[:2]))

                    hq_qids = _claim_entity_ids(ent, "P159")
                    hq_labels = _qid_labels(session, hq_qids)
                    hq = _safe_text(", ".join([hq_labels.get(q, "") for q in hq_qids if hq_labels.get(q, "")]))
                    if hq:
                        full_address = hq if "india" in hq.lower() else f"{hq}, India"

                    if website:
                        contact = _extract_contact_from_website(website)
                        email = _safe_text(contact.get("email", ""))
                        phone = _safe_text(contact.get("phone", ""))
                        source_page = _safe_text(contact.get("source_page", ""))

        if not website:
            website = _website_from_wikipedia(session, company_name)
        if not website:
            website = _website_from_duckduckgo(session, company_name)
        if website and not (email and phone):
            contact = _extract_contact_from_website(website)
            if not email:
                email = _safe_text(contact.get("email", ""))
            if not phone:
                phone = _safe_text(contact.get("phone", ""))
            if not source_page:
                source_page = _safe_text(contact.get("source_page", ""))

        if not source_page:
            source_page = f"https://www.wikidata.org/wiki/{qid}" if qid else "https://en.wikipedia.org/wiki/List_of_unicorn_startup_companies"

        row = {
            "company_name": company_name,
            "website": website,
            "full_address": full_address,
            "director_or_founder": director_or_founder,
            "founded_year": founded_year,
            "email": email,
            "phone": phone,
            "source_page": source_page,
            "scrape_status": "ok",
        }
        filled = sum(1 for k in ["website", "full_address", "director_or_founder", "founded_year", "email", "phone"] if _safe_text(row.get(k, "")))
        if filled < 3:
            row["scrape_status"] = "partial"
        rows.append({k: _safe_text(row.get(k, "")) for k in COLUMNS})
        if len(rows) >= limit:
            break

    return pd.DataFrame(rows, columns=COLUMNS).fillna("")
