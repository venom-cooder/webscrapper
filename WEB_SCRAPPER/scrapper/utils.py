import re
from urllib.parse import urlparse

COLUMNS = [
    "company_name",
    "website",
    "full_address",
    "director_or_founder",
    "founded_year",
    "email",
    "phone",
    "source_page",
    "scrape_status",
]

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_RE = re.compile(r"(?:\+?\d[\d\s().-]{7,}\d)")
YEAR_CONTEXT_RE = re.compile(
    r"\b(?:founded|established|since|started|incorporated|formed)\D{0,30}(18\d{2}|19\d{2}|20[0-2]\d)\b",
    flags=re.IGNORECASE,
)
PERSON_ROLE_PATTERNS = [
    re.compile(
        r"(?i)\b(?:founder|co-founder|director|managing director|ceo|chief executive officer|chairman|chairperson)\b"
        r"[^A-Za-z]{0,24}([A-Z][A-Za-z'.-]+(?:\s+[A-Z][A-Za-z'.-]+){1,3})"
    ),
    re.compile(
        r"\b([A-Z][A-Za-z'.-]+(?:\s+[A-Z][A-Za-z'.-]+){1,3})\s*,\s*"
        r"(?i:(?:founder|co-founder|director|ceo|chief executive officer|managing director|chairman|chairperson))\b"
    ),
]


def normalize_url(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    if not url.startswith(("http://", "https://")):
        url = f"https://{url}"
    return url


def root_domain(url: str) -> str:
    host = urlparse(url).netloc.lower().strip()
    return host.replace("www.", "")


def pick_first(values):
    for v in values:
        if v:
            return v
    return ""


def clean_text(text: str) -> str:
    text = (text or "").strip()
    return re.sub(r"\s+", " ", text)


def find_email(text: str) -> str:
    m = EMAIL_RE.search(text or "")
    if not m:
        return ""
    email = m.group(0).strip().lower()
    if email.endswith((".png", ".jpg", ".jpeg", ".svg")):
        return ""
    return email


def find_phone(text: str) -> str:
    if not text:
        return ""
    candidates = PHONE_RE.findall(text)
    for c in candidates:
        normalized = normalize_phone(c)
        if normalized:
            return normalized
    return ""


def find_founded_year(text: str) -> str:
    text = text or ""
    m = YEAR_CONTEXT_RE.search(text)
    if m:
        return m.group(1)
    return ""


def find_director_or_founder(text: str) -> str:
    text = text or ""
    for pattern in PERSON_ROLE_PATTERNS:
        m = pattern.search(text)
        if m:
            return clean_text(m.group(1))
    return ""


def normalize_phone(value: str) -> str:
    candidate = clean_text(value)
    if not candidate:
        return ""
    digits = re.sub(r"\D", "", candidate)
    if len(digits) < 10 or len(digits) > 15:
        return ""
    if digits in ("1234567890", "0123456789", "0000000000"):
        return ""
    if len(set(digits)) <= 2:
        return ""
    seq = "01234567890123456789"
    rev = seq[::-1]
    if digits in seq or digits in rev:
        return ""
    if "123456789" in digits or "987654321" in digits:
        return ""
    # Reject obvious date-like forms such as 10-25-2023.
    if re.search(r"\b\d{1,2}[-/]\d{1,2}[-/]\d{2,4}\b", candidate):
        return ""
    if candidate.count("-") >= 2 and len(digits) <= 10 and candidate.startswith(("20", "19")):
        return ""
    if candidate.startswith("+"):
        return "+" + digits
    return digits


def normalize_year(value: str) -> str:
    value = clean_text(value)
    if not value:
        return ""
    if not re.fullmatch(r"(18\d{2}|19\d{2}|20[0-2]\d)", value):
        return ""
    # Strict cap helps avoid "latest report year" being misread as founded year.
    if int(value) > 2021:
        return ""
    return value


def normalize_address(value: str) -> str:
    value = clean_text(value)
    if not value:
        return ""
    if len(value) < 12 or len(value) > 140:
        return ""
    lowered = value.lower()
    junk_tokens = ("cookie", "javascript", "privacy", "terms", "menu", "learn more", "subscribe")
    if any(t in lowered for t in junk_tokens):
        return ""
    # Address should usually include separators and location hints.
    if "," not in value:
        return ""
    if not re.search(r"\d", value):
        return ""
    # Reject promotional/news sentence-like fragments.
    sentence_tokens = (
        "launches",
        "investor",
        "performance",
        "reports",
        "generated",
        "mission",
        "trends",
        "worldwide",
        "smoking kills",
        "why ",
        "read more",
        "learn more",
        "our ",
        "we ",
    )
    if any(tok in lowered for tok in sentence_tokens):
        return ""
    location_tokens = (
        "street",
        "st",
        "road",
        "rd",
        "avenue",
        "ave",
        "blvd",
        "lane",
        "suite",
        "floor",
        "city",
        "usa",
        "india",
        "uk",
        "canada",
        "singapore",
    )
    if not any(tok in lowered for tok in location_tokens):
        return ""
    marketing_tokens = ("trends", "reshape", "worldwide", "trusted by", "global capabilities")
    if any(tok in lowered for tok in marketing_tokens):
        return ""
    return value


def normalize_person_name(value: str) -> str:
    value = clean_text(value)
    if not value:
        return ""
    if len(value) < 4 or len(value) > 80:
        return ""
    # Must look like at least first+last name.
    parts = value.split()
    if len(parts) < 2 or len(parts) > 4:
        return ""
    if re.search(r"\d", value):
        return ""
    low = value.lower()
    banned = (
        " and ",
        " our ",
        " review",
        "read ",
        "message",
        "launches",
        "investor",
        "relations",
        "group",
        "corporation",
        "company",
        "inc",
        "ltd",
        "llc",
        "chief",
        "officer",
        "board",
        "performance",
        "ceo",
        "executive",
        "chairman",
        "president",
        "head",
        "leadership",
        "commercial",
        "connectivity",
        "talks",
        "receives",
        "meet ",
        "bio",
    )
    if any(b in low for b in banned):
        return ""
    for p in parts:
        if not re.fullmatch(r"[A-Z][A-Za-z'.-]+", p):
            return ""
    if len(parts) == 4 and parts[:2] == parts[2:]:
        return ""
    return value
