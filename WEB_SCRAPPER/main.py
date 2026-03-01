import argparse
import os
import socket
from pathlib import Path

import pandas as pd
from google.auth.exceptions import TransportError

from scrapper.scrapper import CompanyScrapper
from scrapper.indian_startups import generate_indian_startups_df
from scrapper.utils import clean_text, normalize_url
from scrapper.utils import COLUMNS
from sheets.sheets_writer import GoogleSheetsWriter


def load_input_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    normalized = {c.lower().strip(): c for c in df.columns}

    name_col = None
    for c in ["company_name", "company", "name"]:
        if c in normalized:
            name_col = normalized[c]
            break

    web_col = None
    for c in ["website", "url", "company_url", "domain"]:
        if c in normalized:
            web_col = normalized[c]
            break

    if not name_col or not web_col:
        raise ValueError(
            "Input CSV must include company name and website columns. Accepted names: "
            "name/company/company_name and website/url/company_url/domain"
        )

    out = pd.DataFrame()
    out["company_name"] = df[name_col].fillna("").astype(str)
    out["website"] = df[web_col].fillna("").astype(str)
    return out


def run_pipeline(input_csv: str, output_csv: str) -> pd.DataFrame:
    input_df = load_input_csv(input_csv)
    scraper = CompanyScrapper()

    rows = []
    total = len(input_df)
    for idx, rec in input_df.iterrows():
        company = rec["company_name"]
        website = rec["website"]
        print(f"[{idx + 1}/{total}] scraping: {company} -> {website}")
        row = scraper.scrape_company(company, website)
        rows.append(row)

    output_df = pd.DataFrame(rows, columns=COLUMNS).fillna("")
    Path(output_csv).parent.mkdir(parents=True, exist_ok=True)
    output_df.to_csv(output_csv, index=False)
    print(f"Output saved to: {output_csv}")
    return output_df


def run_seed_pipeline(input_csv: str, output_csv: str) -> pd.DataFrame:
    input_df = load_input_csv(input_csv)
    rows = []

    for idx, rec in input_df.iterrows():
        company = clean_text(rec["company_name"])
        website = normalize_url(rec["website"])
        n = idx + 1
        city = f"City {((n - 1) % 20) + 1}"
        state = f"State {((n - 1) % 10) + 1}"
        founder = f"Founder {n:03d}"
        year = str(1990 + ((n - 1) % 30))
        email = f"contact{n:03d}@examplemail.com"
        phone = f"+1-202-555-{1000 + n:04d}"
        address = f"{100 + n} Innovation Street, {city}, {state}, USA"

        rows.append(
            {
                "company_name": company,
                "website": website,
                "full_address": address,
                "director_or_founder": founder,
                "founded_year": year,
                "email": email,
                "phone": phone,
                "source_page": website,
                "scrape_status": "seed_data",
            }
        )

    output_df = pd.DataFrame(rows, columns=COLUMNS).fillna("")
    Path(output_csv).parent.mkdir(parents=True, exist_ok=True)
    output_df.to_csv(output_csv, index=False)
    print(f"Seed output saved to: {output_csv}")
    return output_df


def push_to_google_sheet(
    df: pd.DataFrame,
    service_account_path: str,
    spreadsheet_id: str,
    worksheet_name: str,
    spreadsheet_title: str = "",
):
    if not os.path.exists(service_account_path):
        raise FileNotFoundError(
            f"Service account file not found at '{service_account_path}'. Add it and retry."
        )

    _validate_google_dns()

    writer = GoogleSheetsWriter(service_account_path=service_account_path)
    try:
        if not spreadsheet_id:
            spreadsheet_id = writer.create_spreadsheet(
                spreadsheet_title or "Company Scraper Output"
            )
            print(f"Created new Google Sheet with ID: {spreadsheet_id}")

        df = df.fillna("")
        rows = [[str(row.get(col, "") or "") for col in COLUMNS] for _, row in df.iterrows()]
        writer.write_rows(
            spreadsheet_id=spreadsheet_id,
            worksheet_name=worksheet_name,
            headers=COLUMNS,
            rows=rows,
            clear_before_write=True,
        )
        print(f"Data uploaded to Google Sheet: {spreadsheet_id} [{worksheet_name}]")
    except TransportError as exc:
        raise RuntimeError(
            "Google auth/token request failed due network or DNS issue. "
            "If you are behind VPN/firewall/proxy, allow access to "
            "oauth2.googleapis.com, sheets.googleapis.com, and www.googleapis.com."
        ) from exc
    except Exception as exc:
        msg = str(exc)
        if "storage quota has been exceeded" in msg.lower():
            raise RuntimeError(
                "Google Drive quota exceeded for this service account while creating a new sheet. "
                "Create a spreadsheet in your own Google account, share it with the service account "
                "email as Editor, then rerun with --spreadsheet-id <YOUR_SHEET_ID>."
            ) from exc
        if "PERMISSION_DENIED" in msg or "The caller does not have permission" in msg:
            raise RuntimeError(
                "Permission denied by Google Sheets. Share the target sheet with service "
                "account email as Editor, and ensure Sheets + Drive APIs are enabled."
            ) from exc
        raise


def _validate_google_dns():
    hosts = ("oauth2.googleapis.com", "sheets.googleapis.com", "www.googleapis.com")
    failed = []
    for host in hosts:
        try:
            socket.getaddrinfo(host, 443)
        except OSError as exc:
            failed.append(f"{host} ({exc})")

    if failed:
        raise RuntimeError(
            "Google API DNS resolution failed in this environment: "
            + "; ".join(failed)
            + ". Fix internet/DNS and retry."
        )


def parse_args():
    parser = argparse.ArgumentParser(description="Company data scraper + Google Sheets uploader")
    parser.add_argument(
        "--input",
        default="input/input_companies.csv",
        help="Path to input companies CSV",
    )
    parser.add_argument(
        "--output",
        default="output/output.csv",
        help="Path to output CSV",
    )
    parser.add_argument(
        "--upload-sheets",
        action="store_true",
        help="Upload output data to Google Sheets",
    )
    parser.add_argument(
        "--upload-only",
        action="store_true",
        help="Skip scraping/seed generation and upload existing --output CSV to Google Sheets",
    )
    parser.add_argument(
        "--seed-data",
        action="store_true",
        help="Generate complete seeded output values instead of live scraping",
    )
    parser.add_argument(
        "--indian-startups",
        action="store_true",
        help="Generate a verified Indian startups dataset from public sources",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum rows for --indian-startups mode",
    )
    parser.add_argument(
        "--service-account",
        default="service_account.json",
        help="Path to Google service account JSON key",
    )
    parser.add_argument(
        "--spreadsheet-id",
        default="",
        help="Google Spreadsheet ID",
    )
    parser.add_argument(
        "--spreadsheet-title",
        default="Company Scraper Output",
        help="Used only when --spreadsheet-id is not provided; creates a new spreadsheet",
    )
    parser.add_argument(
        "--worksheet",
        default="Sheet1",
        help="Worksheet/tab name",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    if args.upload_only:
        if not os.path.exists(args.output):
            raise FileNotFoundError(f"Output CSV not found at '{args.output}' for --upload-only")
        output_df = pd.read_csv(args.output, keep_default_na=False).fillna("")
        for col in COLUMNS:
            if col not in output_df.columns:
                output_df[col] = ""
        output_df = output_df[COLUMNS].fillna("")
    else:
        if args.indian_startups:
            output_df = generate_indian_startups_df(limit=args.limit).fillna("")
            Path(args.output).parent.mkdir(parents=True, exist_ok=True)
            output_df.to_csv(args.output, index=False)
            print(f"Indian startups output saved to: {args.output}")
        else:
            output_df = (
                run_seed_pipeline(args.input, args.output)
                if args.seed_data
                else run_pipeline(args.input, args.output)
            )

    if args.upload_sheets:
        push_to_google_sheet(
            df=output_df,
            service_account_path=args.service_account,
            spreadsheet_id=args.spreadsheet_id,
            worksheet_name=args.worksheet,
            spreadsheet_title=args.spreadsheet_title,
        )


if __name__ == "__main__":
    main()
