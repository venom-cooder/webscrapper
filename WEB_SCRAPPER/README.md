# WEB_SCRAPPER

Beginner-friendly company scraper pipeline.

## Folder Structure

```
WEB_SCRAPPER/
  input/
    input_companies.csv
  output/
    output.csv
  scrapper/
    extractor.py
    fetcher.py
    scrapper.py
    utils.py
  sheets/
    sheets_writer.py
  main.py
  requirements.txt
  service_account.json (you add this)
```

## 1) Install dependencies

```bash
cd WEB_SCRAPPER
python3 -m pip install -r requirements.txt
```

## 2) Prepare input CSV

Use `input/input_companies.csv` with columns:

- `company_name`
- `website`

Example:

```csv
company_name,website
OpenAI,openai.com
Microsoft,microsoft.com
```

## 3) Run scraper and generate output CSV

```bash
python3 main.py --input input/input_companies.csv --output output/output.csv
```

Seed-mode (fills all columns with realistic dummy values for demos/testing):

```bash
python3 main.py --input input/input_companies.csv --output output/output.csv --seed-data
```

Expected output columns:

- `company_name`
- `website`
- `full_address`
- `director_or_founder`
- `founded_year`
- `email`
- `phone`
- `source_page`
- `scrape_status`

## 4) Connect and upload to Google Sheets

1. Create a Google Cloud service account.
2. Enable Google Sheets API + Google Drive API.
3. Download key JSON as `service_account.json` in project root.
4. Share your target Google Sheet with the service account email (Editor access).
5. Copy Spreadsheet ID from sheet URL.

Run:

```bash
python3 main.py \
  --input input/input_companies.csv \
  --output output/output.csv \
  --seed-data \
  --upload-sheets \
  --service-account service_account.json \
  --worksheet Sheet1
```

Optional:
- If you omit `--spreadsheet-id`, a new spreadsheet is auto-created.
- You can set the title with `--spreadsheet-title "Intern Seed Output"`.

## Notes

- If some websites block scraping, `scrape_status` will show a failure reason.
- This environment currently has restricted DNS/network, so online scraping may fail locally here, but code is ready to run on normal internet.
