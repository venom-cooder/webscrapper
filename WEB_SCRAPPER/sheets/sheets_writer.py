from typing import List

import gspread
from google.oauth2.service_account import Credentials


class GoogleSheetsWriter:
    def __init__(self, service_account_path: str):
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_file(service_account_path, scopes=scopes)
        self.client = gspread.authorize(creds)

    def write_rows(
        self,
        spreadsheet_id: str,
        worksheet_name: str,
        headers: List[str],
        rows: List[List[str]],
        clear_before_write: bool = True,
    ):
        spreadsheet = self.client.open_by_key(spreadsheet_id)
        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=2000, cols=26)

        if clear_before_write:
            worksheet.clear()

        values = [headers] + rows
        worksheet.update("A1", values)

    def create_spreadsheet(self, title: str) -> str:
        spreadsheet = self.client.create(title)
        return spreadsheet.id
