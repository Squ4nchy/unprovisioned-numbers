# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.14.0
#   kernelspec:
#     display_name: upn-env
#     language: python
#     name: upn-env
# ---

# +
import os
import json
import requests as r

import pandas as pd
from google.oauth2 import service_account
from google.auth.transport.requests import Request

class GoogleSheetsApi:
    
    upn_sheet_endpoint = ("<google sheets api>")
    
    @staticmethod
    def oauth_for_scope_spreadsheet():
        upn_project_secrets = os.path.join(
            os.path.abspath(""),
            "data",
            "unprovisioned-numbers-60b5effbe263.json"
        )
            
        SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
        credentials = service_account.Credentials.from_service_account_file(
            upn_project_secrets, scopes=SCOPES
        )
        
        return credentials
    
    @staticmethod
    def update_credentials_token(credentials):
        credentials.refresh(Request())
        
    @classmethod
    def pull_current_spreadsheet_values_from_sheets(cls, credentials):
        if not credentials.valid:
            cls.update_credentials_token(credentials)
        
        full_web_data = r.get(cls.upn_sheet_endpoint,
                              headers={
                                  "Authorization": f"Bearer {credentials.token}"
                              }).text
        sheet_values = json.loads(full_web_data)['values']
        column_names = sheet_values.pop(0)
        
        return pd.DataFrame(sheet_values, columns=column_names)

    def update_full_sheet_with_additional_values(self, credentials):
        if not credentials.valid:
            cls.update_credentials_token(credentials)
        
        update_values = self.make_values_json_readable()
        
        return r.put(
            f"{self.upn_sheet_endpoint}?valueInputOption=USER_ENTERED",
            json={
                "range": "unprovisioned_numbers",
                "majorDimension": "ROWS",
                "values": update_values
            },
            headers={"Authorization": f"Bearer {credentials.token}"})
