import requests as rs
from zipfile import ZipFile
from io import BytesIO, StringIO
import pandas as pd
import numpy as np
import json
import datetime as dt
import warnings
import duckdb as db

def main():
    script_start_time = dt.datetime.now().strftime("%d %b %Y %I:%M %p")
    print(f'main script started execution at {script_start_time}')

    # Global variables
    base_url = 'https://api.partnercenter.microsoft.com'
    relative_invoices_url = '/v1/invoices'

    # Define functions

    def set_display_options():
        """Set display options for Pandas and warnings."""
        pd.set_option('max_colwidth', None)
        warnings.filterwarnings('ignore')
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)

    def get_duckdb_client(db_file_path: str):
        """Connect to DuckDB and return the connection object."""
        return db.connect(db_file_path)

    def parse_secrets(secrets_file_path: str) -> dict:
        """Parse the secrets.json file and return the secrets as a dictionary."""
        with open(secrets_file_path) as f:
            secrets = json.load(f)
        return secrets

    def execute_query(sql_string: str) -> None:
        """Execute a SQL query using DuckDB global connection and print the results."""
        try:
            return cxn.query(sql_string)
        except Exception as e:
            print(f"Query execution failed: {e}")

    def print_query(sql_string: str) -> None:
        """Execute a SQL query using DuckDB global connection and print the results."""
        try:
            return cxn.query(sql_string).show(max_width=100000, max_rows=100000)
        except Exception as e:
            print(f"Query execution failed: {e}")

    def get_access_token(refresh_token: str, app_id: str, app_secret: str) -> str:
        """Get an access token using the provided refresh token, app ID, and app secret."""
        request_body = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "scope": "openid",
            "resource": "https://api.partnercenter.microsoft.com",
            "client_id": app_id,
            "client_secret": app_secret,
        }

        response = rs.post(
            "https://login.windows.net/6e75cca6-47f0-47a3-a928-9d5315750bd9/oauth2/token",
            data=request_body
        )
        
        response.raise_for_status()
        return response.json()['access_token']

    def get_invoices(base_url: str, headers: dict) -> dict:
        """Get all invoices from partner center."""
        response = rs.get(f"{base_url}{relative_invoices_url}", headers=headers)
        response.raise_for_status()
        return response.json().get('items', [])

    set_display_options()

    # Parse secrets file
    print('Trying to parse secrets file')
    try:
        secrets = parse_secrets('../secrets.json')
        print('Secrets file found')
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f'Secrets file not found or some error in file: {e}')
        return

    # Get DuckDB client
    print('Trying to connect to DuckDB')
    try:
        cxn = get_duckdb_client('../duckdb.db')
        print('Connected to DuckDB')
    except Exception as ex:
        print(f'Unable to connect to DuckDB: {ex}')
        return

    refresh_token = secrets['refresh_token']
    app_id = secrets['app_id']
    app_secret = secrets['app_secret']

    # Get access token
    print('Trying to obtain access token')
    try:
        access_token = get_access_token(refresh_token, app_id, app_secret)
        print(f'Refresh Token valid, access token obtained. \nAccess token: {access_token[:20]}...')
    except Exception as ex:
        print(f'Unable to obtain access token: {ex}')
        return

    http_headers = {'Authorization': 'Bearer ' + access_token}

    # Fetch invoices
    print('Fetching invoices')
    try:
        invoices = get_invoices(base_url, http_headers)
        print(f'Fetched {len(invoices)} invoices')
    except Exception as ex:
        print(f'Unable to fetch invoices: {ex}')
        return
    
    invoices_df = pd.DataFrame(invoices)
    print_query("SELECT * FROM invoices_df LIMIT 5")

if __name__ == "__main__":
    main()
