import csv
from io import StringIO
from secret_manager import SecretsManager
from datetime import datetime
import requests
import pandas as pd
from azure.storage.blob import BlobServiceClient

# Initialize SecretsManager to fetch credentials
secrets = SecretsManager()

class PartnerCenterAPIClient:
    def __init__(self, base_url: str, client_id: str, client_secret: str, tenant_id: str, 
                 invoice_url: str, invoice_line_items_url: str, scope: str, blob_connection_string: str, blob_container_name: str):
        self.base_url = base_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.scope = scope  
        self.invoice_url = invoice_url
        self.invoice_line_items_url = invoice_line_items_url
        self.blob_connection_string = blob_connection_string
        self.blob_container_name = blob_container_name.lower()

        # Initialize BlobServiceClient
        self.blob_service_client = BlobServiceClient.from_connection_string(blob_connection_string)
        print("PartnerCenterAPIClient initialized successfully.")

    def check_blob_exists(self, blob_name: str) -> bool:
        blob_client = self.blob_service_client.get_blob_client(container=self.blob_container_name, blob=blob_name)
        exists = blob_client.exists()
        print(f"Checked if blob '{blob_name}' exists: {exists}")
        return exists

    def get_access_token(self) -> str:
        token_url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        body = {
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'scope': self.scope, 
            'grant_type': 'client_credentials'
        }

        response = requests.post(token_url, data=body, headers=headers)
        if response.status_code == 200:
            token_data = response.json()
            self.access_token = token_data['access_token']
            print("Access token retrieved successfully.")
        else:
            raise Exception(f"Failed to authenticate: {response.status_code}, {response.content}")

        return self.access_token

    def get_invoice_ids(self) -> dict:
        headers = {'Authorization': f'Bearer {self.access_token}', 'Content-Type': 'application/json'}
        response = requests.get(f"{self.invoice_url}", headers=headers)
        if response.status_code == 200:
            invoices = response.json().get('items', [])
            print(f"Retrieved {len(invoices)} invoices.")
        else:
            raise Exception(f"Error fetching invoices: {response.status_code}, {response.content}")

        d = {}
        for invoice in invoices:
            d[invoice["id"]] = datetime.strptime(invoice["billingPeriodStartDate"], "%Y-%m-%dT%H:%M:%SZ").date()
        print(f"Parsed invoice IDs and charge start dates: {d}")
        return d

    def filter_invoices(self, invoice_ids: dict) -> dict:
        filtered_invoices = {key: value for key, value in invoice_ids.items() if key.startswith('G')}
        print(f"Filtered {len(filtered_invoices)} invoice(s) starting with 'G'.")
        return filtered_invoices

    def get_invoice_line_items(self, invoice_id: str) -> list[dict]:
        line_items_url = self.invoice_line_items_url.replace("<invoiceID>", invoice_id)

        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }

        response = requests.get(line_items_url, headers=headers)
        if response.status_code == 200:
            line_items = response.json().get('items', [])
            print(f"Retrieved {len(line_items)} line items for invoice {invoice_id}.")
            
            for item in line_items:
                item.pop('priceAdjustmentDescription', None)
                item.pop('attributes', None)
                item.pop('productQualifiers', None)

            return line_items
        else:
            raise Exception(f"Error fetching invoice line items for {invoice_id}: {response.status_code}, {response.content}")

    def write_to_blob_storage(self, df: bytes, blob_name: str):
        container_client = self.blob_service_client.get_container_client(self.blob_container_name)
        if not container_client.exists():
            container_client.create_container()
            print(f"Created blob container: {self.blob_container_name}")

        blob_client = self.blob_service_client.get_blob_client(container=self.blob_container_name, blob=blob_name)
        blob_client.upload_blob(df, overwrite=True)
        print(f"Uploaded CSV file to Azure Blob Storage with name: {blob_name}")

def main():
    base_url = secrets.partner_api_base_url
    client_id = secrets.client_id
    client_secret = secrets.client_secret
    tenant_id = secrets.tenant_id
    invoice_url = secrets.invoice_url
    invoice_line_items_url = secrets.invoice_line_item_url
    scope = secrets.scope
    blob_connection_string = secrets.blob_connection_string
    blob_container_name = secrets.blob_container_name

    # Initialize API client
    api_client = PartnerCenterAPIClient(base_url, client_id, client_secret, tenant_id, invoice_url, invoice_line_items_url, scope, blob_connection_string, blob_container_name)

    # Fetch access token and invoice data
    print("Fetching access token...")
    access_token = api_client.get_access_token()

    # Fetch invoice IDs
    print("Fetching invoice IDs...")
    invoice_ids = api_client.get_invoice_ids()

    # Filter invoices starting with 'G'
    matching_invoice_ids = api_client.filter_invoices(invoice_ids)

    for idx, (invoice_id, charge_start_date) in enumerate(matching_invoice_ids.items()):
        print(f"Processing invoice {invoice_id} for {charge_start_date}...")

        # Fetch line items for the current invoice
        line_items = api_client.get_invoice_line_items(invoice_id)

        # Convert line items to DataFrame
        invoice_df = pd.DataFrame(line_items)

        # Generate blob name using year and month
        blob_name = f"{charge_start_date.year}-{charge_start_date.month:02d}_recon_line_items.csv"
        print(f"Generated blob name: {blob_name}")

        # Convert DataFrame to CSV and upload to Azure Blob Storage
        csv_data = invoice_df.to_csv(index=False)
        api_client.write_to_blob_storage(df=bytes(csv_data, encoding='utf-8'), blob_name=blob_name)

    print("All invoices processed and uploaded.")

if __name__ == "__main__":
    main()
