import csv
from io import StringIO
from secret_manager import SecretsManager
from datetime import datetime, timedelta
import requests
import re
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
import time

# Initialize SecretsManager to fetch credentials
secrets = SecretsManager()

class PartnerCenterAPIClient:
    def __init__(self, base_url: str, client_id: str, client_secret: str, tenant_id: str, 
                 invoice_url: str, invoice_line_items_url: str, scope: str, blob_connection_string: str, blob_container_name: str, blob_directory_name: str):
        """
        Initializes the PartnerCenterAPIClient with the provided credentials and URLs.
        """
        self.base_url = base_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.scope = scope  
        self.invoice_url = invoice_url
        self.invoice_line_items_url = invoice_line_items_url
        self.blob_connection_string = blob_connection_string
        self.blob_container_name = blob_container_name

        # Initialize BlobServiceClient
        self.blob_service_client = BlobServiceClient.from_connection_string(blob_connection_string)
        self.blob_directory_name = blob_directory_name

    def check_blob_exists(self, blob_name: str) -> bool:
        """
        Checks if the specified blob exists in the Azure Blob Storage directory.
        """
        blob_path = f"{self.blob_directory_name}/{blob_name}"
        blob_client = self.blob_service_client.get_blob_client(container=self.blob_container_name, blob=blob_path)
        return blob_client.exists()

    def resume_fabric_capacity(self):
        """
        Calls the API to resume Fabric capacity and waits 30 seconds to allow the capacity to be fully available.
        """
        resume_url = "https://prod2-03.centralindia.logic.azure.com:443/workflows/3abceea03f6d477a999aab317aae1f0b/triggers/When_a_HTTP_request_is_received/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2FWhen_a_HTTP_request_is_received%2Frun&sv=1.0&sig=9GidnrLPemnX8ekgH1Abdi2lejeclnGZgNW9-6-e1J8"
        
        body = {"Action": "resume"}
        headers = {'Content-Type': 'application/json'}

        response = requests.post(resume_url, json=body, headers=headers)
        
        if response.status_code == 200 or response.status_code == 202:
            print("Fabric capacity resumed successfully or is in the process of starting.")
            print("Waiting 30 seconds to ensure the capacity is fully available...")
            time.sleep(30)  # Wait for 30 seconds to allow the capacity to open
        else:
            raise Exception(f"Failed to resume Fabric capacity: {response.status_code}, {response.content}")


    def get_access_token(self) -> str:
        """
        Obtains an access token for authentication with the Partner Center API.
        """
        token_url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
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
        else:
            raise Exception(f"Failed to authenticate: {response.status_code}, {response.content}")

        return self.access_token

    def get_invoice_ids(self) -> list[dict]:
        """
        Retrieves the list of invoice IDs from the Partner Center API.

        Returns:
            list[dict]: List of invoice data with IDs and billing dates.
        """
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        response = requests.get(f"{self.invoice_url}", headers=headers)
        if response.status_code == 200:
            return response.json().get('items', [])
        else:
            raise Exception(f"Error fetching invoices: {response.status_code}, {response.content}")

    def filter_invoices(self, invoice_ids: list[dict]) -> list[str]:
        """
        Filters invoice IDs to find those corresponding to the last month.

        Args:
            invoice_ids (list[dict]): List of invoice data to filter.

        Returns:
            list[str]: List of filtered invoice IDs for the last month.
        """
        # Filter invoices to find those starting with 'G'
        filtered_invoices = [invoice for invoice in invoice_ids if invoice['id'].startswith('G')]

        # Get the current date and calculate the previous month
        today = datetime.today()
        first_day_of_current_month = today.replace(day=1)
        last_month = first_day_of_current_month - timedelta(days=1)

        # Format the target month in YYYY-MM format
        target_month = last_month.strftime('%Y-%m')

        # Collect IDs for the invoices matching the target month
        matching_invoice_ids = [invoice['id'] for invoice in filtered_invoices if invoice['billingPeriodStartDate'].startswith(target_month)]

        return matching_invoice_ids

    def get_invoice_line_items(self, invoice_id: str) -> list[dict]:
        """
        Retrieves line items for a specific invoice ID from the Partner Center API.

        Args:
            invoice_id (str): The ID of the invoice to fetch line items for.

        Returns:
            list[dict]: List of line items associated with the invoice, excluding 'priceAdjustmentDescription'.
        """
        # Replace the invoice ID in the line items URL
        line_items_url = self.invoice_line_items_url.replace("<invoiceID>", invoice_id)

        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }

        response = requests.get(line_items_url, headers=headers)
        if response.status_code == 200:
            line_items = response.json().get('items', [])
            
            # Remove 'priceAdjustmentDescription' from each item
            for item in line_items:
                item.pop('priceAdjustmentDescription', None)  # Remove 'priceAdjustmentDescription' if exists
                item.pop('attributes', None)  # Remove 'attributes' if exists
                item.pop('productQualifiers', None)  # Remove 'productqualifies' if exists
        
            return line_items
        else:
            raise Exception(f"Error fetching invoice line items for {invoice_id}: {response.status_code}, {response.content}")

    def write_to_blob_storage(self, line_items: list[dict], blob_name: str):
        """
        Writes the line items to Azure Blob Storage in CSV format.

        Args:
            line_items (list[dict]): The line items to write.
            blob_name (str): The name of the blob to store the data.
        """
        # Convert complex types like lists or dictionaries into strings for CSV
        for item in line_items:
            for key, value in item.items():
                if isinstance(value, (dict, list)):
                    item[key] = str(value)  # Convert to string to handle complex types in CSV

        # Create a DataFrame from the line items
        df = pd.DataFrame(line_items)

        # Create a CSV file in memory
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        # Ensure the container exists
        container_client = self.blob_service_client.get_container_client(self.blob_container_name)
        if not container_client.exists():
            container_client.create_container()

        # Get blob client for the container and blob using the blob_name parameter
        blob_client = self.blob_service_client.get_blob_client(
            container=self.blob_container_name,
            blob=f"{self.blob_directory_name}/{blob_name}"  # Use blob_name here
        )

        # Upload CSV content to Azure Blob Storage
        blob_client.upload_blob(csv_buffer.getvalue(), overwrite=True)
        print(f"Successfully uploaded line items to blob '{blob_name}' in Azure Blob Storage.")


def main():
    # Load secrets
    base_url = secrets.partner_api_base_url
    client_id = secrets.client_id
    client_secret = secrets.client_secret
    tenant_id = secrets.tenant_id
    invoice_url = secrets.invoice_url
    invoice_line_items_url = secrets.invoice_line_item_url
    scope = secrets.scope
    blob_connection_string = secrets.blob_connection_string
    blob_container_name = secrets.blob_container_name
    blob_directory_name = secrets.blob_directory_name

    # Initialize API client
    api_client = PartnerCenterAPIClient(base_url, client_id, client_secret, tenant_id, invoice_url, invoice_line_items_url, scope, blob_connection_string, blob_container_name, blob_directory_name)

    try:
        # Calculate previous year and month
        current_date = datetime.now()
        previous_month = current_date.month - 1 if current_date.month > 1 else 12
        previous_year = current_date.year if current_date.month > 1 else current_date.year - 1

        # Set blob name with previous month in 'RCLI-YYYYMM' format
        blob_name = f"RCLI-{previous_year}{previous_month:02d}.csv"

        print(f"Checking if blob '{blob_name}' exists in the directory '{api_client.blob_directory_name}'...")

        if api_client.check_blob_exists(blob_name):
            print(f"Blob '{blob_name}' already exists in the directory. Exiting process.")
            return  # Exit if the blob is present

        print(f"Blob '{blob_name}' does not exist in the directory. Proceeding with resuming Fabric capacity...")
        api_client.resume_fabric_capacity()

        # Fetch access token and invoice data
        print("Fetching access token...")
        access_token = api_client.get_access_token()
        print("Access token retrieved successfully.")

        # Fetch invoice IDs
        print("Fetching invoice IDs...")
        invoice_ids = api_client.get_invoice_ids()
        print(f"Retrieved {len(invoice_ids)} invoice IDs that start with G.")

        # Calculate the previous month dynamically
        today = datetime.today()
        first_day_of_current_month = today.replace(day=1)
        last_month = first_day_of_current_month - timedelta(days=1)
        billing_month = last_month.strftime('%Y-%m-01')
        last_month_name = last_month.strftime('%B')  # Get the name of the previous month

        # Filter invoices for the previous month
        print(f"Filtering invoices to find invoices for {last_month_name}...")
        matching_invoice_ids = api_client.filter_invoices(invoice_ids)

        if matching_invoice_ids:
            print(f"Found matching invoice ID(s) for {last_month_name}: {matching_invoice_ids}")
            invoice_id = matching_invoice_ids[0]
            print(f"Fetching line items for invoice ID: {invoice_id}...")

            line_items = api_client.get_invoice_line_items(invoice_id)

            # Write to blob storage using the same blob_name
            api_client.write_to_blob_storage(line_items, blob_name)

        else:
            print(f"No matching invoices for {last_month_name}.")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()