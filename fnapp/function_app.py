import azure.functions as func
import logging
import csv
from io import StringIO
from datetime import datetime, timedelta
import requests
import re
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
import time
import os

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

class PartnerCenterAPIClient:
    def __init__(self, base_url: str, client_id: str, client_secret: str, tenant_id: str, 
                 invoice_url: str, invoice_line_items_url: str, scope: str, blob_connection_string: str, blob_container_name: str, blob_directory_name: str):
        """
        Initializes the PartnerCenterAPIClient with the provided credentials and Blob storage configuration.

        Args:
            base_url (str): The base URL for Partner Center API.
            client_id (str): The client ID for authentication.
            client_secret (str): The client secret for authentication.
            tenant_id (str): The tenant ID for authentication.
            invoice_url (str): The URL to retrieve invoices.
            invoice_line_items_url (str): The URL to retrieve line items for a specific invoice.
            scope (str): The scope for API access.
            blob_connection_string (str): Connection string for Azure Blob Storage.
            blob_container_name (str): Name of the Blob Storage container.
            blob_directory_name (str): Name of the directory in Blob Storage.
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
        self.blob_directory_name = blob_directory_name

        # Initialize BlobServiceClient
        self.blob_service_client = BlobServiceClient.from_connection_string(blob_connection_string)
        self.blob_container_name = blob_container_name.lower()
        print("PartnerCenterAPIClient initialized successfully.")

    def check_blob_exists_in_directory(self, blob_directory_name: str, blob_name: str) -> bool:
        """
        Checks if the specified blob exists within a directory in the Azure Blob Storage container.

        Args:
            directory_name (str): The directory to check within the container.
            blob_name (str): The name of the blob to check.

        Returns:
            bool: True if the blob exists, False otherwise.
        """
        full_blob_path = f"{blob_directory_name}/{blob_name}"
        blob_client = self.blob_service_client.get_blob_client(container=self.blob_container_name, blob=full_blob_path)
        blob_exists = blob_client.exists()
        print(f"Checked existence for blob '{full_blob_path}': {'Exists' if blob_exists else 'Does not exist'}")
        return blob_exists
    
    def resume_fabric_capacity(self):
        """
        Calls an external API to resume Fabric capacity and waits 30 seconds to ensure it is fully available.
        """
        print("Attempting to resume Fabric capacity...")
        resume_url = "https://prod2-03.centralindia.logic.azure.com:443/workflows/3abceea03f6d477a999aab317aae1f0b/triggers/When_a_HTTP_request_is_received/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2FWhen_a_HTTP_request_is_received%2Frun&sv=1.0&sig=9GidnrLPemnX8ekgH1Abdi2lejeclnGZgNW9-6-e1J8"
        body = {"Action": "resume"}
        headers = {'Content-Type': 'application/json'}
        response = requests.post(resume_url, json=body, headers=headers)
        
        if response.status_code in [200, 202]:
            print("Fabric capacity resumed successfully. Waiting 30 seconds to ensure availability...")
            time.sleep(30)
        else:
            raise Exception(f"Failed to resume Fabric capacity: {response.status_code}, {response.content}")

    def get_access_token(self) -> str:
        """
        Obtains an access token for authentication with the Partner Center API.

        Returns:
            str: The access token.
        """
        print("Requesting access token...")
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
            print("Access token obtained successfully.")
        else:
            raise Exception(f"Failed to authenticate: {response.status_code}, {response.content}")
        return self.access_token
    
    def get_invoice_ids(self) -> list[dict]:
        """
        Retrieves the list of invoice IDs from the Partner Center API.

        Returns:
            list[dict]: List of invoice data.
        """
        print("Fetching invoice IDs...")
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        response = requests.get(self.invoice_url, headers=headers)
        if response.status_code == 200:
            invoices = response.json().get('items', [])
            print(f"Retrieved {len(invoices)} invoices.")
            return invoices
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
        print("Filtering invoices for last month's billing period...")
        filtered_invoices = [invoice for invoice in invoice_ids if invoice['id'].startswith('G')]
        today = datetime.today()
        first_day_of_current_month = today.replace(day=1)
        last_month = first_day_of_current_month - timedelta(days=1)
        target_month = last_month.strftime('%Y-%m')
        matching_invoice_ids = [invoice['id'] for invoice in filtered_invoices if invoice['billingPeriodStartDate'].startswith(target_month)]
        print(f"Found {len(matching_invoice_ids)} matching invoice(s) for {target_month}.")
        return matching_invoice_ids

    def get_invoice_line_items(self, invoice_id: str) -> list[dict]:
        """
        Retrieves line items for a specific invoice ID from the Partner Center API.

        Args:
            invoice_id (str): The ID of the invoice to fetch line items for.

        Returns:
            list[dict]: List of line items associated with the invoice.
        """
        print(f"Fetching line items for invoice ID: {invoice_id}...")
        line_items_url = self.invoice_line_items_url.replace("<invoiceID>", invoice_id)
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        response = requests.get(line_items_url, headers=headers)
        if response.status_code == 200:
            line_items = response.json().get('items', [])
            print(f"Retrieved {len(line_items)} line item(s) for invoice ID: {invoice_id}.")
            for item in line_items:
                item.pop('priceAdjustmentDescription', None)
                item.pop('attributes', None)
                item.pop('productQualifiers', None)
            return line_items
        else:
            raise Exception(f"Error fetching invoice line items for {invoice_id}: {response.status_code}, {response.content}")

    def write_to_blob_storage(self, line_items: list[dict], invoice_id: str):
        """
        Writes the line items to Azure Blob Storage in CSV format.

        Args:
            line_items (list[dict]): The line items to write.
            invoice_id (str): The ID of the invoice, used for naming the file.
        """
        print(f"Writing line items for invoice {invoice_id} to Blob Storage...")
        for item in line_items:
            for key, value in item.items():
                if isinstance(value, (dict, list)):
                    item[key] = str(value)
        df = pd.DataFrame(line_items)
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        container_client = self.blob_service_client.get_container_client(self.blob_container_name)
        if not container_client.exists():
            container_client.create_container()
        blob_client = self.blob_service_client.get_blob_client(
            container=self.blob_container_name,
            blob=f"invoice_line_items.csv"
        )
        blob_client.upload_blob(csv_buffer.getvalue(), overwrite=True)
        print(f"Successfully uploaded invoice {invoice_id} line items to Azure Blob Storage in CSV format.")

@app.route(route="reconfn")
def reconfn(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP trigger function that processes a request to fetch, filter, and store invoice line items from Partner Center API.

    Args:
        req (func.HttpRequest): The HTTP request object.

    Returns:
        func.HttpResponse: A response indicating success or any errors encountered.
    """
    logging.info('HTTP trigger function processing request...')

    # Environment Variables Setup
    base_url = os.getenv('PARTNER_API_BASE_URL')
    client_id = os.getenv('CLIENT_ID')
    client_secret = os.getenv('CLIENT_SECRET')
    tenant_id = os.getenv('TENANT_ID')
    invoice_url = os.getenv('INVOICE_URL')
    invoice_line_items_url = os.getenv('INVOICE_LINE_ITEMS_URL')
    scope = os.getenv('SCOPE')
    blob_connection_string = os.getenv('BLOB_CONNECTION_STRING')
    blob_container_name = os.getenv('BLOB_CONTAINER_NAME')
    blob_directory_name = os.getenv('BLOB_DIRECTORY_NAME')

    api_client = PartnerCenterAPIClient(base_url, client_id, client_secret, tenant_id, invoice_url, invoice_line_items_url, scope, blob_connection_string, blob_container_name, blob_directory_name)

    try:
        current_date = datetime.now()
        if current_date.month == 1:
            previous_month = 12
            year = current_date.year - 1
        else:
            previous_month = current_date.month - 1
            year = current_date.year
        blob_name = f"RCLI-{year}{previous_month:02d}.csv"
        
        # Check if blob already exists
        if api_client.check_blob_exists_in_directory(blob_directory_name, blob_name):
            print(f"Blob '{blob_name}' already exists. Exiting.")
            return func.HttpResponse("File already exists in the archives, exiting the process.")

        print(f"Blob '{blob_name}' not found. Resuming Fabric capacity and proceeding.")
        api_client.resume_fabric_capacity()

        # Fetching and Processing Invoice Data
        access_token = api_client.get_access_token()
        invoice_ids = api_client.get_invoice_ids()
        matching_invoice_ids = api_client.filter_invoices(invoice_ids)

        if matching_invoice_ids:
            invoice_id = matching_invoice_ids[0]
            line_items = api_client.get_invoice_line_items(invoice_id)
            api_client.write_to_blob_storage(line_items, invoice_id)
        else:
            print("No matching invoices found for the last month.")
            
    except Exception as e:
        print(f"Error occurred: {e}")
        return func.HttpResponse(f"An error occurred: {e}")

    print("Function executed successfully.")
    return func.HttpResponse("Function executed successfully")