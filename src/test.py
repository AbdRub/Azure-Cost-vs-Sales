import csv
from io import StringIO
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from secret_manager import SecretsManager
from datetime import datetime, timedelta
import requests

# Initialize SecretsManager to fetch credentials
secrets = SecretsManager()

class PartnerCenterAPIClient:
    def __init__(self, base_url: str, client_id: str, client_secret: str, tenant_id: str, 
                 invoice_url: str, invoice_line_items_url: str, scope: str, blob_connection_string: str, blob_container_name: str):
        """
        Initializes the PartnerCenterAPIClient with the provided credentials and URLs.

        Args:
            base_url (str): Base URL for the Partner Center API.
            client_id (str): Client ID for OAuth authentication.
            client_secret (str): Client secret for OAuth authentication.
            tenant_id (str): Tenant ID for OAuth authentication.
            invoice_url (str): URL for fetching invoices.
            invoice_line_items_url (str): URL for fetching invoice line items.
            scope (str): Scope for OAuth authentication.
            blob_connection_string (str): Azure Blob Storage connection string.
            blob_container_name (str): Name of the container in Azure Blob Storage.
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
        self.blob_service_client = BlobServiceClient.from_connection_string(self.blob_connection_string)

    def get_access_token(self) -> str:
        """
        Obtains an access token for authentication with the Partner Center API.

        Returns:
            str: Access token for authorization.
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
        Filters invoice IDs to find those corresponding to July 2024.

        Args:
            invoice_ids (list[dict]): List of invoice data to filter.

        Returns:
            list[str]: List of filtered invoice IDs for July 2024.
        """
        # Filter invoices to find those starting with 'G'
        filtered_invoices = [invoice for invoice in invoice_ids if invoice['id'].startswith('G')]

        # Set the target month to July 2024
        target_month = '2024-07'

        # Collect IDs for the invoices matching the target month
        matching_invoice_ids = [invoice['id'] for invoice in filtered_invoices if invoice['billingPeriodStartDate'].startswith(target_month)]

        return matching_invoice_ids

    def get_invoice_line_items(self, invoice_id: str) -> list[dict]:
        """
        Retrieves line items for a specific invoice ID from the Partner Center API.

        Args:
            invoice_id (str): The ID of the invoice to fetch line items for.

        Returns:
            list[dict]: List of line items associated with the invoice.
        """
        # Replace the invoice ID in the line items URL
        line_items_url = self.invoice_line_items_url.replace("<invoiceID>", invoice_id)

        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }

        response = requests.get(line_items_url, headers=headers)
        if response.status_code == 200:
            return response.json().get('items', [])
        else:
            raise Exception(f"Error fetching invoice line items for {invoice_id}: {response.status_code}, {response.content}")

    def write_to_blob_storage(self, line_items: list[dict], invoice_id: str):
        """
        Writes the line items to Azure Blob Storage in CSV format.

        Args:
            line_items (list[dict]): The line items to write.
            invoice_id (str): The ID of the invoice, used for naming the file.
        """
        # Create a CSV in memory
        csv_output = StringIO()
        writer = csv.DictWriter(csv_output, fieldnames=line_items[0].keys())
        writer.writeheader()
        writer.writerows(line_items)

        # Get blob client for the container and blob (using invoice_id for filename)
        blob_client = self.blob_service_client.get_blob_client(container=self.blob_container_name, blob=f"invoice_{invoice_id}.csv")

        # Upload CSV content to Azure Blob Storage
        blob_client.upload_blob(csv_output.getvalue(), overwrite=True)
        print(f"Successfully uploaded invoice {invoice_id} line items to Azure Blob Storage.")

def main():
    """
    Main function to execute the workflow of fetching invoices, processing data,
    and writing the line items to Azure Blob Storage.
    """
    # Load credentials from secrets
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

    try:
        print("Fetching access token...")
        access_token = api_client.get_access_token()
        print("Access token retrieved successfully.")

        # Fetch invoice IDs
        print("Fetching invoice IDs...")
        invoice_ids = api_client.get_invoice_ids()
        print(f"Retrieved {len(invoice_ids)} invoice IDs that starts with G.")

        # Filter invoices and find matching months
        print("Filtering invoices to find July 2024 invoice...")
        matching_invoice_ids = api_client.filter_invoices(invoice_ids)

        if matching_invoice_ids:
            print(f"Found matching invoice ID(s): {matching_invoice_ids}")
            invoice_id = matching_invoice_ids[0]
            print(f"Fetching line items for invoice ID: {invoice_id}...")
            line_items = api_client.get_invoice_line_items(invoice_id)

            # Add billing_month information
            current_month_minus_1 = '2024-07-01'
            for item in line_items:
                item['billing_month'] = current_month_minus_1

            # Write line items to Blob Storage
            api_client.write_to_blob_storage(line_items, invoice_id)

        else:
            print("No matching invoices for July 2024.")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
