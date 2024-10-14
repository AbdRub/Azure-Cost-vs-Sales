import requests
import pandas as pd
from secret_manager import SecretsManager
from datetime import datetime
import pyodbc
from IPython.display import display

# Initialize SecretsManager to fetch credentials
secrets = SecretsManager()

class PartnerCenterAPIClient:
    def __init__(self, base_url: str, client_id: str, client_secret: str, tenant_id: str, 
                 invoice_url: str, invoice_line_items_url: str, scope: str):
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
        """
        self.base_url = base_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.scope = scope  
        self.invoice_url = invoice_url
        self.invoice_line_items_url = invoice_line_items_url

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

    def get_invoice_ids(self) -> list[str]:
        """
        Retrieves the list of invoice IDs from the Partner Center API.

        Returns:
            list[str]: List of invoice IDs.
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

    def filter_invoices(self, invoice_ids: list[str]) -> list[str]:
        """
        Filters invoice IDs to find those corresponding to the previous month.

        Args:
            invoice_ids (list[str]): List of invoice IDs to filter.

        Returns:
            list[str]: List of filtered invoice IDs for the previous month.
        """
        # Convert to DataFrame
        df_invoices = pd.DataFrame(invoice_ids)
        filtered_invoices = df_invoices[df_invoices['id'].str.startswith('G')]

        # Parse 'billingPeriodStartDate' to datetime format
        filtered_invoices['billingPeriodStartDate'] = pd.to_datetime(filtered_invoices['billingPeriodStartDate'])

        # Find the maximum month from 'billingPeriodStartDate'
        max_date = filtered_invoices['billingPeriodStartDate'].max()

        # Get the current month - 1
        current_month_minus_1 = (datetime.now().replace(day=1) - pd.DateOffset(months=1)).strftime('%Y-%m')

        # Compare max_date with current_month_minus_1
        max_month_str = max_date.strftime('%Y-%m')

        if max_month_str == current_month_minus_1:
            matching_invoices = filtered_invoices[filtered_invoices['billingPeriodStartDate'].dt.strftime('%Y-%m') == max_month_str]
            invoice_ids = matching_invoices['id'].tolist()
            return invoice_ids
        else:
            return []

    def get_invoice_line_items(self, invoice_id: str) -> list:
        """
        Retrieves line items for a specific invoice ID from the Partner Center API.

        Args:
            invoice_id (str): The ID of the invoice to fetch line items for.

        Returns:
            list: List of line items associated with the invoice.
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
            return line_items
        else:
            raise Exception(f"Error fetching invoice line items for {invoice_id}: {response.status_code}, {response.content}")

class FabricDatawarehouse:
    def __init__(self, connection_string: str):
        """
        Initializes the FabricDatawarehouse with the provided connection string.

        Args:
            connection_string (str): Connection string for the Microsoft Fabric Datawarehouse.
        """
        self.connection_string = connection_string
    
    def check_billing_month(self, billing_month: str) -> bool:
        """
        Checks if the specified billing month exists in the datawarehouse.

        Args:
            billing_month (str): The billing month to check for existence.

        Returns:
            bool: True if the billing month exists, False otherwise.
        """
        query = f"SELECT COUNT(1) FROM pc.raw_data WHERE billing_month = ?"
        
        with pyodbc.connect(self.connection_string) as conn:
            cursor = conn.cursor()
            cursor.execute(query, (billing_month,))
            count = cursor.fetchone()[0]
            return count > 0

    def delete_billing_month(self, billing_month: str):
        """
        Deletes records for the specified billing month from the datawarehouse.

        Args:
            billing_month (str): The billing month for which records should be deleted.
        """
        delete_query = f"DELETE FROM pc.raw_data WHERE billing_month = ?"
        
        with pyodbc.connect(self.connection_string) as conn:
            cursor = conn.cursor()
            cursor.execute(delete_query, (billing_month,))
            conn.commit()

    def insert_data(self, df: pd.DataFrame):
        """
        Inserts data from a DataFrame into the datawarehouse.

        Args:
            df (pd.DataFrame): The DataFrame containing data to insert.
        """
        # Create a comma-separated string of column names for the SQL query
        columns = ', '.join(df.columns)
        
        # Create a parameterized query for inserting data
        placeholders = ', '.join(['?' for _ in df.columns])
        insert_query = f"INSERT INTO pc.raw_data ({columns}) VALUES ({placeholders})"
        
        with pyodbc.connect(self.connection_string) as conn:
            cursor = conn.cursor()
            
            for index, row in df.iterrows():
                cursor.execute(insert_query, *row)  # Use unpacking to pass the row values
                
            conn.commit()

def main():
    """
    Main function to execute the workflow of fetching invoices, processing data, 
    and interacting with the datawarehouse.
    """
    base_url = secrets.partner_api_base_url
    client_id = secrets.client_id
    client_secret = secrets.client_secret
    tenant_id = secrets.tenant_id
    invoice_url = secrets.invoice_url
    invoice_line_items_url = secrets.invoice_line_item_url
    scope = secrets.scope
    connection_string = secrets.connection_string
    
    # Initialize API and Datawarehouse clients
    api_client = PartnerCenterAPIClient(base_url, client_id, client_secret, tenant_id, invoice_url, invoice_line_items_url, scope)
    fabric_dw = FabricDatawarehouse(connection_string)

    try:
        access_token = api_client.get_access_token()

        # Fetch invoice IDs
        invoice_ids = api_client.get_invoice_ids()

        # Filter invoices and find matching months
        matching_invoice_ids = api_client.filter_invoices(invoice_ids)

        if matching_invoice_ids:
            invoice_id = matching_invoice_ids[0]
            line_items = api_client.get_invoice_line_items(invoice_id)
            df_line_items = pd.DataFrame(line_items)

            # Add billing_month column to DataFrame
            current_month_minus_1 = (datetime.now().replace(day=1) - pd.DateOffset(months=1)).strftime('%Y-%m')
            df_line_items['billing_month'] = current_month_minus_1

            # Check if current month-1 already exists in the Datawarehouse
            if fabric_dw.check_billing_month(current_month_minus_1):
                print(f"{current_month_minus_1} is already present. Deleting existing records and rewriting.")
                fabric_dw.delete_billing_month(current_month_minus_1)
            
            # Insert the new data
            fabric_dw.insert_data(df_line_items)
            print(f"Data for {current_month_minus_1} inserted successfully.")

        else:
            print("No matching invoices for the current month - 1.")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
