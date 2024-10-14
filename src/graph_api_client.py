import io
import json
import time
import requests
from typing import Optional, Dict
# from bigquery_writer import BigQueryUploader
from blob_client import AzureBlobDownloader
from blob_url_parser import BlobURLParser
from resource_location import ResourceLocationParser
from secret_manager import SecretsManager


class GraphAPIClient:
    def __init__(self, tenant_id: str, client_id: str, client_secret: str, scope: str) -> None:
        """
        Initialize the GraphAPIClient with tenant, client, and authentication information.

        Args:
            tenant_id (str): The tenant ID for the Azure Active Directory.
            client_id (str): The client ID for the Azure app.
            client_secret (str): The client secret for the Azure app.
            scope (str): The scope of the access required.
        """
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.scope = scope
        self.access_token: Optional[str] = None
        self.base_token_url = f'https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token'
        print('Graph API Client initialized.')

    def get_access_token(self) -> str:
        """
        Authenticate with Microsoft to obtain an access token.

        Returns:
            str: The access token if the authentication is successful.

        Raises:
            Exception: If authentication fails due to invalid credentials or other errors.
        """
        token_url = self.base_token_url
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        body = {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'scope': self.scope
        }

        print(f"Authenticating with Microsoft Graph API...")
        response = requests.post(token_url, data=body, headers=headers)
        
        if response.status_code == 200:
            token_data = response.json()
            self.access_token = token_data['access_token']
            print("Token retrieved successfully.")
        else:
            print(f"Authentication failed. Status code: {response.status_code}")
            raise Exception(f"Failed to authenticate: {response.status_code}, {response.content}")

        return self.access_token

    def initialize_unbilled_request(self, api_url: str, billing_period: str) -> Dict[str, str]:
        """
        Submit a request to generate a billing report based on the billing period.

        Args:
            api_url (str): The API URL to submit the billing request.
            billing_period (str): The billing period for which to generate the billing report.

        Returns:
            Dict[str, str]: A dictionary containing headers, including the URL to check the operation status.

        Raises:
            Exception: If the request fails due to missing tokens or server errors.
        """
        if not self.access_token:
            raise Exception("Access token is missing. Authenticate first.")

        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        body = {
            "currencyCode": "INR",
            "billingPeriod": billing_period,
            "attributeSet": "full"
        }

        response = requests.post(api_url, headers=headers, json=body)

        if response.status_code == 202:
            print('Request accepted. Processing has started.')
            return response.headers
        else:
            raise Exception(f"Failed to make request. Status code: {response.status_code}. Content: {response.content}")
        

    def check_operation_status(self, operation_url: str) -> dict:
        """
        Poll the operation status until it completes.

        Args:
            operation_url (str): The URL to check the operation status.

        Returns:
            Dict: A dictionary containing the final status and any additional details.

        Raises:
            Exception: If the request fails or the access token is invalid.
        """
        if not self.access_token:
            raise Exception("Access token is missing. Authenticate first.")

        headers = {
            'Authorization': f'Bearer {self.access_token}'
        }

        while True:
            response = requests.get(operation_url, headers=headers)
            
            if response.status_code in [401, 403]:
                raise Exception("Access token is expired or invalid. Please refresh the token and try again.")
            elif response.status_code == 200:
                json_data = response.json()
                # print('type json data:', type(json_data))

                status = json_data.get('status')
                print(f"Status: {status}")
                # print(json.dumps(json_data, indent=2))

                if status in ['succeeded', 'failed']:
                    return json_data

                retry_after = int(response.headers.get('Retry-After', 10))
                print(f"Retrying after {retry_after} seconds...")
                time.sleep(retry_after)
            else:
                raise Exception(f"Request failed. Status code: {response.status_code}. Response: {response.content}")


# Example usage of GraphAPIClient
def main() -> None:
    """
    Demonstrates how to use the GraphAPIClient class.
    """
    secrets = SecretsManager()

    graph_client = GraphAPIClient(
        tenant_id=secrets.tenant_id,
        client_id=secrets.client_id,
        client_secret=secrets.client_secret,
        scope=secrets.scope
    )

    # Test Auth
    access_token = graph_client.get_access_token()
    if not access_token:
        raise Exception("Failed to authenticate with Microsoft.")
    
    # Test unbilled request
    headers = graph_client.initialize_unbilled_request(api_url=secrets.unbilled_endpoint, billing_period='current')
    operation_url = headers.get('Location')
    if not operation_url:
        raise Exception("Failed to initialize unbilled request.")
    
    # Test check status
    result = graph_client.check_operation_status(operation_url)

    resource_location_json = json.dumps(result.get('resourceLocation'))
    
    # init_resource_location = ResourceLocationParser(resource_location_json)
    
    # Extract information
    # rootDirectory, sasToken, blob_name = init_resource_location.parse_resource_location().values()
    # storage_account_name, container_name = BlobURLParser(rootDirectory).extract_storage_info()
    
    # Use in-memory stream
    # blob_stream = io.BytesIO()
    
    # # Initialize AzureBlobDownloader
    # downloader = AzureBlobDownloader(storage_account_name, sasToken, container_name, blob_name)

    # try:
    #     # Download blob into the in-memory stream
    #     downloader.download_blob_to_stream(container_name, blob_name, blob_stream)

    #     # Unzip the in-memory stream
    #     unzipped_stream = downloader.unzip_blob_stream(blob_stream)

    #     # Verify if the unzipped_stream contains data
    #     unzipped_stream.seek(0)  # Reset the pointer
       
    #     if unzipped_stream.read(1):  # Check if there's any data
    #         print("Blob unzipped and downloaded to memory stream successfully.")
    #         unzipped_stream.seek(0)  # Reset pointer again after reading
    #         print(unzipped_stream)
            
    #         # Initialize BigQueryUploader
    #         uploader = BigQueryUploader(
    #             project_id=secrets.project_id,
    #             dataset_id=secrets.dataset_id,
    #             table_id=secrets.table_id
    #         )

    #         # Ensure the table exists or will be created
    #         uploader.create_table_if_not_exists()

    #         # Upload the unzipped stream to BigQuery
    #         uploader.upload_data(unzipped_stream)

    #     else:
    #         print("Blob download and unzip failed.")
    # except Exception as e:
    #     print(f"Error occurred: {e}")
    # finally:
    #     # Reset the stream pointer for further use
    #     blob_stream.seek(0)
    #     if 'unzipped_stream' in locals():
    #         unzipped_stream.seek(0)  # Ensure this is also reset if it was created


if __name__ == "__main__":
    main()
