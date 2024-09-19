import json
from blob_client import AzureBlobDownloader
from blob_url_parser import BlobURLParser
from graph_api_client import GraphAPIClient
from resource_location import ResourceLocationParser
from secret_manager import SecretsManager
from unzipper import BigQueryWriter, GzipExtractor

def main() -> None:
    """
    Main function that demonstrates the usage of several components such as 
    SecretsManager, GraphAPIClient, AzureBlobDownloader, and BlobURLParser 
    to authenticate with Microsoft Graph API, retrieve unbilled usage data, 
    and download it from Azure Blob Storage.

    Workflow:
    1. Retrieve secrets from SecretsManager.
    2. Authenticate using GraphAPIClient and retrieve an access token.
    3. Initialize an unbilled request and check its operation status.
    4. Parse the resource location and SAS token to obtain blob storage information.
    5. Download a file (gzipped JSON) from the Azure Blob Storage.

    Raises:
        Exception: If authentication or any subsequent step fails.
    """
    secrets = SecretsManager()

    # Initialize the Graph API client using credentials from SecretsManager
    graph_client = GraphAPIClient(
        tenant_id=secrets.tenant_id,
        client_id=secrets.client_id,
        client_secret=secrets.client_secret,
        scope=secrets.scope
    )

    # Authenticate and obtain an access token
    access_token = graph_client.get_access_token()
    if not access_token:
        raise Exception("Failed to authenticate with Microsoft.")
    
    # Initialize an unbilled request for the current billing period
    headers = graph_client.initialize_unbilled_request(api_url=secrets.unbilled_endpoint, billing_period='current')
    operation_url = headers.get('Location')
    if not operation_url:
        raise Exception("Failed to initialize unbilled request.")
    
    # Check the status of the unbilled operation request
    result = graph_client.check_operation_status(operation_url)
    
    # Parse the resource location details from the response
    resouceLocationJSON = json.dumps(result.get('resourceLocation'))
    init_resource_location = ResourceLocationParser(resouceLocationJSON)
    
    # Extract the root directory, SAS token, and blob name from the resource location
    rootDirectory, sasToken, blob_name = init_resource_location.parse_resource_location().values()

    # Extract the storage account name and container name from the root directory URL
    storage_account_name, container_name = BlobURLParser(rootDirectory).extract_storage_info()

    # Define the local path for downloading the blob
    download_file_path = "./blob.json.gz"

    # Initialize the Azure Blob Downloader and download the blob to the specified path
    downloader = AzureBlobDownloader(storage_account_name, sasToken, container_name, blob_name)
    downloader.download_blob(container_name, blob_name, download_file_path)


if __name__ == "__main__":
    main()
