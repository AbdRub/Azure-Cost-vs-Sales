  # Import your BigQueryUploader
import io
import json
from blob_client import AzureBlobDownloader
from blob_url_parser import BlobURLParser
from graph_api_client import GraphAPIClient
from resource_location import ResourceLocationParser
from secret_manager import SecretsManager
from bigquery_writer import BigQueryUploader

def main() ->None:

    """
    Main function that demonstrates the usage of several components such as 
    SecretsManager, GraphAPIClient, AzureBlobDownloader, BlobURLParser, 
    and BigQueryUploader to authenticate with Microsoft Graph API, 
    retrieve unbilled usage data, download it from Azure Blob Storage, 
    unzip it, and upload the data to BigQuery.

    Workflow:
    1. Retrieve secrets from SecretsManager.
    2. Authenticate using GraphAPIClient and retrieve an access token.
    3. Initialize an unbilled request and check its operation status.
    4. Parse the resource location and SAS token to obtain blob storage information.
    5. Download a gzipped JSON file from Azure Blob Storage into an in-memory stream.
    6. Unzip the file and upload the data to BigQuery.

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
    resource_location_json = json.dumps(result.get('resourceLocation'))
    init_resource_location = ResourceLocationParser(resource_location_json)
    
    # Extract the root directory, SAS token, and blob name from the resource location
    root_directory, sas_token, blob_name = init_resource_location.parse_resource_location().values()

    # Extract the storage account name and container name from the root directory URL
    storage_account_name, container_name = BlobURLParser(root_directory).extract_storage_info()

    # In-memory stream for blob data
    blob_stream = io.BytesIO()

    # Initialize the Azure Blob Downloader and download the blob into an in-memory stream
    downloader = AzureBlobDownloader(storage_account_name, sas_token, container_name, blob_name)
    downloader.download_blob_to_stream(container_name, blob_name, blob_stream)

    # Unzip the in-memory stream
    unzipped_stream = downloader.unzip_blob_stream(blob_stream)

    # Ensure the unzipped stream contains data
    unzipped_stream.seek(0)  # Reset the pointer for further operations
    if not unzipped_stream.read(1):  # Check if there's any data
        raise Exception("Blob download or unzip failed.")
    
    # Reset the pointer of unzipped stream for BigQuery uploading
    unzipped_stream.seek(0)

    # Initialize BigQueryUploader with credentials from SecretsManager
    uploader = BigQueryUploader(
        project_id=secrets.project_id,
        dataset_id=secrets.dataset_id,
        table_id=secrets.table_id
    )

    # Ensure the BigQuery table exists or will be created on upload
    uploader.create_table_if_not_exists()

    # Upload the unzipped data to BigQuery
    uploader.upload_data(unzipped_stream)
    print("Unzipped blob data uploaded to BigQuery successfully.")

if __name__ == "__main__":
    main()
