import io
from typing import Optional
from blob_client import AzureBlobDownloader
from blob_url_parser import BlobURLParser
from graph_api_client import GraphAPIClient
from resource_location import ResourceLocationParser
from secret_manager import SecretsManager
from bigquery_writer import BigQueryUploader

def main() -> None:
    """
    Main function to authenticate with Microsoft Graph API, retrieve unbilled usage data, 
    download it from Azure Blob Storage, unzip it, process the JSON to add a `billing_month` field, 
    and upload the data to BigQuery.

    Workflow:
    1. Retrieve secrets from SecretsManager.
    2. Authenticate using GraphAPIClient and retrieve an access token.
    3. Initialize an unbilled request and check its operation status.
    4. Parse the resource location and SAS token to obtain blob storage information.
    5. Download a gzipped JSON file from Azure Blob Storage into an in-memory stream.
    6. Unzip the file and process the data by adding a billing_month field.
    7. Upload the processed data to BigQuery.
    """
    # Step 1: Retrieve secrets from SecretsManager
    print("Retrieving secrets...")
    secrets = SecretsManager()

    # Step 2: Initialize the Graph API client using credentials from SecretsManager
    print("Initializing GraphAPIClient...")
    graph_client = GraphAPIClient(
        tenant_id=secrets.tenant_id,
        client_id=secrets.client_id,
        client_secret=secrets.client_secret,
        scope=secrets.scope
    )

    # Step 3: Authenticate and obtain an access token
    access_token: Optional[str] = graph_client.get_access_token()
    if not access_token:
        raise Exception("Failed to authenticate with Microsoft.")

    # Step 4: Initialize an unbilled request for the current billing period
    print("Initializing unbilled request...")
    headers = graph_client.initialize_unbilled_request(api_url=secrets.unbilled_endpoint, billing_period='current')
    operation_url: Optional[str] = headers.get('Location')
    if not operation_url:
        raise Exception("Failed to initialize unbilled request.")

    # Step 5: Check the status of the unbilled operation request
    result = graph_client.check_operation_status(operation_url)
    
    # Step 6: Parse the resource location details from the response
    resource_location: Optional[str] = result.get('resourceLocation')
    if not resource_location:
        raise Exception("Resource location not found in the result.")

    # Step 7: Parse the resource location to extract storage account details
    resource_parser = ResourceLocationParser(resource_location)
    parsed_location = resource_parser.parse_resource_location()
    
    root_directory = parsed_location['rootDirectory']
    sas_token = parsed_location['sasToken']
    blob_name = parsed_location['blobName']

    # Step 8: Extract the storage account name and container name from the root directory URL
    print("Extracting storage account and container information...")
    blob_parser = BlobURLParser(root_directory)
    storage_account_name, container_name = blob_parser.extract_storage_info()
    print(f"Extracted Storage Account Name & Container Name")

    # Step 9: In-memory stream for blob data
    blob_stream = io.BytesIO()

    # Step 10: Initialize the Azure Blob Downloader and download the blob into an in-memory stream
    print("Downloading blob from Azure Blob Storage...")
    downloader = AzureBlobDownloader(storage_account_name, sas_token, container_name, blob_name)
    downloader.download_blob_to_stream(container_name, blob_name, blob_stream)

    # Step 11: Unzip the in-memory stream
    unzipped_stream = downloader.unzip_blob_stream(blob_stream)

    # Step 12: Ensure the unzipped stream contains data
    print("Verifying the unzipped stream...")
    unzipped_stream.seek(0)  # Reset the pointer for further operations
    if not unzipped_stream.read(1):  # Check if there's any data
        raise Exception("Blob download or unzip failed.")
    print("Unzipped stream verification successful.")

    # Step 13: Reset the pointer of unzipped stream for processing
    unzipped_stream.seek(0)

    # Step 14: Process the unzipped stream to add the `billing_month` field
    json_data = downloader.process_stream_to_json_with_billing_month(unzipped_stream)

    # Step 15: Initialize BigQueryUploader with credentials from SecretsManager
    print("Initializing BigQueryUploader...")
    uploader = BigQueryUploader(
        project_id=secrets.project_id,
        dataset_id=secrets.dataset_id,
        table_id=secrets.table_id
    )

    # Step 16: Ensure the BigQuery table exists or will be created on upload
    print("Ensuring the BigQuery table exists...")
    uploader.create_table_if_not_exists()

    # Step 17: Upload the processed data to BigQuery
    print("Uploading data to BigQuery...")
    uploader.upload_data(json_data)
    print("Processed blob data with billing month uploaded to BigQuery successfully.")

if __name__ == "__main__":
    main()
