import io
import json
import csv
import time
import os
from datetime import datetime
from blob_client import AzureBlobDownloader
from blob_url_parser import BlobURLParser
from graph_api_client import GraphAPIClient
from resource_location import ResourceLocationParser
from secret_manager import SecretsManager

def count_rows_in_stream(stream: io.BytesIO) -> int:
    """
    Count the number of rows in the unzipped JSON stream.
    
    Args:
        stream (io.BytesIO): The in-memory stream of the unzipped file.
    
    Returns:
        int: The number of rows in the file.
    """
    stream.seek(0)  # Reset pointer to the beginning
    row_count = 0
    
    for line in stream:
        row_count += 1

    return row_count

def write_row_count_to_file(row_count: int) -> None:
    """
    Write the row count to a file with a timestamp.
    
    Args:
        row_count (int): The number of rows counted.
    """
    timestamp = datetime.now().strftime("%H:%M")
    row_count_msg = f"The row_count at {timestamp} is {row_count}\n"
    
    # Append row count to a file
    with open('row_count_log.txt', 'a') as file:
        file.write(row_count_msg)
    print(row_count_msg)

def main() -> None:
    """
    Main function to download a file, unzip it, count rows, and log the row count.
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
    
    # Reset the pointer of unzipped stream for row counting
    unzipped_stream.seek(0)

    # Count the number of rows in the unzipped file
    row_count = count_rows_in_stream(unzipped_stream)

    # Log the row count with the current timestamp
    write_row_count_to_file(row_count)

    # Save the unzipped stream locally
    unzipped_stream.seek(0)
    with open('unzipped_data.json', 'wb') as f:
        f.write(unzipped_stream.read())

    print("File downloaded and row count logged.")
    
    # Commenting out BigQuery upload for now
    # uploader = BigQueryUploader(
    #     project_id=secrets.project_id,
    #     dataset_id=secrets.dataset_id,
    #     table_id=secrets.table_id
    # )
    # uploader.upload_data(unzipped_stream)
    
    print("BigQuery upload part is commented out.")

if __name__ == "__main__":
    while True:
        main()
        time.sleep(3600)  # Wait for one hour before running the process again
