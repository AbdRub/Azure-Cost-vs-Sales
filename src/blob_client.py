import io
import gzip
import json
from azure.storage.blob import BlobServiceClient
from datetime import datetime
from blob_url_parser import BlobURLParser 
from resource_location import ResourceLocationParser

class AzureBlobDownloader:

    def __init__(self, account_url, sas_token, container_name, blob_name):
        self.account_url = account_url
        self.sas_token = sas_token
        self.container_name = container_name
        self.blob_name = blob_name

    def download_blob_to_stream(self, container_name: str, blob_name: str, stream: io.BytesIO) -> None:
        """
        Download blob into a provided in-memory stream.

        Args:
            container_name (str): The container name.
            blob_name (str): The blob name.
            stream (io.BytesIO): The stream to download the blob to.
        """
        # print(f"Downloading blob '{blob_name}' from container '{container_name}'...")
        blob_service_client = BlobServiceClient(account_url=self.account_url, credential=self.sas_token)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        downloaded_stream = blob_client.download_blob()
        downloaded_stream.readinto(stream)  # Writes data into the provided stream
        stream.seek(0)  # Reset stream pointer to the beginning
        print("Blob downloaded successfully.")
        return downloaded_stream

    def unzip_blob_stream(self, compressed_stream: io.BytesIO) -> io.BytesIO:
        """
        Unzip a compressed in-memory stream and return a new in-memory stream with the unzipped content.

        Args:
            compressed_stream (io.BytesIO): The in-memory stream containing compressed data.

        Returns:
            io.BytesIO: A new in-memory stream containing the unzipped data.
        """
        print("Unzipping the blob data...")
        with gzip.GzipFile(fileobj=compressed_stream, mode='rb') as gz:
            unzipped_stream = io.BytesIO(gz.read())
        unzipped_stream.seek(0)  # Reset stream pointer to the beginning
        print("Blob unzipped successfully.")
        return unzipped_stream

    def process_stream_to_json_with_billing_month(self, unzipped_stream: io.BytesIO) -> list:
        """
        Converts the unzipped stream of line items into valid JSON format and appends a billing_month field.

        Args:
            unzipped_stream (io.BytesIO): The in-memory unzipped stream containing JSON line items.

        Returns:
            list: A list of JSON objects with the added billing_month field.
        """
        print("Processing the unzipped stream and adding billing_month...")
        unzipped_stream.seek(0)  # Ensure stream is at the start
        processed_data = []
        
        # Get current year and month
        current_year = datetime.now().year
        current_month = datetime.now().month
        
        # Format billing_month as "YYYY-MM-01"
        billing_month = f"{current_year}-{current_month:02d}-01"
        
        # Read the stream line by line
        for line in unzipped_stream:
            # Convert the line to a JSON object
            json_obj = json.loads(line.decode('utf-8'))
            
            # Add the billing_month field
            json_obj["billing_month"] = billing_month
            
            # Append the processed JSON object to the list
            processed_data.append(json_obj)
        
        print(f"Added billing_month '{billing_month}' to {len(processed_data)} records.")
        return processed_data

def main():
    # Simulate parsing resource location
    init_resource_location = ResourceLocationParser("""{
    "id": "b8d16d34-910e-47e4-8fa5-c934976a9a11",
    "createdDateTime": "2024-10-08T00:57:36.037Z",
    "schemaVersion": "2",
    "dataFormat": "compressedJSON",
    "partitionType": "default",
    "eTag": "69D0qrTA8NTziitwV",
    "partnerTenantId": "6e75cca6-47f0-47a3-a928-9d5315750bd9",
    "rootDirectory": "https://adlsreconprodeastus2001.blob.core.windows.net/unbilledusagefastpath/v1/202410080035/PartnerTenantId=6e75cca6-47f0-47a3-a928-9d5315750bd9/BillingMonth=202410/Currency=INR/Fragment=full/PartitionType=default",
    "sasToken": "skoid=5e84b29d-a991-42fa-8553-a691d7bac68f&sktid=975f013f-7f24-47e8-a7d3-abc4752bf346&skt=2024-10-08T06%3A16%3A29Z&ske=2024-10-09T06%3A16%3A29Z&sks=b&skv=2021-08-06&sv=2021-08-06&se=2024-10-08T07%3A16%3A29Z&sr=d&sp=rl&sdd=7&sig=HG402TdxXsajOiGJRFJ7Tr7XpUEtm4J1OyIh59dkVic%3D",
    "blobCount": 1,
    "blobs": [
      {
        "name": "part-00103-9a209a5e-0378-4bb5-9b6a-75bb0fe7ae84.c000.json.gz",
        "partitionValue": "default"
      }]}""")

    rootDirectory, sasToken, blob_name = init_resource_location.parse_resource_location().values()
    storage_account_name, container_name = BlobURLParser(rootDirectory).extract_storage_info()

    downloader = AzureBlobDownloader(storage_account_name, sasToken, container_name, blob_name)
    
    # Download blob into the in-memory stream
    downloaded_stream = io.BytesIO()  # Create an empty BytesIO stream
    downloader.download_blob_to_stream(container_name, blob_name, downloaded_stream)

    # Unzip the in-memory stream
    unzipped_stream = downloader.unzip_blob_stream(downloaded_stream)

    # Process the unzipped stream to add billing_month
    processed_data = downloader.process_stream_to_json_with_billing_month(unzipped_stream)
        
    # Output the processed data (you can write it to BigQuery, file, etc.)
    for item in processed_data:
        print(json.dumps(item, indent=4))

    # Further processing can be done here (e.g., uploading to BigQuery)
    print("Processing complete. Billing month added to all records.")

if __name__ == "__main__":
    main()
