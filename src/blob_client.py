import io
import gzip
from azure.storage.blob import BlobServiceClient
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
        blob_service_client = BlobServiceClient(account_url=self.account_url, credential=self.sas_token)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        download_stream = blob_client.download_blob()
        download_stream.readinto(stream)  # Writes data into the provided stream
        stream.seek(0)  # Reset stream pointer to the beginning

    def unzip_blob_stream(self, compressed_stream: io.BytesIO) -> io.BytesIO:
        """
        Unzip a compressed in-memory stream and return a new in-memory stream with the unzipped content.

        Args:
            compressed_stream (io.BytesIO): The in-memory stream containing compressed data.

        Returns:
            io.BytesIO: A new in-memory stream containing the unzipped data.
        """
        with gzip.GzipFile(fileobj=compressed_stream, mode='rb') as gz:
            unzipped_stream = io.BytesIO(gz.read())
        unzipped_stream.seek(0)  # Reset stream pointer to the beginning
        return unzipped_stream

def main():
    init_resource_location = ResourceLocationParser("""{
        "id": "269cc652-ae8e-4d79-875c-83b9ae24701b",
        "createdDateTime": "2024-09-08T09:37:19.895Z",
        "schemaVersion": "2",
        "dataFormat": "compressedJSON",
        "partitionType": "default",
        "eTag": "8UwOGIikpmtH3gxpD",
        "partnerTenantId": "6e75cca6-47f0-47a3-a928-9d5315750bd9",
        "rootDirectory": "https://adlsreconbuprodeastus201.blob.core.windows.net/billedusagefastpath/v1/PartnerTenantId=6e75cca6-47f0-47a3-a928-9d5315750bd9/BillingMonth=202408/InvoiceId=G058476717/InvoiceVersion=202409080713/Fragment=full/PartitionType=default",
        "sasToken": "skoid=5e84b29d-a991-42fa-8553-a691d7bac68f&sktid=975f013f-7f24-47e8-a7d3-abc4752bf346&skt=2024-09-17T11%3A41%3A03Z&ske=2024-09-18T11%3A41%3A03Z&sks=b&skv=2021-08-06&sv=2021-08-06&se=2024-09-17T12%3A41%3A03Z&sr=d&sp=rl&sdd=7&sig=E4HlgmLWx07YvO%2BElhpkIsiDR1Ezr1lcYcRoKeugkpM%3D",
        "blobCount": 1,
        "blobs": [
            {
                "name": "part-00005-30a2b930-993e-44fa-b5f2-2a16bdaea2b0.c000.json.gz",
                "partitionValue": "default"
            }
        ]
    }""")

    rootDirectory, sasToken, blob_name = init_resource_location.parse_resource_location().values()
    storage_account_name, container_name = BlobURLParser(rootDirectory).extract_storage_info()

    blob_stream = io.BytesIO()
    downloader = AzureBlobDownloader(storage_account_name, sasToken, container_name, blob_name)
    
    # Download blob into the in-memory stream
    downloader.download_blob_to_stream(container_name, blob_name, blob_stream)

    # Unzip the in-memory stream
    unzipped_stream = downloader.unzip_blob_stream(blob_stream)

    # Verify if the unzipped_stream contains data
    unzipped_stream.seek(0)  # Reset the pointer
    if unzipped_stream.read(1):  # Check if there's any data
        print("Blob unzipped and downloaded to memory stream successfully.")
    else:
        print("Blob download and unzip failed.")

    # Reset the stream pointer for further use
    unzipped_stream.seek(0)

if __name__ == "__main__":
    main()
