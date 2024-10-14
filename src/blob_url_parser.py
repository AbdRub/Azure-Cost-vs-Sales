from urllib.parse import urlparse

class BlobURLParser:
    """
    A class to parse and extract information from a blob storage URL.

    Attributes:
        url (str): The URL of the blob storage.
        parsed_url (ParseResult): The result of parsing the URL using urlparse.
        storage_account_name (str): The extracted storage account name.
        container_name (str): The extracted container name.
    """
    
    def __init__(self, url: str):
        """
        Initializes the BlobURLParser with a URL.

        Args:
            url (str): The URL of the blob storage.
        """
        self.parsed_url = urlparse(url)

    def extract_storage_info(self) -> tuple[str, str]:
        """
        Extract the storage account name and container name from the URL.

        Returns:
            tuple[str, str]: A tuple containing the storage account name and the container name.

        Raises:
            ValueError: If the URL does not contain enough parts to extract the storage account and container name.
        """
        # Extract the storage account name and container name
        path_parts = self.parsed_url.path.strip('/').split('/')

        if len(path_parts) < 1:
            raise ValueError("URL does not contain enough parts to extract storage account and container name.")

        # Storage account name is the first part after "https://"
        self.storage_account_name = "https://" + self.parsed_url.netloc

        # Container name is the rest of the path after the storage account name
        self.container_name = '/'.join(path_parts)

        return self.storage_account_name, self.container_name

# Example URL
if __name__ == "__main__":
    url = "https://adlsreconprodeastus2001.blob.core.windows.net/unbilledusagefastpath/v1/202410021222/PartnerTenantId=6e75cca6-47f0-47a3-a928-9d5315750bd9/BillingMonth=202410/Currency=INR/Fragment=full/PartitionType=default"

    storage_account_name, container_name = BlobURLParser(url).extract_storage_info()
    # print("Storage Account Name:", storage_account_name)
    # print("Container Name:", container_name)
