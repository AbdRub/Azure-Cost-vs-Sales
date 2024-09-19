from typing import Dict, Any
import json

class ResourceLocationParser:
    """
    A class to parse the resourceLocation attribute from a JSON string. 
    This attribute is part of the response returned by check_operation_status method
    of the graph api client class

    Attributes:
        resource_location (Dict[str, Any]): A dictionary representing the resource location data.
    """
    
    def __init__(self, resource_location: str):
        """
        Initializes the ResourceLocationParser with a JSON string representing the resource location.

        Args:
            resource_location (str): A JSON string representing the resource location.
        """
        self.resource_location = json.loads(resource_location)

    def parse_resource_location(self) -> Dict[str, Any]:
        """
        Parse the resource location dictionary and return a new dictionary with the relevant information.

        Returns:
            Dict[str, Any]: A dictionary containing the parsed resource location information. The dictionary includes:
                - 'rootDirectory': The root directory URL.
                - 'sasToken': The SAS token for accessing the blob.
                - 'blobName': The name of the first blob in the 'blobs' list.
        """
        parsed_resources = {
            'rootDirectory': self.resource_location.get('rootDirectory'),
            'sasToken': self.resource_location.get('sasToken'),
            'blobName': self.resource_location.get('blobs', [{}])[0].get('name')
        }
        return parsed_resources
    
    def __repr__(self) -> str:
        """
        Return a string representation of the ResourceLocationParser instance.

        Returns:
            str: A string representation of the root directory, SAS token, and blob name.
        """
        return f"""
        rootDirectory = {self.resource_location.get('rootDirectory')}

        sasToken = {self.resource_location.get('sasToken')}
        
        blobName = {self.resource_location.get('blobs', [{}])[0].get('name')}
"""

# Test

def main() -> None:
    """
    Main function to test the ResourceLocationParser class.
    """
    parsed_resources = ResourceLocationParser("""{
    "id": "7aa0cc3a-9606-452d-b9f5-10a043b31cc2",
    "createdDateTime": "2024-09-17T00:38:17.625Z",
    "schemaVersion": "2",
    "dataFormat": "compressedJSON",
    "partitionType": "default",
    "eTag": "7qR839vAvOmOWA50F",
    "partnerTenantId": "6e75cca6-47f0-47a3-a928-9d5315750bd9",
    "rootDirectory": "https://adlsreconprodeastus2001.blob.core.windows.net/unbilledusagefastpath/v1/202409162348/PartnerTenantId=6e75cca6-47f0-47a3-a928-9d5315750bd9/BillingMonth=202409/Currency=INR/Fragment=full/PartitionType=default",
    "sasToken": "skoid=5e84b29d-a991-42fa-8553-a691d7bac68f&sktid=975f013f-7f24-47e8-a7d3-abc4752bf346&skt=2024-09-17T09%3A43%3A07Z&ske=2024-09-18T09%3A43%3A07Z&sks=b&skv=2021-08-06&sv=2021-08-06&se=2024-09-17T10%3A43%3A07Z&sr=d&sp=rl&sdd=7&sig=EHmFPZ2j3YOy3lcxSJczTuBqv0UVwaJpDVL%2FMlof8mI%3D",
    "blobCount": 1,
    "blobs": [
      {
        "name": "part-00149-5f294d55-e65a-4d27-ba07-49bc5e88df5c.c000.json.gz",
        "partitionValue": "default"
      }]}""")
    
    print(parsed_resources.parse_resource_location())

if __name__ == "__main__":
    main()
