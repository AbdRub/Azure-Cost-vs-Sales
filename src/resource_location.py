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
    
    def __init__(self, resource_location: dict):
        """
        Initializes the ResourceLocationParser with a JSON string representing the resource location.

        Args:
            resource_location (dict): A dict representing the resource location.
        """
        self.resource_location = resource_location

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
    "id": "5c8c940e-732c-4fff-ad52-59e6104bfa9a",
    "createdDateTime": "2024-10-03T14:19:47.543Z",
    "schemaVersion": "2",
    "dataFormat": "compressedJSON",
    "partitionType": "default",
    "eTag": "Dn5exsggssBg7NoT2",
    "partnerTenantId": "6e75cca6-47f0-47a3-a928-9d5315750bd9",
    "rootDirectory": "https://adlsreconprodeastus2001.blob.core.windows.net/unbilledusagefastpath/v1/202410031156/PartnerTenantId=6e75cca6-47f0-47a3-a928-9d5315750bd9/BillingMonth=202410/Currency=INR/Fragment=full/PartitionType=default",
    "sasToken": "skoid=5e84b29d-a991-42fa-8553-a691d7bac68f&sktid=975f013f-7f24-47e8-a7d3-abc4752bf346&skt=2024-10-03T17%3A04%3A58Z&ske=2024-10-04T17%3A04%3A58Z&sks=b&skv=2021-08-06&sv=2021-08-06&se=2024-10-03T18%3A04%3A58Z&sr=d&sp=rl&sdd=7&sig=9y11IgmFHymP22aR0apB3xmvHfbyo9vEP51fwShGKD8%3D",
    "blobCount": 1,
    "blobs": [
      {
        "name": "part-00103-94a51804-4bc7-4574-9fd3-50caec206fca.c000.json.gz",
        "partitionValue": "default"
      }]}""")
    
    print(parsed_resources.parse_resource_location())

if __name__ == "__main__":
    main()
