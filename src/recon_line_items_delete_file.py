from azure.storage.blob import BlobServiceClient, BlobClient
from secret_manager import SecretsManager
import requests

secrets = SecretsManager()

def delete_blob_file(connection_string: str, container_name: str, blob_name: str):
    """
    Deletes a file (blob) from the specified Azure Blob Storage container.
    
    Args:
        connection_string (str): Azure Storage account connection string.
        container_name (str): Name of the container in the storage account.
        blob_name (str): Name of the file/blob to be deleted.
    """
    try:
        # Initialize the BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        # Get the client for the blob you want to delete
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        
        # Delete the blob
        blob_client.delete_blob()
        print(f"Blob '{blob_name}' deleted successfully from container '{container_name}'.")

    except Exception as e:
        print(f"An error occurred: {e}")
def suspend_fabric_capacity():
    """
    Calls the API to resume Fabric capacity and waits 30 seconds to allow the capacity to be fully available.
    """
    resume_url = "https://prod2-03.centralindia.logic.azure.com:443/workflows/3abceea03f6d477a999aab317aae1f0b/triggers/When_a_HTTP_request_is_received/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2FWhen_a_HTTP_request_is_received%2Frun&sv=1.0&sig=9GidnrLPemnX8ekgH1Abdi2lejeclnGZgNW9-6-e1J8"
    
    body = {"Action": "suspend"}
    headers = {'Content-Type': 'application/json'}

    response = requests.post(resume_url, json=body, headers=headers)


# Example usage:
def main():
    # Load secrets for Azure Blob Storage connection
    connection_string = secrets.blob_connection_string
    container_name = secrets.blob_container_name
    blob_name = "invoice_line_items.parquet"  # Replace with the name of the blob you want to delete

    try:
        # Delete the specified blob
        delete_blob_file(connection_string, container_name, blob_name)

        # After deleting the blob, suspend Fabric capacity
        print("Suspending Fabric capacity...")
        suspend_fabric_capacity()
    
    except Exception as e:
        print(f"An error occurred in the main function: {e}")

if __name__ == "__main__":
    main()
