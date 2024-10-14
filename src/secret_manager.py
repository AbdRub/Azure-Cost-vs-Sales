import os
from dotenv import load_dotenv


class SecretsManager:
    def __init__(self, env_file=r'C:\Users\BRIO-LT-Subham\Documents\PROJECTS\Brio-Internal-projects\Azure-Cost-vs-Sales\secrets.env'):
        # Load the environment file
        if load_dotenv(env_file):

        # Load secrets
            self.partner_api_base_url = os.getenv('PARTNER_API_BASE_URL')
            self.tenant_id = os.getenv('TENANT_ID')
            self.client_id = os.getenv('CLIENT_ID')
            self.client_secret = os.getenv('CLIENT_SECRET')
            self.scope = os.getenv('SCOPE')
            self.graph_base_url = os.getenv('GRAPH_BASE_URL')
            self.billed_endpoint = os.getenv('BILLED_ENDPOINT')
            self.unbilled_endpoint = os.getenv('UNBILLED_ENDPOINT')
            self.project_id = os.getenv('PROJECT_ID')
            self.dataset_id = os.getenv('DATASET_ID')
            self.table_id = os.getenv('TABLE_ID')
            self.invoice_url = os.getenv('INVOICE_URL')
            self.invoice_line_item_url = os.getenv('INVOICE_LINE_ITEMS_URL')
            self.fabric_server = os.getenv('FABRIC_SERVER')
            self.fabric_client_id = os.getenv('FABRIC_CLIENT_ID')
            self.fabric_client_secret = os.getenv('FABRIC_CLIENT_SECRET')
            self.blob_container_name = os.getenv('BLOB_CONTAINER_NAME')
            self.blob_connection_string = os.getenv('BLOB_CONNECTION_STRING')
        else:
            print("couldnt load env.")
            return

    def __bool__(self):
        # Check if none of the attributes are None
        return all([
            self.partner_api_base_url,
            self.tenant_id,
            self.client_id,
            self.client_secret,
            self.scope,
            self.graph_base_url
        ])
    
    def __repr__(self):
        return f"SecretsManager({self.__dict__})"


# Test the __bool__ method in the __main__ block
def test_secrets_manager(secrets:SecretsManager):
    """Test if the __bool__ method correctly verifies that no attribute is None"""

    assert bool(secrets),"SecretsManager failed to load required secrets."
    print("Test Success: Secrets loaded successfully!")


if __name__ == "__main__":
    secrets = SecretsManager()
    test_secrets_manager(secrets)
    print(secrets)