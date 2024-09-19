import requests


class PartnerCenterAPIClient:
    def __init__(self, base_url: str, client_id: str, client_secret: str, tenant_id: str, scope: str):
        self.base_url = base_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.scope = scope
        self.access_token = None

    def authenticate(self) -> str:
        """
        Authenticate with Microsoft to get the access token.
        """
        token_url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        body = {
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'scope': self.scope,
            'grant_type': 'client_credentials'
        }

        response = requests.post(token_url, data=body, headers=headers)
        if response.status_code == 200:
            token_data = response.json()
            self.access_token = token_data['access_token']
            print("Token retrieved successfully.")
        else:
            raise Exception(f"Failed to authenticate: {response.status_code}, {response.content}")

        return self.access_token

    def get_invoice_ids(self) -> list:
        """
        Get invoice IDs from the Microsoft Partner Center API.
        """
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        
        # Fetch invoices from Partner Center API
        response = requests.get(f"{self.base_url}/v1/invoices?size=200&offset=0", headers=headers)
        if response.status_code == 200:
            return response.json().get('items', [])
        else:
            raise Exception(f"Error fetching invoices: {response.status_code}, {response.content}")
