import os
import requests
from dotenv import load_dotenv
import zipfile
import io
import time

# Load environment variables from secrets.env
load_dotenv('../secrets.env')

# Fetch the tenant_id, client_id, client_secret, and scope from the .env file
tenant_id = os.getenv("TENANT_ID")
client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")
scope = os.getenv("SCOPE")
grant_type = os.getenv("GRANT_TYPE")

# Function to get access token
def get_access_token():
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    body = {
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": scope,
        "grant_type": grant_type
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    try:
        response = requests.post(url, data=body, headers=headers)
        response.raise_for_status()
        token_info = response.json()
        return token_info.get('access_token')
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err} - Status code: {response.status_code}")
        print(f"Response content: {response.text}")
    except requests.exceptions.RequestException as req_err:
        print(f"Request error occurred: {req_err}")
    except Exception as err:
        print(f"An error occurred: {err}")
    return None

# Function to submit billing usage request
def submit_billing_usage_request(access_token):
    url = "https://graph.microsoft.com/v1.0/reports/partners/billing/usage/unbilled/export"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    body = {
        "currencyCode": "INR",
        "billingPeriod": "current"
    }
    try:
        response = requests.post(url, json=body, headers=headers)
        response.raise_for_status()
        location_url = response.headers.get('Location')
        return location_url
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err} - Status code: {response.status_code}")
        print(f"Response content: {response.text}")
    except requests.exceptions.RequestException as req_err:
        print(f"Request error occurred: {req_err}")
    except Exception as err:
        print(f"An error occurred: {err}")
    return None

# Function to check unbilling usage status
def check_billing_usage_status(access_token, location_url):
    try:
        while True:
            response = requests.get(location_url, headers={"Authorization": f"Bearer {access_token}"})
            response.raise_for_status()
            
            status_info = response.json()
            status = status_info.get('status')
            print(f"Billing usage request status: {status}")
            
            if status in ['failed', 'succeeded']:
                return status_info  # Return status_info to be used later
            elif status == 'processing':
                retry_after = int(response.headers.get('Retry-After', 30))  # Default to 30 seconds if not provided
                print(f"Processing, retrying after {retry_after} seconds...")
                time.sleep(retry_after)
            else:
                raise Exception(f"Unexpected status: {status}")
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err} - Status code: {response.status_code}")
        print(f"Response content: {response.text}")
    except requests.exceptions.RequestException as req_err:
        print(f"Request error occurred: {req_err}")
    except Exception as err:
        print(f"An error occurred: {err}")
    return None  # Return None if there's an error or status is not found

# Function to get invoice IDs
def get_invoice_ids(access_token):
    url = "https://api.partnercenter.microsoft.com/v1/invoices?size=200&offset=0"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        invoices = response.json()
        invoice_ids = [invoice['id'] for invoice in invoices.get('items', [])]
        return invoice_ids
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err} - Status code: {response.status_code}")
        print(f"Response content: {response.text}")
    except requests.exceptions.RequestException as req_err:
        print(f"Request error occurred: {req_err}")
    except Exception as err:
        print(f"An error occurred: {err}")
    return []

# Function to download and handle the file from Azure Blob Storage
def download_and_process_blob(status_info, output_directory):
    try:
        # Extract rootDirectory and SAS token from status_info
        root_directory = status_info.get('resourceLocation').get('rootDirectory')
        sas_token = status_info.get('resourceLocation').get('sasToken')
        
        # Construct the URL for listing blobs in the root directory
        list_blobs_url = f"{root_directory}?{sas_token}"
        print(f"Listing blobs URL: {list_blobs_url}")
        
        # Download the list of blobs
        response = requests.get(list_blobs_url)
        response.raise_for_status()
        
        # Check if the response is empty
        if not response.content:
            print("No content in the response.")
            return
        
        try:
            blobs_info = response.json()
        except ValueError:
            print(f"Response is not in JSON format. Response content: {response.text}")
            return
        
        # Construct the URL for the blob
        blob_url = f"{root_directory}/?{sas_token}"
        print(f"Blob URL: {blob_url}")

        # Download the blob
        blob_response = requests.get(blob_url)
        blob_response.raise_for_status()

        # Check if the file is a zip file
        content_type = blob_response.headers.get('Content-Type', '')
        if 'application/zip' in content_type:
            # Handle zip file
            with zipfile.ZipFile(io.BytesIO(blob_response.content)) as z:
                z.extractall(output_directory)
                print(f"Extracted files to {output_directory}")
        else:
            # Handle non-zip file
            with open(os.path.join(output_directory, blob_url), 'wb') as f:
                f.write(blob_response.content)
                print(f"Downloaded file to {output_directory}")

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err} - Status code: {response.status_code}")
        print(f"Response content: {response.text}")
    except requests.exceptions.RequestException as req_err:
        print(f"Request error occurred: {req_err}")
    except Exception as err:
        print(f"An error occurred: {err}")



# Main execution
def main():
    access_token = get_access_token()
    
    if access_token:
        location_url = submit_billing_usage_request(access_token)
        if location_url:
            status_info = check_billing_usage_status(access_token, location_url)
            if status_info and status_info.get('status') == 'succeeded':
                output_directory = "path/to/output/directory"  # Define your output directory
                download_and_process_blob(status_info, output_directory)
            else:
                print("Failed to get valid status info or status not succeeded.")
        else:
            print("Failed to submit billing usage request.")
    else:
        print("Failed to acquire access token.")

if __name__ == "__main__":
    main()