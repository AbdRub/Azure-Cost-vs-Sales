import io
import json
from google.cloud import bigquery
from google.cloud.bigquery import WriteDisposition
from secret_manager import SecretsManager
from blob_client import AzureBlobDownloader
from blob_url_parser import BlobURLParser
from resource_location import ResourceLocationParser
from datetime import datetime


class BigQueryUploader:
    def __init__(self, project_id: str, dataset_id: str, table_id: str) -> None:
        """
        Initialize the BigQueryUploader with the necessary project, dataset, and table details.

        Args:
            project_id (str): The Google Cloud project ID.
            dataset_id (str): The BigQuery dataset ID.
            table_id (str): The BigQuery table ID.
        """
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.client = bigquery.Client(project=self.project_id)
        self.table_ref = self.client.dataset(self.dataset_id).table(self.table_id)

    def create_table_if_not_exists(self) -> None:
        """
        Create the BigQuery table if it doesn't already exist, using a predefined schema.
        """
        schema = self._get_explicit_schema()  # Use the explicit schema

        table = bigquery.Table(self.table_ref, schema=schema)
        try:
            self.client.get_table(self.table_ref)  # Check if table exists
            print(f"Table {self.table_id} already exists.")
        except Exception:
            # If the table does not exist, create it
            self.client.create_table(table)
            print(f"Table {self.table_id} created successfully.")

    def _get_explicit_schema(self) -> list:
        """
        Define the explicit schema with the desired field order.
        
        Returns:
            list: A list of BigQuery SchemaField objects in the desired order.
        """
        return [
            bigquery.SchemaField("PartnerId", "STRING"),
            bigquery.SchemaField("PartnerName", "STRING"),
            bigquery.SchemaField("CustomerId", "STRING"),
            bigquery.SchemaField("CustomerName", "STRING"),
            bigquery.SchemaField("CustomerDomainName", "STRING"),
            bigquery.SchemaField("CustomerCountry", "STRING"),
            bigquery.SchemaField("MpnId", "STRING"),
            bigquery.SchemaField("Tier2MpnId", "STRING"),
            bigquery.SchemaField("ProductId", "STRING"),
            bigquery.SchemaField("SkuId", "STRING"),
            bigquery.SchemaField("AvailabilityId", "STRING"),
            bigquery.SchemaField("SkuName", "STRING"),
            bigquery.SchemaField("ProductName", "STRING"),
            bigquery.SchemaField("PublisherName", "STRING"),
            bigquery.SchemaField("PublisherId", "STRING"),
            bigquery.SchemaField("SubscriptionDescription", "STRING"),
            bigquery.SchemaField("SubscriptionId", "STRING"),
            bigquery.SchemaField("ChargeStartDate", "TIMESTAMP"),
            bigquery.SchemaField("ChargeEndDate", "TIMESTAMP"),
            bigquery.SchemaField("UsageDate", "DATE"),
            bigquery.SchemaField("MeterType", "STRING"),
            bigquery.SchemaField("MeterCategory", "STRING"),
            bigquery.SchemaField("MeterId", "STRING"),
            bigquery.SchemaField("MeterSubCategory", "STRING"),
            bigquery.SchemaField("MeterName", "STRING"),
            bigquery.SchemaField("MeterRegion", "STRING"),
            bigquery.SchemaField("Unit", "STRING"),
            bigquery.SchemaField("ResourceLocation", "STRING"),
            bigquery.SchemaField("ConsumedService", "STRING"),
            bigquery.SchemaField("ResourceGroup", "STRING"),
            bigquery.SchemaField("ResourceURI", "STRING"),
            bigquery.SchemaField("ChargeType", "STRING"),
            bigquery.SchemaField("UnitPrice", "FLOAT"),
            bigquery.SchemaField("Quantity", "FLOAT"),
            bigquery.SchemaField("UnitType", "STRING"),
            bigquery.SchemaField("BillingPreTaxTotal", "FLOAT"),
            bigquery.SchemaField("BillingCurrency", "STRING"),
            bigquery.SchemaField("PricingPreTaxTotal", "FLOAT"),
            bigquery.SchemaField("PricingCurrency", "STRING"),
            bigquery.SchemaField("ServiceInfo1", "STRING"),
            bigquery.SchemaField("ServiceInfo2", "STRING"),
            bigquery.SchemaField("Tags", "STRING"),
            bigquery.SchemaField("AdditionalInfo", "STRING"),
            bigquery.SchemaField("EffectiveUnitPrice", "FLOAT"),
            bigquery.SchemaField("PCToBCExchangeRate", "FLOAT"),
            bigquery.SchemaField("PCToBCExchangeRateDate", "TIMESTAMP"),
            bigquery.SchemaField("EntitlementId", "STRING"),
            bigquery.SchemaField("EntitlementDescription", "STRING"),
            bigquery.SchemaField("PartnerEarnedCreditPercentage", "FLOAT"),
            bigquery.SchemaField("CreditPercentage", "FLOAT"),
            bigquery.SchemaField("CreditType", "STRING"),
            bigquery.SchemaField("BenefitType", "STRING"),
            bigquery.SchemaField("billing_month", "DATE"),
        ]


    def upload_data(self, json_data: list) -> None:
        """
        Uploads data from a list of JSON objects to BigQuery.

        Args:
            json_data (list): The list of JSON dictionaries with billing_month field already included.
        """
        # Ensure the table exists or create it with inferred schema
        self.create_table_if_not_exists()  # Corrected call to create_table_if_not_exists

        # Get the current billing month from the processed data
        if json_data:
            billing_month = json_data[0].get('billing_month')
        else:
            raise ValueError("No data available to infer the billing month.")

        # Delete rows for the current billing month before uploading
        self._delete_existing_rows(billing_month)

        # Define job configuration for inserting data
        job_config = bigquery.LoadJobConfig(
            write_disposition=WriteDisposition.WRITE_APPEND  # Append new data instead of truncating
        )

        # Upload data to BigQuery
        load_job = self.client.load_table_from_json(json_data, self.table_ref, job_config=job_config)
        load_job.result()  # Wait for the job to complete
        print(f"Uploaded {len(json_data)} records to {self.table_id}.")



    def _delete_existing_rows(self, billing_month: str) -> None:
        """
        Delete existing rows for the given billing month.

        Args:
            billing_month (str): The billing month for which to delete existing rows.
        """
        # Ensure billing_month is in 'YYYY-MM-DD' format, e.g., "2024-10-01"
        billing_month_date = datetime.strptime(billing_month, "%Y-%m-%d").date()

        query = f"""
        DELETE FROM `{self.project_id}.{self.dataset_id}.{self.table_id}`
        WHERE CAST(billing_month AS DATE) = @billing_month
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("billing_month", "DATE", billing_month_date)  # Use the date object
            ]
        )
        query_job = self.client.query(query, job_config=job_config)
        query_job.result()  # Wait for the job to complete
        print(f"Deleted existing rows for billing_month: {billing_month_date}.")

def main():
    """
    Main function to handle blob download, process it, and upload it to BigQuery.
    """
    # Load secrets
    secrets = SecretsManager()

    # Step 1: Parse resource location to get rootDirectory, sasToken, and blobName
    resource_location_response = ("""{
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
    
    resource_parser = ResourceLocationParser(resource_location_response)
    parsed_resource_location = resource_parser.parse_resource_location()
    root_directory = parsed_resource_location['rootDirectory']
    sas_token = parsed_resource_location['sasToken']
    blob_name = parsed_resource_location['blobName']

    # Step 2: Parse the root directory to get storage account name and container name
    blob_parser = BlobURLParser(root_directory)
    storage_account_name, container_name = blob_parser.extract_storage_info()

    # Step 3: Download and process the blob using AzureBlobDownloader
    downloader = AzureBlobDownloader(storage_account_name, sas_token, container_name, blob_name)
    
    try:
        # Download and unzip the blob
        blob_stream = io.BytesIO()
        downloader.download_blob_to_stream(container_name, blob_name, blob_stream)
        unzipped_stream = downloader.unzip_blob_stream(blob_stream)

        # Reset the unzipped stream for reading
        unzipped_stream.seek(0)

        # Step 4: Process the unzipped stream to JSON with billing_month
        processed_data = downloader.process_stream_to_json_with_billing_month(unzipped_stream)

        # **Check if the processed data is empty**
        if not processed_data:
            raise ValueError("No records found in the processed data.")

        print(f"Added billing_month to {len(processed_data)} records.")

        # Step 5: Initialize BigQueryUploader
        uploader = BigQueryUploader(
            project_id=secrets.project_id,
            dataset_id=secrets.dataset_id,
            table_id=secrets.table_id
        )

        # Step 6: Upload the processed data to BigQuery
        uploader.upload_data(processed_data)
        print("Data uploaded to BigQuery successfully.")

    except Exception as e:
        print(f"Error during processing: {e}")

if __name__ == "__main__":
    main()
