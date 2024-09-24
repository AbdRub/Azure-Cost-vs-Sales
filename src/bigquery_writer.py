import io
import json
from secret_manager import SecretsManager
from google.cloud import bigquery
from dotenv import load_dotenv
from blob_client import AzureBlobDownloader


class BigQueryUploader:
    def __init__(self, project_id: str, dataset_id: str, table_id: str) -> None:
        """
        Initializes the BigQueryUploader instance with the necessary project, dataset, and table information.
        
        Args:
            project_id (str): Google Cloud Project ID.
            dataset_id (str): BigQuery dataset ID.
            table_id (str): BigQuery table ID.
        """
        self.client = bigquery.Client(project=project_id)
        self.dataset_id = dataset_id
        self.table_id = table_id

    def create_table_if_not_exists(self) -> None:
        """
        Checks if the table exists in BigQuery. If not, creates it.
        """
        dataset_ref = self.client.dataset(self.dataset_id)
        table_ref = dataset_ref.table(self.table_id)

        try:
            self.client.get_table(table_ref)
            print(f"Table {self.table_id} already exists. It will be truncated before data upload.")
        except Exception as e:
            print(f"Table {self.table_id} not found. Creating table...")
            schema = [
                bigquery.SchemaField("field1", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("field2", "INTEGER", mode="NULLABLE"),
                # Define your schema fields here...
            ]
            table = bigquery.Table(table_ref, schema=schema)
            self.client.create_table(table)
            print(f"Table {self.table_id} created successfully.")

    def upload_data(self, data_stream) -> None:
        """
        Uploads data from an in-memory stream to BigQuery.
        
        This method truncates the table before uploading data (WRITE_TRUNCATE).
        
        Args:
            data_stream (BytesIO): The in-memory data stream containing JSON rows.
        """
        dataset_ref = self.client.dataset(self.dataset_id)
        table_ref = dataset_ref.table(self.table_id)

        # Read data from the stream (assuming NDJSON format - one JSON object per line)
        json_rows = []
        for line in data_stream:
            json_rows.append(json.loads(line))

        # Configure the load job to replace the existing data
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",  # Replaces the entire data in the table
            autodetect=True,  # Automatically infer schema
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )

        # Log the truncation action
        print(f"Truncating table {self.table_id} and uploading new data...")

        # Load the JSON data into BigQuery
        load_job = self.client.load_table_from_json(json_rows, table_ref, job_config=job_config)
        load_job.result()  # Wait for the job to complete
        print(f"Data uploaded to {self.table_id}.")

# Example usage of BigQueryUploader
def main() -> None:
    """
    Demonstrates how to use the BigQueryUploader class.
    """
    # Retrieve secrets from SecretsManager
    secrets = SecretsManager()

    project_id = secrets.project_id
    dataset_id = secrets.dataset_id
    table_id = secrets.table_id

    # Initialize BigQueryUploader
    uploader = BigQueryUploader(project_id=project_id, dataset_id=dataset_id, table_id=table_id)
    
    # Ensure the table exists (if not, it will be created during data upload)
    uploader.create_table_if_not_exists()

    blob_downloader = AzureBlobDownloader()
    blob_downloader.download_blob() 
    
    # Assuming `unzipped_stream` is available from previous steps (from AzureBlobDownloader)
    try:
        # Upload data to BigQuery
        unzipped_stream = AzureBlobDownloader.unzip_blob_stream()
        uploader.upload_data(unzipped_stream)
    except Exception as e:
        print(f"Error occurred: {e}")

if __name__ == "__main__":
    main()
