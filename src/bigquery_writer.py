import io
import json
from secret_manager import SecretsManager
from google.cloud import bigquery
from dotenv import load_dotenv
from blob_client import AzureBlobDownloader

class BigQueryUploader:
    def __init__(self, project_id: str, dataset_id: str, table_id: str) -> None:
        """
        Initialize the BigQueryUploader with project, dataset, and table information
        retrieved from environment variables.
        """
        print('BigQueryUploader init started..')
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.client = bigquery.Client(project=self.project_id)
        self.dataset_ref = self.client.dataset(self.dataset_id)
        self.table_ref = self.dataset_ref.table(self.table_id)
        print('BigQueryUploader init success')

    def create_table_if_not_exists(self) -> None:
        """
        Create the BigQuery table if it does not exist. 
        Schema will be inferred by BigQuery automatically when data is uploaded.
        """
        try:
            self.client.get_table(self.table_ref)
            print("Table already exists.")
        except Exception as e:
            print(f"Error occurred: {e}")
            # self.table_ref.create()
            print("Table does not exist. It will be created when data is uploaded.")

    def upload_data(self, data_stream: io.BytesIO) -> None:
        """
        Upload data from the in-memory stream to the BigQuery table. Schema will be auto-detected.

        Args:
            data_stream (io.BytesIO): The in-memory stream containing the data.
        """
        # Ensure the stream is at the beginning
        data_stream.seek(0)
        
        # Load data into BigQuery with autodetect schema
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True  # Let BigQuery infer the schema
        )
        
        load_job = self.client.load_table_from_file(data_stream, self.table_ref, job_config=job_config)
        load_job.result()  # Wait for the job to complete
        print("Data uploaded successfully.")

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
    
    # Assuming `unzipped_stream` is available from previous steps (from AzureBlobDownloader)
    try:
        # Upload data to BigQuery
        unzipped_stream = AzureBlobDownloader.unzip_blob_stream()
        uploader.upload_data(unzipped_stream)
    except Exception as e:
        print(f"Error occurred: {e}")

if __name__ == "__main__":
    main()
