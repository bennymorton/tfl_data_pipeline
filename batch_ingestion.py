import os
from google.cloud import bigquery
from google.api_core import retry
import pandas as pd
from typing import List

class BigQueryLoader:
    def __init__(self, project_id: str, dataset_id: str):
        """
        Initialize the BigQuery loader
        
        Args:
            project_id (str): Your Google Cloud project ID
            dataset_id (str): The BigQuery dataset ID where tables will be created
        """
        self.client = bigquery.Client(project=project_id)
        self.dataset_id = dataset_id
        self.project_id = project_id

    def load_csv_to_bigquery(
        self,
        csv_file: str,
        table_id: str,
        schema: List[bigquery.SchemaField] = None,
        write_disposition: str = "WRITE_TRUNCATE"
    ) -> None:
        """
        Load a single CSV file into BigQuery
        
        Args:
            csv_file (str): Path to the CSV file
            table_id (str): The ID of the table to create/update
            schema (List[bigquery.SchemaField], optional): Schema for the table
            write_disposition (str): How to handle existing data (WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY)
        """
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # Skip header row
            autodetect=True if schema is None else False,
            schema=schema,
            write_disposition=write_disposition
        )

        with open(csv_file, "rb") as source_file:
            job = self.client.load_table_from_file(
                source_file,
                table_ref,
                job_config=job_config
            )
        
        # Wait for the job to complete
        job.result()
        
        print(f"Loaded {csv_file} into {table_ref}")

    def batch_load_csvs(
        self,
        csv_directory: str,
        schema_mapping: dict = None,
        write_disposition: str = "WRITE_TRUNCATE"
    ) -> None:
        """
        Load all CSV files from a directory into BigQuery
        
        Args:
            csv_directory (str): Directory containing CSV files
            schema_mapping (dict, optional): Dictionary mapping table names to their schemas
            write_disposition (str): How to handle existing data
        """
        for filename in os.listdir(csv_directory):
            if filename.endswith('.csv'):
                # Use the filename (without extension) as the table name
                table_id = os.path.splitext(filename)[0]
                file_path = os.path.join(csv_directory, filename)
                
                schema = schema_mapping.get(table_id) if schema_mapping else None
                
                self.load_csv_to_bigquery(
                    csv_file=file_path,
                    table_id=table_id,
                    schema=schema,
                    write_disposition=write_disposition
                )

def main():
    # Replace these with your actual values
    PROJECT_ID = "white-ground-452819-j2"
    DATASET_ID = "staging_data"
    CSV_DIRECTORY = "/Users/benmorton/Desktop/project_files/personal_projects/tfl_data_pipeline/TfL_station_data_detailed/"
    
    # Initialize the loader
    loader = BigQueryLoader(PROJECT_ID, DATASET_ID)
    
    # Example schema:
    # schemas = {
    #     "table_name": [
    #         bigquery.SchemaField("column1", "STRING"),
    #         bigquery.SchemaField("column2", "INTEGER"),
    #     ]
    # }
    
    # Load all CSV files
    loader.batch_load_csvs(
        csv_directory=CSV_DIRECTORY,
    )

if __name__ == "__main__":
    main()
