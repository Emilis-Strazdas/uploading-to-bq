import configparser
import logging
import platform
from dataclasses import dataclass, field
from typing import Tuple, Optional

import pandas as pd

from google.auth import default, exceptions
from google.cloud import bigquery, secretmanager, storage
from google.oauth2 import service_account

LOGGER = logging.getLogger(__name__)
# -----------------------------------------------------------------------------

@dataclass
class GcpSettings:
    """
    A class for managing Google Cloud Platform (GCP) settings and operations.

    This class allows you to interact with Google Cloud Storage (GCS),
    BigQuery, read data from GCS and write Pandas DataFrames to BQ, 
    and access Google Secret Manager.
    """
    scopes: list = field(default_factory=lambda: [
            'https://www.googleapis.com/auth/bigquery',
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/cloud-platform'
        ])
    ini_file_path: str = 'bq.ini'  # Path to the ini file for fallback credentials

    credentials: Optional[service_account.Credentials] = field(init=False, default=None)
    project_id: Optional[str] = field(init=False, default=None)
    secret_manager_client: Optional[secretmanager.SecretManagerServiceClient] = field(init=False, default=None)
    storage_client: Optional[storage.Client] = field(init=False, default=None)
    bigquery_client: Optional[bigquery.Client] = field(init=False, default=None)

    def __post_init__(self) -> None:
        """
        Initialize GCP settings with configuration.
        """
        self.credentials, self.project_id = self._get_credentials_and_project_id()
        self.secret_manager_client = self._get_secret_manager_client()
        self.storage_client = self._get_storage_client()
        self.bigquery_client = self._get_bigquery_client()

    def _get_credentials_and_project_id(self) -> Tuple:
        """
        Attempts to obtain GCP credentials and project ID using default credentials.
        If it fails, it falls back to loading credentials from an ini file.

        Returns:
            tuple: A tuple containing the credentials and project ID.
        """
        try:
            LOGGER.debug("Attempting to load default GCP credentials.")
            credentials, project_id = default(scopes=self.scopes)
            LOGGER.info("Default GCP credentials loaded successfully.")
            return credentials, project_id
        except exceptions.DefaultCredentialsError as e:
            LOGGER.warning(f"Default credentials not found: {e}. Attempting to load from ini file.")
            return self._load_credentials_from_ini()
        except Exception as e:
            LOGGER.error(f"An unexpected error occurred while loading default credentials: {e}")
            raise

    def _load_credentials_from_ini(self) -> Tuple:
        """
        Loads credentials and project ID from the bq.ini file.

        Returns:
            tuple: A tuple containing the credentials and project ID.
        """
        config = configparser.ConfigParser()
        read_files = config.read(self.ini_file_path)

        if not read_files:
            LOGGER.error(f"Ini file '{self.ini_file_path}' not found or is empty.")
            raise FileNotFoundError(f"Ini file '{self.ini_file_path}' not found or is empty.")

        try:
            creds_path = config['GCP']['credentials_path']
            project_id = config['GCP']['project_id']
            LOGGER.debug(f"Credentials path: {creds_path}")
            LOGGER.debug(f"Project ID: {project_id}")
        except KeyError as e:
            LOGGER.error(f"Missing required configuration in {self.ini_file_path}: {e}")
            raise KeyError(f"Missing required configuration in {self.ini_file_path}: {e}")

        try:
            # Load the service account credentials
            credentials = service_account.Credentials.from_service_account_file(
                creds_path,
                scopes=self.scopes
            )
            LOGGER.info("Service account credentials loaded successfully from ini file.")
        except Exception as e:
            LOGGER.error(f"Failed to load service account credentials from '{creds_path}': {e}")
            raise

        return credentials, project_id

    def read_from_storage(
        self,
        bucket_name: str,
        file_path: str
    ) -> str:
        """
        Reads the content of a file from a storage bucket.

        Args:
            bucket_name (str): The name of the storage bucket.
            file_path (str): The path to the file within the bucket.

        Returns:
            str: The content of the file as a string.
        """
        LOGGER.debug('Reading from %s/%s...', bucket_name, file_path)
        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(file_path)
        content = blob.download_as_text()
        LOGGER.debug(
            'Reading from %s/%s completed successfully.',
            bucket_name,
            file_path
            )

        return content

    def upload_to_storage(
        self,
        source_object: str,
        bucket_name: str,
        file_path: str
    ) -> None:
        """
        Uploads a string as a file to a storage bucket.

        Args:
            source_object (str): The data to upload.
            bucket_name (str): The name of the storage bucket.
            file_path (str): The path where the file will be stored within the bucket.
        """
        LOGGER.debug('Uploading file to %s/%s...', bucket_name, file_path)
        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(file_path)
        blob.upload_from_string(data=source_object, timeout=300.0)
        LOGGER.debug(
            'File uploaded to %s/%s successfully.',
            bucket_name,
            file_path
            )

    def read_from_bigquery(
        self,
        query: str,
        **kwargs: dict
        ) -> pd.DataFrame:
        """Reads data from BigQuery.

        Args:
            query (str): The query to execute.
            kwargs (dict): Additional keyword arguments to pass to the BigQuery client.

        Returns:
            pd.DataFrame: The data read from BigQuery.
        """
        LOGGER.debug('Fetching data from BigQuery...')
        query_job = self.bigquery_client.query(query, **kwargs)
        data_frame = query_job.to_dataframe()
        LOGGER.debug('Data fetched successfully.')
        return data_frame

    def upload_to_bigquery(
        self,
        data_frame: pd.DataFrame,
        dataset_id: str,
        table_id: str,
        write_disposition: str
    ) -> None:
        """
        Writes data to a BigQuery table.

        Args:
            data_frame (pd.DataFrame): The data to write to BigQuery.
            dataset_id (str): The ID of the dataset to write to.
            table_id (str): The ID of the table to write to.
            write_disposition (str): The write disposition for the BigQuery job.
                One of "WRITE_TRUNCATE", "WRITE_APPEND", or "WRITE_EMPTY".

        Returns:
            None
        """
        LOGGER.debug('Uploading data to %s.%s table...', dataset_id, table_id)
        table_ref = self.bigquery_client.dataset(dataset_id).table(table_id)
        job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)

        job = self.bigquery_client.load_table_from_dataframe(
            data_frame, table_ref, job_config=job_config
            )
        job.result()  # Wait for the job to complete.
        LOGGER.debug('Data uploaded to %s.%s table successfully.', dataset_id, table_id)

    def _get_secret_manager_client(self) -> secretmanager.SecretManagerServiceClient:
        """
        Returns a Google Secret Manager client.

        Returns:
            secretmanager.SecretManagerServiceClient: The Google Secret Manager client.
        """
        return secretmanager.SecretManagerServiceClient(credentials=self.credentials)

    def _get_storage_client(self) -> storage.Client:
        """
        Returns a Google Cloud Storage client.

        Returns:
            storage.Client: The Google Cloud Storage client.
        """
        return storage.Client(credentials=self.credentials, project=self.project_id)

    def _get_bigquery_client(self) -> bigquery.Client:
        """
        Returns a Google BigQuery client.

        Returns:
            bigquery.Client: The Google BigQuery client.
        """
        return bigquery.Client(credentials=self.credentials, project=self.project_id)
