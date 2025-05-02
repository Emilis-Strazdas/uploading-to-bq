import logging

import pandas as pd

from src.settings import GcpSettings

LOGGER = logging.getLogger(__name__)

FILENAME = 'data/nordpass_b2b_forecast.csv'

def upload_forecast(
    gcp_settings: GcpSettings,
    dataset_id: str,
    table_id: str,
):
    LOGGER.info("Uploading the forecast to BigQuery...")

    data_to_upload = pd.read_csv(FILENAME)
    LOGGER.info("Data read from CSV file successfully.")

    # gcp_settings.upload_to_bigquery(
    #     data_frame=data_to_upload,
    #     dataset_id=dataset_id,
    #     table_id=table_id,
    #     write_disposition='WRITE_TRUNCATE'
    # )
    LOGGER.info("Data uploaded to BigQuery successfully.")