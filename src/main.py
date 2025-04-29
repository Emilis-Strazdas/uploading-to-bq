import logging
import pandas as pd
from src.settings import configure_logger, GcpSettings

LOGGER = logging.getLogger(__name__)

FILENAME = 'data/nordpass_b2b_forecast.csv'

DATASET_ID = 'test_estrazdas'
TABLE_ID = 'nordpass_b2b_forecast'

def main():
    gcp_settings = GcpSettings()
    LOGGER.info("GCP settings loaded successfully.")

    data_to_upload = pd.read_csv(FILENAME)
    LOGGER.info("Data read from CSV file successfully.")

    gcp_settings.upload_to_bigquery(
        data_frame=data_to_upload,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        write_disposition='WRITE_TRUNCATE'
    )
    LOGGER.info("Data uploaded to BigQuery successfully.")

if __name__ == "__main__":
    configure_logger()
    LOGGER.info("Starting the application...")
    main()
    LOGGER.info("Application finished.")