import logging

import pandas as pd

from src.settings import configure_logger, GcpSettings
from src.upload_budget.upload_budget import upload_budget
from src.upload_forecast.upload_forecast import upload_forecast

LOGGER = logging.getLogger(__name__)

DATASET_ID = 'test_estrazdas'
TABLE_ID = 'nordpass_b2b_forecast'

def main():
    gcp_settings = GcpSettings()
    
    # upload_forecast(
    #     gcp_settings=gcp_settings,
    #     dataset_id=DATASET_ID,
    #     table_id=TABLE_ID,
    # )
    
    upload_budget(
        gcp_settings=gcp_settings,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
    )

if __name__ == "__main__":
    configure_logger()
    LOGGER.info("Starting the application...")
    main()
    LOGGER.info("Application finished.")