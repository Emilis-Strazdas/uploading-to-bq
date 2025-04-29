import logging
import pandas as pd
from src.settings import configure_logger

LOGGER = logging.getLogger(__name__)

FILENAME = 'data/nordpass_b2b_forecast.csv'


def main():

    table = pd.read_csv(FILENAME)
    LOGGER.info("Data read from CSV file successfully.")

    table['last_updated'] = pd.to_datetime('2025-03-31')
    
    table.to_csv(
        FILENAME,
        index=False
    )
    LOGGER.info("Data updated with last_updated column successfully.")

if __name__ == "__main__":
    configure_logger()
    LOGGER.info("Starting the application...")
    main()
    LOGGER.info("Application finished.")