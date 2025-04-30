import logging

import pandas as pd

from src.settings import GcpSettings
from src.upload_budget.parse_budget import parse_budget

LOGGER = logging.getLogger(__name__)

FILENAME = 'data/nordpass_b2b_budget_q2.xlsx'

def upload_budget(
    gcp_settings: GcpSettings,
    dataset_id: str,
    table_id: str,
) -> pd.DataFrame:
    """
    Parse a budget file and return a DataFrame.

    Args:
        file_name (str): The name of the budget file to parse.

    Returns:
        pd.DataFrame: A DataFrame containing the parsed budget data.
    """
    LOGGER.info("Uploading the budget to BigQuery...")

    raw_budget = pd.read_excel(FILENAME)
    LOGGER.info("Data read from Excel file successfully.")
    
    budget = parse_budget(raw_budget)

    # gcp_settings.upload_to_bigquery(
    #     data_frame=budget,
    #     dataset_id=dataset_id,
    #     table_id=table_id,
    #     write_disposition='WRITE_TRUNCATE'
    # )
    LOGGER.info("Data uploaded to BigQuery successfully.")
