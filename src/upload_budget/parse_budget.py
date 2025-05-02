import pandas as pd
import logging

LOGGER = logging.getLogger(__name__)

def parse_budget(raw_budget: pd.DataFrame) -> pd.DataFrame:
    
    LOGGER.info("Starting budget parsing")
    
    df_indexed = _rename_and_index(raw_budget)
    
    df_t = _transpose_and_prepare(df_indexed)
    
    df_melted = _melt_budget(df_t)
    
    df_meta = _add_metadata(df_melted)
    
    budget = _reorder_columns(df_meta)
    LOGGER.info('Budget: %s', budget)
    
    LOGGER.info("Completed budget parsing with shape %s", budget.shape)
    
    return budget


def _rename_and_index(df: pd.DataFrame) -> pd.DataFrame:
    
    LOGGER.debug("Renaming columns and setting MultiIndex")
    
    df_renamed = df.rename(columns={
        df.columns[0]: 'metric',
        df.columns[1]: 'payment_type',
        df.columns[2]: 'subscription_length',
        df.columns[3]: 'company_size',
    }).set_index([
        'metric',
        'payment_type',
        'subscription_length',
        'company_size'
    ])
    
    LOGGER.debug("Indexed DataFrame shape: %s", df_renamed.shape)
    
    return df_renamed

def _transpose_and_prepare(df: pd.DataFrame) -> pd.DataFrame:
    
    LOGGER.debug("Transposing DataFrame and preparing index")
    
    df_t = df.T
    df_t.index = pd.to_datetime(df_t.index).to_period('M').strftime('%Y-%m')
    df_t.index.name = 'ds'
    
    LOGGER.debug("Transposed DataFrame shape: %s", df_t.shape)
    
    return df_t

def _melt_budget(df_t: pd.DataFrame) -> pd.DataFrame:
    
    LOGGER.debug("Resetting index and melting budget data")
    
    df_melted = df_t.reset_index().melt(
        id_vars=[('ds', '', '', '')],
    )
    
    df_clean = df_melted.drop(columns=['metric'], errors='ignore')
    
    df_clean.columns = ['ds' if col == ('ds', '', '', '') else col for col in df_clean.columns]
    df_clean.columns = ['billings' if col == 'value' else col for col in df_clean.columns]
    
    df_clean['billings'] = pd.to_numeric(df_clean['billings'], errors='coerce')
    
    LOGGER.debug("Melted DataFrame shape: %s", df_clean.shape)
    
    return df_clean

def _add_metadata(df: pd.DataFrame) -> pd.DataFrame:
    
    LOGGER.debug("Adding metadata columns to DataFrame")
    
    df['payment_status'] = 'Completed'
    df['previous_subscription_length'] = None
    df['license_count'] = 0.
    df['billings_per_license'] = 0.
    df['last_updated'] = '2025-03-31'
    df['data_type'] = 'budget'
    
    LOGGER.debug("Columns after metadata: %s", df.columns.tolist())
    
    return df

def _reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    
    LOGGER.debug("Reordering columns")
    
    cols = [
        'ds',
        'payment_status',
        'payment_type',
        'company_size',
        'subscription_length',
        'previous_subscription_length',
        'license_count',
        'billings_per_license',
        'billings',
        'last_updated',
        'data_type'
    ]
    
    df_reordered = df[cols]
    
    LOGGER.debug("Final column order: %s", cols)
    
    return df_reordered
