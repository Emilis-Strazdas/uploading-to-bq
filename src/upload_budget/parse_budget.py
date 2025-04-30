import logging

import pandas as pd

LOGGER = logging.getLogger(__name__)

def parse_budget(raw_budget: pd.DataFrame) -> pd.DataFrame:
    """
    Parse a budget file and return a DataFrame with columns
    [ds, metric, payment_type, subscription_length, company_size, budget].
    """
    LOGGER.info("Parsing the budget file...")
    
    raw_budget = raw_budget.rename(columns={
        raw_budget.columns[0]: 'metric',
        raw_budget.columns[1]: 'payment_type',
        raw_budget.columns[2]: 'subscription_length',
        raw_budget.columns[3]: 'company_size',
    }).set_index([
        'metric',
        'payment_type',
        'subscription_length',
        'company_size'
    ])
    
    budget_t = raw_budget.T
    
    budget_t.index = pd.to_datetime(budget_t.index).to_period('M')
    budget_t.index.name = 'ds'
    
    budget = budget_t.reset_index()
    
    budget = (
        budget_t
        .reset_index()
        .melt(
            id_vars=[('ds', '', '', '')],
        )
    )
    
    budget.columns = ['ds', 'metric', 'payment_type', 'subscription_length', 'company_size', 'budget']
    
    budget = budget.drop(columns=['metric'], errors='ignore')
    
    budget = budget.rename(columns={'budget': 'billings'})
    
    budget['payment_status'] = 'Completed'
    budget['previous_subscription_length'] = None
    budget['license_count'] = None
    budget['billings_per_license'] = None
    budget['last_updated'] = '2025-03-31'
    budget['data_type'] = 'budget'
    
    budget = budget[[
        'ds', 'payment_status', 'payment_type', 'company_size', 
        'subscription_length', 'previous_subscription_length', 
        'license_count', 'billings_per_license', 'billings', 
        'last_updated', 'data_type'
    ]]
    
    LOGGER.info('Budget: \n%s', budget)
    LOGGER.info("Budget file parsed successfully.")
    return budget