import pandas as pd

# Helpers

def compute_slippage(
        df_object: pd.DataFrame):

    df_object['slippage'] = 100*(1-(df_object['amountOutUSD'] / df_object['amountInUSD']))

    return df_object

def aggregate_median(
        df_object: pd.DataFrame,
        columns: list = ['amountInUSD.zscore', 'amountOutUSD.zscore',\
            'amountIn.zscore','amountOut.zscore', 'slippage', 'slippage.zscore'],
        group_by: str = 'from'):

    if isinstance(columns, str):
        columns = [columns]

    aggregate_df = pd.DataFrame()
    for col in columns:
        aggregate_df[f"{col}.median"] = df_object.groupby(group_by)[col].aggregate('median')
    
    return aggregate_df
       

def compute_zscore(
        df_object: pd.DataFrame,
        columns: list = ['amountInUSD', 'amountOutUSD', 'amountIn', 'amountOut', 'slippage'],
        group_by: str = 'pool.id'):

    if isinstance(columns, str):
        columns = [columns]

    for col in columns:
        df_object[f"{col}.zscore"] = df_object.groupby(group_by)[col].transform(lambda x: (x - x.mean()) / x.std())
        
    return df_object

        # if event_type not in ['swaps', 'deposits', 'withdraws']:
        #     raise ValueError(f"Invalid event type: {event_type}. Valid event types are 'swaps', 'deposits', 'withdraws'.")

        # if not all((col in self.event_type for col in columns)):
        #     raise ValueError(f"Invalid column names: {event_type}. Valid columns names are {self.event_type.columns}")

        # if groupby not in self.event_type:
        #     raise ValueError(f"Invalid groupby type: {event_type}. Valid groupby values are {self.event_type.columns}")
