import pandas as pd

# Helpers
def find_limit_orders(
        df_object: pd.DataFrame):

    df_object['is_limit_order'] = (df_object['InputTokenAmount0'] == 0) | (df_object['InputTokenAmount1'] == 0)
    df_object['is_limit_order'] = df_object['is_limit_order'].astype(float)
    
    return df_object

def compute_slippage(
        df_object: pd.DataFrame):

    df_object['slippage'] = 100*(1-(df_object['amountOutUSD'] / df_object['amountInUSD']))

    return df_object

def aggregate_features(
        df_object: pd.DataFrame,
        method: str = 'median',
        columns: list = ['amountInUSD.zscore', 'amountOutUSD.zscore',\
            'slippage', 'slippage.zscore', 'sameFromTo'],
        group_by: str = 'from'):
    
    valid_methods = ('median', 'mean')

    if isinstance(columns, str):
        columns = [columns]

    if method not in valid_methods:
        raise ValueError(f"Invalid method: {method}. Valid event types are {valid_methods}")
    
    aggregate_df = pd.DataFrame()
    for col in columns:
        aggregate_df[f"{col}.{method}"] = df_object.groupby(group_by)[col].aggregate('median')
    
    return aggregate_df

def same_from_to(       
        df_object: pd.DataFrame,
        to_float: bool = True
    ):

    df_object['sameFromTo'] = df_object['from'] == df_object['to']
    if to_float:
        df_object['sameFromTo'] = df_object['sameFromTo'].astype('float')

    return df_object

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
