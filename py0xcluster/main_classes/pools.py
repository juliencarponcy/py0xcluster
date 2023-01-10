import os 

import pandas as pd

from py0xcluster.utils.query_utils import *

class PoolSelector:
    def __init__(
            self, 
            subgraph_url: str,
            min_daily_volume_USD: int = 100000, 
            start_date: tuple = None, 
            end_date: tuple = None,
            days_batch_size: int = 15,
            min_days_active: int = None
            ) -> pd.DataFrame:
        
            self.subgraph_url = subgraph_url
            self.min_daily_volume_USD = min_daily_volume_USD
            self.start_date = start_date
            self.end_date = end_date
            self.days_batch_size = days_batch_size
            self.min_days_active = min_days_active
    
    def _normalize_pools_data(self, pools_data: list):
        # first level of normalization
        df = pd.json_normalize(pools_data, meta=['pool.inputTokens'])
        df_input_tokens = pd.json_normalize(df['pool.inputTokens'])

        # normalize all columns from df_input_tokens
        df_list = list()

        for col in df_input_tokens.columns:
            v = pd.json_normalize(df_input_tokens[col])
            v.columns = [f'token{col}.{c}' for c in v.columns]
            df_list.append(v)

            # combine into one dataframe
            # TODO do not force index, implement specifcally for pool.inputTokens
            df_normalized = pd.concat([df.iloc[:, 0:8]] + df_list, axis=1)

        return df_normalized

    def _preprocess_pools_data(self, df_pools):
        float_cols = [col for col in df_pools.columns if 'USD' in col]
        str_cols = [col for col in df_pools.columns if ('USD' not in col) and ('timestamp' not in col)]

        df_pools['timestamp'] = pd.to_datetime(df_pools['timestamp'], unit='s')

        for col in float_cols:
            df_pools[col] = df_pools[col].astype('float64')

        for col in str_cols:
            df_pools[col] = df_pools[col].astype('string')

        return df_pools

    def keep_only_stable_pools(self, df_pools_data: pd.DataFrame, verbose: bool = True):
        is_stable_pool = df_pools_data['token0.lastPriceUSD'].between(0.99,1.01) & df_pools_data['token1.lastPriceUSD'].between(0.99,1.01)

        if verbose: 
            print(f'{~is_stable_pool.sum()} non-stable pools snapshots (over {df_pools_data.shape[0]}) have been removed')

        df_pools_data = df_pools_data[is_stable_pool]

        return df_pools_data
    
    def remove_stable_pools(self, df_pools_data: pd.DataFrame, verbose: bool = True):
        is_stable_pool = df_pools_data['token0.lastPriceUSD'].between(0.99,1.01) & df_pools_data['token1.lastPriceUSD'].between(0.99,1.01)

        if verbose: 
            print(f'{is_stable_pool.sum()} stable pools snapshots (over {df_pools_data.shape[0]}) have been removed')

        df_pools_data = df_pools_data[~is_stable_pool]

        return df_pools_data

    def remove_illiquid_pools(self, df_pools_data: pd.DataFrame, min_TVL:int, verbose: bool = True):
        # remove snapshots where the liquidity is under min_TVL
        snapshots_to_remove = df_pools_data['pool.totalValueLockedUSD'] < min_TVL
        if verbose:
            print(f'{snapshots_to_remove.sum()} illiquid pools snapshots (over {df_pools_data.shape[0]}) have been removed ')
        
        df_pools_data = df_pools_data[~snapshots_to_remove]
        
        return df_pools_data

    def get_pools_data(self, verbose: bool = False):

        root_folder = os.path.abspath(os.path.join(__file__, '..', '..'))
        query_file_path = os.path.join(root_folder, 'queries', 'messari_getActivePools.gql')
                
        # Create the client
        client = GraphQLClient(self.subgraph_url)
        
        # Generate list of 2 items tuple, start and end date of the date batch
        days_batch_lim = [dates_lim for dates_lim in 
            days_interval_tuples(self.start_date, self.end_date, self.days_batch_size)]
        
        full_results = []
        for days_batch in days_batch_lim:
            start_batch = timestamp_tuple_to_unix(days_batch[0])
            end_batch = timestamp_tuple_to_unix(days_batch[1])

            if verbose:
                print(f'Queriying from {days_batch[0]} to {days_batch[1]}')

            variables = {
                'start_date': start_batch,
                'end_date': end_batch,
                'minVolumeUSD': self.min_daily_volume_USD
                }


            # Run the GraphQL query
            result = client.run_query(query_file_path, variables=variables)
            
            # Break the loop if there are no more results
            if len(result) == 0:
                print('no data for this batch, exiting query')
                break

            # Add the results to the list
            full_results.extend(result)

        # normalize data from liquidityPoolDailySnapshots
        df_pools_data = self._normalize_pools_data(full_results)
        
        # convert types
        df_pools_data = self._preprocess_pools_data(df_pools_data)
        
        if verbose:
            print(f'{df_pools_data.shape[0]} lquidity pools snapshots retrieved')
        return df_pools_data
            
