import os 
from dataclasses import dataclass

import pandas as pd

from py0xcluster.utils.query_utils import *

@dataclass
class PoolsData:
    pools_df: pd.DataFrame
    subgraph_url: str
    min_daily_volume_USD: int
    min_TVL: int
    start_date: tuple
    end_date: tuple
    days_batch_size: int
    

class PoolSelector:
    def __init__(
        self, 
        subgraph_url: str,
        min_daily_volume_USD: int = 100000,
        min_TVL: int = None,
        start_date: tuple = None, 
        end_date: tuple = None,
        days_batch_size: int = 15
        ) -> pd.DataFrame:
    
        self.subgraph_url = subgraph_url
        self.min_daily_volume_USD = min_daily_volume_USD
        self.min_TVL = min_TVL
        self.start_date = start_date
        self.end_date = end_date
        self.days_batch_size = days_batch_size
        

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

    # Aggregate the different snapshots to have single entries for each pools
    def _aggregate_snapshots(self, df_pools):
        # create a dataframe aggregating all the (script) descriptive features of 
        # the pools described in the gathered snapshots
        pools_description = df_pools.drop_duplicates(subset='pool.name', keep='first')
        pools_description = pools_description.select_dtypes(include='string').set_index('pool.symbol')

        # Compute MEDIAN of numeric values (volume, TVL, pries)
        # consider adding flexibility for aggregation method
        pools_stats = df_pools.groupby('pool.name').agg('median', numeric_only=True).sort_values('dailyVolumeUSD', ascending=False)
        pools_stats = pools_stats.merge(pools_description, on='pool.name') 
        
        #TODO create a new class for the aggregate
        return pools_stats

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

    def _remove_illiquid_pools(self, df_pools_data: pd.DataFrame, verbose: bool = True):
        # remove snapshots where the liquidity is under min_TVL
        snapshots_to_remove = df_pools_data['pool.totalValueLockedUSD'] < self.min_TVL
        if verbose:
            print(f'{snapshots_to_remove.sum()} illiquid pools snapshots (over {df_pools_data.shape[0]}) have been removed ')
        
        df_pools_data = df_pools_data[~snapshots_to_remove]
        
        return df_pools_data

    def _get_pools_data(self, verbose: bool = False):

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

    def create_pool_selection(self, stables: str = 'exclude', verbose: bool = True) -> PoolsData: 
        
        # Fetch and pre-process data
        pools_data = self._get_pools_data(self)

        # Select how to deal with stablecoins pools snapshots
        if stables == 'only':
            df_pools = self.keep_only_stable_pools(pools_data, verbose = verbose)
        elif stables == 'exclude':
            df_pools = self.remove_stable_pools(pools_data, verbose = verbose)
        elif stables == 'include':
            pass
        else:
            Exception(f'Invalid stables keyword argument: {stables}')

        # Remove pool snapshots with TVL < min_TVL
        df_pools = self._remove_illiquid_pools(pools_data, verbose = verbose)

        # Perform aggregation of the collected snapshots over the different days
        df_pools = self._aggregate_snapshots(pools_data)
        
        if verbose:
            print(f'{df_pools.shape[0]} pools were selected')

        return PoolsData(pools_df=df_pools,
                        subgraph_url=self.subgraph_url,
                        min_daily_volume_USD=self.min_daily_volume_USD,
                        min_TVL=self.min_TVL,
                        start_date=self.start_date,
                        end_date=self.end_date,
                        days_batch_size=self.days_batch_size)
            
