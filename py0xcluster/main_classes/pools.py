from pathlib import Path
from dataclasses import dataclass

import pandas as pd

from py0xcluster.utils.query_utils import *

@dataclass
class PoolsRegister:
    pools_df: pd.DataFrame
    subgraph_url: str
    min_vol: int
    min_TVL: int
    start_date: tuple
    end_date: tuple
    days_batch_size: int
    

class PoolSelector:
    def __init__(
        self, 
        subgraph_url: str,
        query_file_path: str,
        min_vol: int = 500,
        max_vol: int = 1000000,
        min_TVL: int = 500,
        max_TVL: int = 100000,
        min_fees: int = 0,
        min_mintburnUSD: int = 100,
        start_date: tuple = None, 
        end_date: tuple = None,
        days_batch_size: int = 15
        ):
    
        self.subgraph_url = subgraph_url
        self.query_file_path = Path(query_file_path)
        self.min_vol = min_vol
        self.max_vol = max_vol
        self.min_TVL = min_TVL
        self.max_TVL = max_TVL
        self.min_fees = min_fees
        self.min_mintburnUSD = min_mintburnUSD
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
        # TODO remove this hack of typing using string matching
        float_cols = [col for col in df_pools.columns if 'USD' in col]
        str_cols = [col for col in df_pools.columns if ('USD' not in col) and ('timestamp' not in col)]

        df_pools['timestamp'] = pd.to_datetime(df_pools['timestamp'], unit='s')

        for col in float_cols:
            df_pools[col] = df_pools[col].astype('float64')

        for col in str_cols:
            df_pools[col] = df_pools[col].astype('string')

        return df_pools

    def keep_only_stable_pools(self, df_pools_data: pd.DataFrame, verbose: bool = False):
        is_stable_pool = df_pools_data['token0.lastPriceUSD'].between(0.90,1.1) & df_pools_data['token1.lastPriceUSD'].between(0.90,1.1)

        if verbose: 
            print(f'{~is_stable_pool.sum()} non-stable pools snapshots (over {df_pools_data.shape[0]}) have been removed')

        df_pools_data = df_pools_data[is_stable_pool == True]

        return df_pools_data
    
    def remove_stable_pools(self, df_pools_data: pd.DataFrame, verbose: bool = False):
        is_stable_pool = df_pools_data['token0.lastPriceUSD'].between(0.90,1.1) & df_pools_data['token1.lastPriceUSD'].between(0.90,1.1)

        if verbose: 
            print(f'{is_stable_pool.sum()} stable pools snapshots (over {df_pools_data.shape[0]}) have been removed')

        df_pools_data = df_pools_data[is_stable_pool == False]
        return df_pools_data

    def _remove_illiquid_pools(self, df_pools_data: pd.DataFrame, verbose: bool = False):
        # remove snapshots where the liquidity is under min_TVL
        snapshots_to_remove = df_pools_data['pool.totalValueLockedUSD'] < self.min_TVL
        if verbose:
            print(f'{snapshots_to_remove.sum()} illiquid pools snapshots (over {df_pools_data.shape[0]}) have been removed ')
        
        df_pools_data = df_pools_data[~snapshots_to_remove]
        
        return df_pools_data

    def _get_pools_data(self, verbose: bool = False):
        
        pools_query_variables = {
            'min_TVL': self.min_TVL,
            'min_fees': self.min_fees,
            'min_mintburnUSD': self.min_mintburnUSD,
            'min_tvl': self.min_TVL,
            'max_tvl': self.max_TVL,
            'min_vol': self.min_vol,
            'max_vol': self.max_vol,
        }

        full_results = run_batched_query(
            subgraph_url = self.subgraph_url, 
            query_file = self.query_file_path, 
            start_date = self.start_date, 
            end_date = self.end_date, 
            days_batch_size = self.days_batch_size, 
            query_variables = pools_query_variables, 
            verbose = verbose)    

        # df_pools_data = self._normalize_pools_data(full_results['poolDayDatas'])
        
        # # convert types
        # df_pools_data = self._preprocess_pools_data(df_pools_data)
    
        # if verbose:
        #     print(f'{df_pools_data.shape[0]} lquidity pools snapshots retrieved')
        return full_results #df_pools_data

    def create_pool_selection(self, stables: str = 'exclude', verbose: bool = False) -> PoolsRegister: 
        
        # Fetch and pre-process data
        pools_data = self._get_pools_data(verbose = verbose)
        return pools_data
        # # Select how to deal with stablecoins pools snapshots
        # if stables == 'only':
        #     df_pools = self.keep_only_stable_pools(pools_data, verbose = verbose)
        # elif stables == 'exclude':
        #     df_pools = self.remove_stable_pools(pools_data, verbose = verbose)
        # elif stables == 'include':
        #     pass
        # else:
        #     Exception(f'Invalid stables keyword argument: {stables}')

        # # Remove pool snapshots with TVL < min_TVL
        # df_pools = self._remove_illiquid_pools(df_pools, verbose = verbose)

        # # Perform aggregation of the collected snapshots over the different days
        # df_pools = self._aggregate_snapshots(df_pools)
        
        # if verbose:
        #     print(f'{df_pools.shape[0]} pools were selected')

        # return PoolsRegister(pools_df=df_pools,
        #                 subgraph_url=self.subgraph_url,
        #                 min_daily_volume_USD=self.min_daily_volume_USD,
        #                 min_TVL=self.min_TVL,
        #                 start_date=self.start_date,
        #                 end_date=self.end_date,
        #                 days_batch_size=self.days_batch_size)
            
