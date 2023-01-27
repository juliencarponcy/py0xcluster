import pandas as pd

from py0xcluster.utils.query_utils import *
from py0xcluster.main_classes.pools import PoolsRegister

class PoolsEvents:
    def __init__(
        self,
        swaps: pd.DataFrame,
        deposits: pd.DataFrame,
        withdraws: pd.DataFrame,
        subgraph_url: str,
        pools_data: PoolsRegister,
        pools_ids: list,
        start_date: tuple,
        end_date: tuple,
        days_batch_size: int,
        ):

        self.swaps = swaps
        self.deposits = deposits
        self.withdraws = withdraws

        self.subgraph_url = subgraph_url
        self.pools_data = pools_data
        self.pool_ids = pools_data.pools_df['pool.id']
        self.start_date = start_date
        self.end_date = end_date
        self.days_batch_size = days_batch_size

class PoolEventGetter:
    def __init__(
        self,
        subgraph_url: str,
        pools_data : PoolsRegister,
        pool_ids : list = None,
        start_date: tuple = None, 
        end_date: tuple = None,
        days_batch_size: int = 2
        ):

        self.subgraph_url = subgraph_url
        self.pools_data = pools_data

        # turn pool_ids into a 1 item list if only one pool requested
        if isinstance(pool_ids, str):
            pool_ids = [pool_ids]

        if not pool_ids:
            self.pool_ids = pools_data.pools_df['pool.id'].values
        else:
            self.pool_ids = pool_ids

        self.start_date = start_date
        self.end_date = end_date
        self.days_batch_size = days_batch_size

    def _apply_decimals(self, pool_results: dict, pool_id: str):

        decimals = self.pools_data.pools_df.loc[self.pools_data.pools_df['pool.id'] == pool_id, ['token0.decimals', 'token1.decimals']].values.squeeze()
        decimals = [int(decimal) for decimal in decimals]
        for entity in list(pool_results.keys()):
            amount_cols = [col for col in pool_results[entity].columns
                if ('mount' in col) and ('USD' not in col)]
            
            for col in amount_cols:
                if '0' in col:
                    pool_results[entity][col] = pool_results[entity][col].astype(float) / (10 ** decimals[0])
                elif '1' in col:
                    pool_results[entity][col] = pool_results[entity][col].astype(float) / (10 ** decimals[1])

        return pool_results

    def get_events(self, verbose: bool = False):

        # initialize dictionary for all pools
        full_results = dict()

        for pool_nb, pool_id in enumerate(self.pool_ids):

            if verbose:
                print(f'pool nb {pool_nb}/{len(self.pool_ids)}: {pool_id}')
            events_query_variables = {
                'pool_id': pool_id
            }

            pool_results = run_batched_query(
                subgraph_url = self.subgraph_url, 
                query_file = 'messari_getPoolEvents.gql', 
                start_date = self.start_date, 
                end_date = self.end_date, 
                days_batch_size = self.days_batch_size, 
                query_variables = events_query_variables, 
                verbose = verbose)    

            pool_results = self._normalize_pool_events(pool_results)
            
            full_results = self._aggregate_events(full_results, pool_id, pool_results)

        full_results = self._preprocess_events_data(full_results)
        
        poolsData = PoolsEvents(
            full_results['swaps'],
            full_results['deposits'],
            full_results['withdraws'],
            self.subgraph_url,
            self.pools_data,
            self.pool_ids,
            self.start_date,
            self.end_date,
            self.days_batch_size
        )

        return poolsData

    def _preprocess_events_data(self, full_results):
        for entity in list(full_results.keys()):
            
            int_cols = ['blockNumber']
            float_cols = [col for col in full_results[entity].columns if 'mount' in col]
            str_cols =  ['from','to','id']# [col for col in full_results[entity].columns if ('mount' not in col) and ('timestamp' not in col)]
            cat_cols = ['pool.id']

            full_results[entity]['timestamp'] = pd.to_datetime(full_results[entity]['timestamp'], unit='s')

            for col in int_cols:
                full_results[entity][col] = full_results[entity][col].astype('int')

            for col in float_cols:
                full_results[entity][col] = full_results[entity][col].astype('float64')

            for col in str_cols:
                full_results[entity][col] = full_results[entity][col].astype('string')

            for col in cat_cols:
                full_results[entity][col] = full_results[entity][col].astype('category')

        return full_results

    def _aggregate_events(self, full_results, pool_id, pool_results):
        # check whether data comes from the first pull
        # (empty aggregate)
        is_empty = len(full_results) == 0

        # pool_results = self._apply_decimals(pool_results, pool_id)

        for entity in list(pool_results.keys()):
            pool_results[entity]['pool.id'] = pool_id
            # pd.to_datetime(pool_results[entity]['timestamp'])

            if is_empty:
                full_results = pool_results
            else:
                full_results[entity] = pd.concat([full_results[entity],pool_results[entity]], ignore_index=True)
            
        return full_results

    def _normalize_pool_events(self, pools_data: list):
        
        dict_of_df = dict()

        for entity in list(pools_data.keys()):
            dict_of_df[entity] = pd.json_normalize(pools_data[entity], meta=['inputTokenAmounts'])
            if entity in ['deposits', 'withdraws'] and not dict_of_df[entity].empty:
                df_inputTokenAmounts = dict_of_df[entity]['inputTokenAmounts'].apply(pd.Series)
                df_inputTokenAmounts
                df_inputTokenAmounts.rename({0: 'InputTokenAmount0', 1: 'InputTokenAmount1'}, axis=1, inplace=True)
                dict_of_df[entity].drop(columns=['inputTokenAmounts'], inplace=True)
                dict_of_df[entity] = pd.concat([dict_of_df[entity], df_inputTokenAmounts], axis=1)
        
        return dict_of_df
