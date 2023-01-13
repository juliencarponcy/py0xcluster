import pandas as pd

from py0xcluster.utils.query_utils import *

class PoolEventGetter:
    def __init__(
        self,
        subgraph_url: str,
        pool_ids: list,
        start_date: tuple = None, 
        end_date: tuple = None,
        days_batch_size: int = 2
        ):

        self.subgraph_url = subgraph_url
        self.pool_ids = pool_ids
        self.start_date = start_date
        self.end_date = end_date
        self.days_batch_size = days_batch_size

    def get_events(self, verbose: bool = False):

        # initialize dictionary for all pools
        full_results = dict()

        for pool_id in self.pool_ids:

            if verbose:
                print(f'pool: {pool_id}')
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
        
        return full_results

    def _preprocess_events_data(self, full_results):
        for entity in list(full_results.keys()):
            
            int_cols = ['blockNumber']
            float_cols = [col for col in full_results[entity].columns if 'mount' in col]
            str_cols =  ['from','to','pool.id']# [col for col in full_results[entity].columns if ('mount' not in col) and ('timestamp' not in col)]
            
            full_results[entity]['timestamp'] = pd.to_datetime(full_results[entity]['timestamp'], unit='s')

            for col in int_cols:
                full_results[entity][col] = full_results[entity][col].astype('int')

            for col in float_cols:
                full_results[entity][col] = full_results[entity][col].astype('float64')

            for col in str_cols:
                full_results[entity][col] = full_results[entity][col].astype('string')


            return full_results

    def _aggregate_events(self, full_results, pool_id, pool_results):
        # check whether data comes from the first pull
        # (empty aggregate)
        is_empty = len(full_results) == 0

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
