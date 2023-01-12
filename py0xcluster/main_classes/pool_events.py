from pandas import json_normalize

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
        
        if len(full_results) == 0:
            full_results = pool_results
        else:
            for entity in list(full_results.keys()):
                full_results[entity].extend(pool_results[entity])

        return full_results

    def _normalize_pool_events(self, pools_data: list):
        
        dict_of_df = dict()

        for entity in list(pools_data.keys()):
            dict_of_df[entity] = json_normalize(pools_data[entity])

        return dict_of_df
