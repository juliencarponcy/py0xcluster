import pandas as pd

from py0xcluster.utils.query_utils import *

class PoolEventGetter:
    def __init__(
        self,
        subgraph_url: str,
        pool_id: str,
        start_date: tuple = None, 
        end_date: tuple = None,
        days_batch_size: int = 2
        ):

        self.subgraph_url = subgraph_url
        self.pool_id = pool_id
        self.start_date = start_date
        self.end_date = end_date
        self.days_batch_size = days_batch_size

    def _get_raw_events(self, verbose: bool = True):

        # root_folder = os.path.abspath(os.path.join(__file__, '..', '..'))
        # query_file_path = os.path.join(root_folder, 'queries', 'messari_getActivePools.gql')
        query_file_path = r'/home/fujiju/Documents/GitHub/py0xcluster/py0xcluster/queries/messari_getPoolEvents.gql'     
        
        # Create the client
        client = GraphQLClient(self.subgraph_url)
                
        # Generate list of 2 items tuple, start and end date of the date batch
        days_batch_lim = [dates_lim for dates_lim in 
            days_interval_tuples(self.start_date, self.end_date, self.days_batch_size)]
        
        full_results = dict()
        for days_batch in days_batch_lim:
            start_batch = timestamp_tuple_to_unix(days_batch[0])
            end_batch = timestamp_tuple_to_unix(days_batch[1])

            if verbose:
                print(f'Queriying from {days_batch[0]} to {days_batch[1]}')

            variables = {
                'start_date': start_batch,
                'end_date': end_batch,
                'pool_id': self.pool_id
                }


            # Run the GraphQL query
            result = client.run_query(query_file_path, variables=variables)
            
            for entity in list(result.keys()):
                # create the entity as key with the list of result
                if len(full_results) < len(result):
                    full_results[entity] = result[entity]
                
                else:
                    # extend the list of entities
                    full_results[entity].extend(result[entity])
        
        return full_results

    def _normalize_pool_events(self, pools_data: list):
        
        dict_of_df = dict()

        for entity in list(pools_data.keys()):
            dict_of_df[entity] = pd.json_normalize(pools_data[entity])

        return dict_of_df
