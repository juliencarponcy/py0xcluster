from dataclasses import dataclass

import pandas as pd

from py0xcluster.utils.query_utils import *
from py0xcluster.utils.time_utils import batch




class AccountGetter:
    def __init__(
        self,
        subgraph_url: str,
        event_ids: list,
        ):

        self.subgraph_url = subgraph_url
        self.event_ids = event_ids
        self.nb_events_by_query = 1000


    def get_accounts(self, verbose: bool = False):
        
        client = GraphQLClient(self.subgraph_url, query_file='messari_getSwapsAccounts.gql')

        for curr_batch in batch(self.event_ids, self.nb_events_by_query):
            variables = {'swap_ids': curr_batch}
            client.run_query(variables)