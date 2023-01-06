import os
import requests

from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport

from dotenv import load_dotenv

import pandas as pd


load_dotenv()

# Infura Project ID
project_id = os.getenv('PROJECT_ID')

# Infura API Key
api_key = os.getenv('API_KEY')

# Subgraph URL
subgraph_url = f'https://api.thegraph.com/subgraphs/name/messari/onchainfx'


class PoolFetcher:
    def __init__(
        self,
        subgraph_url: str,
        project_id: str,
        api_key: str,
        query: str,
        days_batch_size: int = 15,
    ):
        self.QUERY_PATH = r'C:\Users\phar0732\Documents\GitHub\py0xcluster\py0xcluster\queries'

        self.subgraph_url = subgraph_url
        self.project_id = project_id
        self.api_key = api_key
        self.query = gql(self._load_query('messari_fetch_pools.gql'))
        self.days_batch_size = days_batch_size

        # Initialize the transport and client
        self.transport = RequestsHTTPTransport(
            url=f"{self.subgraph_url}",
            use_json=True,
            headers={
                "Content-type": "application/json",
                "x-api-key": f"{self.api_key}",
            },
        )
        self.client = Client(transport=self.transport, fetch_schema_from_transport=True)

    def _get_date_lims(self, start_date, end_date, days_batch_size):
        """
        Generate list of 2 items tuple, start and end date of the date batch
        """
        date_batch_lims = []
        while start_date < end_date:
            date_batch_lims.append((start_date, min(start_date + days_batch_size, end_date)))
            start_date += days


    def _load_query(self, query_file):
        query_path = os.path.join(self.QUERY_PATH, query_file)
        with open(query_path) as f:
            return f.read()