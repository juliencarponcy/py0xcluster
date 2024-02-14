import os

from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from gql.transport.exceptions import TransportQueryError

from py0xcluster.utils.time_utils import *

def run_batched_query(
        subgraph_url: str, 
        query_file: str, 
        start_date: tuple, 
        end_date: tuple, 
        days_batch_size: int, 
        query_variables: dict, 
        verbose: bool = False) -> dict:


    # Create the client
    client = GraphQLClient(subgraph_url, query_file)
    
    # Generate list of 2 items tuple, start and end date of the date batch
    days_batch_lim = [dates_lim for dates_lim in 
        days_interval_tuples(start_date, end_date, days_batch_size)]
    
    # initialize results dictionary
    full_results = dict()

    # loop over batched of days
    for days_batch in days_batch_lim:
        start_batch = timestamp_tuple_to_unix(days_batch[0])
        end_batch = timestamp_tuple_to_unix(days_batch[1])

        if verbose:
            print(f'Queriying from {days_batch[0]} to {days_batch[1]}')

        date_vars = {
            'start_date': start_batch,
            'end_date': end_batch,
            }

        # concatenate query variables with dates loop variables
        variables = query_variables
        variables.update(date_vars)

        # Run the GraphQL query
        result = client.run_query(variables=variables, verbose=False)
        
        for entity in list(result.keys()):
            # create the entity as key with the list of result
            if len(full_results) < len(result):
                full_results[entity] = result[entity]
            
            else:
                # extend the list of entities
                full_results[entity].extend(result[entity])

    return full_results

class GraphQLClient:
    def __init__(self, endpoint, query_file_path):
        # Set up the client
        self.transport = RequestsHTTPTransport(
            url=endpoint,
            use_json=True,
        )

        # Read the query from the .gql file
        with open(query_file_path, 'r') as f:
            self.query = gql(f.read())
        self.client = Client(transport=self.transport, fetch_schema_from_transport=True)

    def _execute_query(self, query, variables):
        """Helper function to execute a query and handle errors"""
        result = self.client.execute(query, variable_values=variables)
        if 'errors' in result:
            raise Exception(result['errors'])
        return result

    def run_query(self, variables: dict = None, verbose: bool = False):

        # Execute the query and return the results
        return self._paginate_query(self.query, variables, verbose)

    def _paginate_query(self, query, variables, verbose: bool = False):
        """Helper function to paginate a query and return all results"""
        # Set up the pagination variables
        first = 1000
        skip = 0

        results = dict()
        # Keep running the query until no more results are returned
        while True:
            # Update the variables with the current pagination values
            variables['first'] = first
            variables['skip'] = skip

            # Execute the query
            try:
                result = self._execute_query(query, variables)
            except TransportQueryError:
                print(f'WARNING: Not all results were returned,\
try smaller batches.\nvariables:\n{variables}')
                break
            # Extract the baseEntity type (a single entity by query
            # must be asked for it to work) to have the key of the
            # list of result
            
            # List the base entities queried
            base_entities = list(result.keys())

            data_lengths = []
            for entity in base_entities:
                
                # Extract the data from the response
                data = result[entity]
                if verbose:
                    print(f' entity: {entity}, skip: {skip}, data length: {len(data)}')
                                
                if skip == 0:
                    results[entity] = data
                
                if len(data) > 0:
                    # Add the results to the list
                    results[entity].extend(data)

                data_lengths.append(len(data))

            # check if the different entities return no more responses
            empty_data = [data_length == 0 for data_length in data_lengths]

            # Break the loop if there are no more results
            if all(empty_data) or max(data_lengths) < first:
                break


            # Increase the skip value by the number of returned results
            skip += first

        return results