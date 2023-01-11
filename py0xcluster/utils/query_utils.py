'''
TODO Build tools to construct custom queries
'''
from datetime import datetime, timedelta
import time 
import json
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport

def timestamp_tuple_to_unix(timestamp_tuple):
    if isinstance(timestamp_tuple, tuple):
        date_time = datetime(*timestamp_tuple)
    elif isinstance(timestamp_tuple, datetime):
        date_time = timestamp_tuple
    unix_ts = int(time.mktime(date_time.timetuple()))
    return unix_ts

def batch(iterable, n:int=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]

def days_interval_tuples(
        start_date:tuple, 
        end_date:tuple, 
        days_batch_size:int):
    '''
    Construct a generator of date limits over an interval
    based on a batch size in days.
    Return a generator which outputs a list of tuples of two datetime
    (start and end of the batch)

    Example: 

    days_batch_lim = [dates_lim for dates_lim in days_interval(start_date, end_date, days_batch_size)]

    >> [(datetime(2022, 6, 10, 0, 0), datetime(2022, 6, 13, 0, 0)),
        (datetime(2022, 6, 13, 0, 0), datetime(2022, 6, 16, 0, 0)),
        (datetime(2022, 6, 16, 0, 0), datetime(2022, 6, 19, 0, 0)),
        (datetime(2022, 6, 19, 0, 0), datetime(2022, 6, 22, 0, 0)),
        (datetime(2022, 6, 22, 0, 0), datetime(2022, 6, 25, 0, 0)),
        (datetime(2022, 6, 25, 0, 0), datetime(2022, 6, 28, 0, 0)),
        (datetime(2022, 6, 28, 0, 0), datetime(2022, 6, 30, 0, 0))]
    '''
    start = datetime(*start_date)
    end = datetime(*end_date)
    curr = start
    while curr < end:
        yield (curr, min(end, curr + timedelta(days_batch_size)))
        curr += timedelta(days_batch_size)

class GraphQLClient:
    def __init__(self, endpoint):
        # Set up the client
        self.transport = RequestsHTTPTransport(
            url=endpoint,
            use_json=True,
        )
        self.client = Client(transport=self.transport, fetch_schema_from_transport=True)

    def _execute_query(self, query, variables):
        """Helper function to execute a query and handle errors"""
        result = self.client.execute(query, variable_values=variables)
        if 'errors' in result:
            raise Exception(result['errors'])
        return result

    def run_query(self, query_file, variables=None):
        # Read the query from the .gql file
        with open(query_file, 'r') as f:
            query = gql(f.read())
        
        # Execute the query and return the results
        return self._paginate_query(query, variables)

    def _paginate_query(self, query, variables, verbose:bool = True):
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
            result = self._execute_query(query, variables)

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
                    print(results.keys(), len(results[entity]))
                    results[entity].extend(data)

                data_lengths.append(len(data))

            print(data_lengths)
            empty_data = [data_length == 0 for data_length in data_lengths]

            # Break the loop if there are no more results
            if all(empty_data) or max(data_lengths) < first:
                break


            # Increase the skip value by the number of returned results
            skip += first

        return results

'''
class GraphQLClient:
    def __init__(self, endpoint):
        # Set up the client
        self.transport = RequestsHTTPTransport(
            url=endpoint,
            use_json=True,
        )
        self.client = Client(transport=self.transport, fetch_schema_from_transport=True)

    # WARNING: pagination will only work if a single BaseEntity type is queried
    # TODO refactor to separate query from pagination
    def run_query(self, query_file, variables=None):
        # Read the query from the .gql file


        with open(query_file, 'r') as f:
            query = gql(f.read())

        # Set up the pagination variables
        first = 1000
        skip = 0
        results = []

        # Keep running the query until no more results are returned
        while True:
            # Update the variables with the current pagination values
            variables['first'] = first
            variables['skip'] = skip

            # Execute the query
            result = self.client.execute(query, variable_values=variables)

            # Check for errors in the response
            if 'errors' in result:
                raise Exception(result['errors'])

            # Extract the baseEntity type (a single entity by query
            # must be asked for it to work) to have the key of the
            # list of result
            base_entity = list(result.keys())[0]

            # Extract the data from the response
            data = result[base_entity]

            # Break the loop if there are no more results
            if len(data) == 0:
                break

            # Add the results to the list
            results.extend(data)

            # Increase the skip value by the number of returned results
            skip += len(data)

        return results
'''