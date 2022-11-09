import requests
import pandas as pd

# Pagination parameter
NB_BY_QUERY = 500

def run_query(subgraph_url, query, variables): # A simple function to use requests.post to make the API call. Note the json= section.
    
    # variables = dict({(k, v) for (k, v) in variables.items()})
    # max_rows, skip, pair_address, dex_name, timestamp_start, timestamp_end
    # print(variables, type(variables))

    request = requests.post(subgraph_url, json={'query': query, 'variables': variables}) #, headers=self.headers)
    # print(request.status_code)
    if request.status_code == 200:
        #print(request.json())
        return request.json()
    else:
        raise Exception("Query failed to run by returning code of {}. {}".format(request.status_code, query))    

def df_from_queries(subgraph_url, queryTemplate, variables, baseobjects):
    full_df = pd.DataFrame()
    
    variables['max_rows'] = NB_BY_QUERY
    variables['skip'] = 0

    resp = run_query(subgraph_url, queryTemplate, variables)
    resp_bout = pd.json_normalize(resp['data'][baseobjects],  max_level=2)
    # print(resp_bout)'swaps'
    while resp_bout.shape[0] > 0:
        resp = run_query(subgraph_url, queryTemplate, variables)
        resp_bout = pd.json_normalize(resp['data'][baseobjects],  max_level=2)
        
        # print(resp_bout.head(5))

        full_df = pd.concat([full_df, resp_bout], axis=0)
        variables['skip'] += variables['max_rows']
    return full_df
