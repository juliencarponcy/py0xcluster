import requests

def run_query(subgraph_url, query, params): # A simple function to use requests.post to make the API call. Note the json= section.
    
    # variables = dict({(k, v) for (k, v) in params.items()})
    # max_rows, skip, pair_address, dex_name, timestamp_start, timestamp_end
    # print(variables, type(variables))

    request = requests.post(subgraph_url, json={'query': query, 'variables': params}) #, headers=self.headers)
    # print(request.status_code)
    if request.status_code == 200:
        #print(request.json())
        return request.json()
    else:
        raise Exception("Query failed to run by returning code of {}. {}".format(request.status_code, query))    
