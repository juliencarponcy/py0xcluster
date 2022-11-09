import pandas as pd
from py0xcluster.utils import web3_utils, requests_utils

class Entity():
    chain_ID: str
    address: str

    def __post_init__(self):
        self.iscontract = web3_utils.is_contract(self.address)
        self.nonce = web3_utils.get_nonce(self.address)

class EntityGroup():
    def __init__(self, addresses):
        ...

class Trades():
    def __init__(self, symbol):
        self.symbol = symbol
        self.data = pd.DataFrame()

class TradesDEX(Trades):
    def __init__(self, symbol:str):
        Trades.__init__(self, symbol)
    #headers = {"Authorization": "token <token_here>"} # might serve later
        
#    """docstring for TradesDEX"""
#    def __init__(self):
#        super(graphQL, self).__init__() # Ineresting this use of super, dont fully understand why he would do that
#        print(self.params["subgraph_url"],' ',self.params["pair_address"])
    def find_pair(self):
        NotImplementedError()
        self.pair_address = []
        return self.pair_address 
    
    def save_swaps(self, pair_address):
        ...
    

    def get_mints(self, dex, params):
        ...
    
    def get_burns(self, dex, params):
        ...

    def preprocess_raw_swaps(self):

        processed_swaps = self.data
        processed_swaps['buy'] = not processed_swaps['amout0In'].astype(bool)
        processed_swaps.drop(
            ['amount0In','amount1In','amount0Out','amount1Out','logIndex', 'timestamp', 'pair.token0.id', 'pair.token0.id'],
            axis=1,
            inplace=True)
        
        

    def get_swaps(self, dex, params):


        if dex == 'uni2':
            subgraph_url = 'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2'
        # print('vartype', type(variables['max_rows']))
        #Variable declaration for the query, these are reffered in the query with $ 
       
                    #todo, start end etc 
        
        # $pair_address
        # 0xce84867c3c02b05dc570d0135103d3fb9cc19433
        # Construct variable query
        queryTemplate = """
        query($max_rows: Int $skip: Int $pair_address: ID! $timestamp_start: BigInt $timestamp_end: BigInt)
        {
        swaps(
            first: $max_rows
            skip: $skip
            orderBy: timestamp
            orderDirection: desc
            where: {
                pair: $pair_address
                timestamp_gte: $timestamp_start
                timestamp_lt: $timestamp_end
                }
            ) 
            {
          
            id
            transaction {
                id
                }
            timestamp
            sender
            to
            amount0In
            amount1In
            amount0Out
            amount1Out
            logIndex         
            amountUSD
            pair {
                token0 {
                    symbol
                    id
                    }
                token1 {
                    symbol
                    id
                    }
                }
            }
        }
        """
        full_df = pd.DataFrame()
        
        params['max_rows'] = 500
        params['skip'] = 0

        resp = requests_utils.run_query(subgraph_url, queryTemplate, params)
        resp_bout = pd.json_normalize(resp['data']['swaps'],  max_level=2)
        # print(resp_bout)
        while resp_bout.shape[0] > 0:
            
            resp = requests_utils.run_query(subgraph_url, queryTemplate, params)
            resp_bout = pd.json_normalize(resp['data']['swaps'],  max_level=2)
            
            # print(resp_bout.head(5))

            full_df = pd.concat([full_df, resp_bout], axis=0)
            params['skip'] += params['max_rows']

        full_df[['amount0In','amount1In','amount0Out','amount1Out','amountUSD']] = full_df[['amount0In','amount1In','amount0Out','amount1Out','amountUSD']].astype(float)    
        full_df['pd_datetime'] = pd.Series(pd.to_datetime(pd.to_numeric(full_df['timestamp'], downcast=None),unit='s'))
        print(f'outputs: {full_df.shape[0]} trades')
        
        self.data = full_df
        return full_df


