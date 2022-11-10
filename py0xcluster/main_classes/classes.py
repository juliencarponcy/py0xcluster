from datetime import datetime, timedelta
import pandas as pd
from py0xcluster.utils import web3_utils, requests_utils, query_utils

### Helpers to store elsewhere
def df_cols_to_numeric(dataframe: pd.DataFrame, numeric_cols: list = None) -> pd.DataFrame:
    for col in numeric_cols:
        dataframe[numeric_cols] = dataframe[numeric_cols].astype(float)
    return dataframe


class Entity():
    chain_ID: str
    address: str

    def __post_init__(self):
        self.iscontract = web3_utils.is_contract(self.address)
        self.nonce = web3_utils.get_nonce(self.address)

class EntityGroup():
    def __init__(self, addresses):
        ...

class Pools():
    def __init__(self, dex_name: str = 'univ2', 
            subgraph_url: str = 'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2'):
        
        self.dex_name = dex_name
        self.subgraph_url = subgraph_url

    def get_pools_by_liquidity(self, min_volume: int = 100000, min_txns: int = 200, start_date: tuple = None) -> pd.DataFrame:
        # initialization for pagination of query results
        if not start_date:
            start_date = datetime.now() - timedelta(1)
        
        start_date = query_utils.timestamp_tuple_to_unix(start_date)
        
        baseobjects = 'pairDayDatas'

        query = '''
            query($max_rows: Int $skip: Int $dailyVolumeUSD_gt:Int, $dailyTxns_gt:Int, $start_date:Int)
            {
                pairDayDatas(
                    first: $max_rows
                    skip: $skip
                    orderBy: dailyVolumeUSD 
                    orderDirection:desc 
                    where: {
                        dailyVolumeUSD_gt: $dailyVolumeUSD_gt 
                        dailyTxns_gt: $dailyTxns_gt 
                        date_gte: $start_date
                        }
                    ) 
                {
                id
                pairAddress
                dailyTxns
                date
                token0 {
                id
                symbol
                totalLiquidity
                txCount
                }
                token1 {
                id
                symbol
                totalLiquidity
                txCount
                }
                    dailyVolumeUSD
                }
            }
            '''

        variables = {
            'dailyVolumeUSD_gt' : min_volume,
            'dailyTxns_gt' : min_txns,
            "start_date": start_date
            }

        full_df = requests_utils.df_from_queries(self.subgraph_url, query, variables, baseobjects)
        numeric_cols = ['dailyTxns', 'dailyVolumeUSD', 'token0.totalLiquidity', 'token0.txCount', 'token1.totalLiquidity','token1.txCount']
        full_df['date'] = pd.to_datetime(full_df['date'], unit='s')
        full_df = df_cols_to_numeric(full_df, numeric_cols)
        
        return full_df

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
#        print(self.variables["subgraph_url"],' ',self.variables["pair_address"])
    def find_pair(self):
        NotImplementedError()
        self.pair_address = []
        return self.pair_address 
    
    def save_swaps(self, pair_address):
        ...
    

    def get_mints(self, dex, variables):
        ...
    
    def get_burns(self, dex, variables):
        ...

    def preprocess_raw_swaps(self):

        processed_swaps = self.data
        processed_swaps['buy'] = not processed_swaps['amout0In'].astype(bool)
        processed_swaps.drop(
            ['amount0In','amount1In','amount0Out','amount1Out','logIndex', 'timestamp', 'pair.token0.id', 'pair.token0.id'],
            axis=1,
            inplace=True)
        
        

    def get_swaps(self, dex, variables):


        if dex == 'uni2':
            subgraph_url = 'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2'

        baseobjects = 'swaps'

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

        full_df = requests_utils.df_from_queries(subgraph_url, queryTemplate, variables, baseobjects)

        full_df[['amount0In','amount1In','amount0Out','amount1Out','amountUSD']] = full_df[['amount0In','amount1In','amount0Out','amount1Out','amountUSD']].astype(float)    
        full_df['pd_datetime'] = pd.Series(pd.to_datetime(pd.to_numeric(full_df['timestamp'], downcast=None),unit='s'))
        print(f'outputs: {full_df.shape[0]} trades')
        
        self.data = full_df
        return full_df



