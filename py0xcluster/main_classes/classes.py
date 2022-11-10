from datetime import datetime, timedelta

import pandas as pd
import numpy as np

from py0xcluster.utils import web3_utils, requests_utils, query_utils

### Helpers to store elsewhere
def df_cols_to_numeric(dataframe: pd.DataFrame, numeric_cols: list = None) -> pd.DataFrame:
    for col in numeric_cols:
        dataframe[numeric_cols] = dataframe[numeric_cols].astype(float)
    return dataframe

def format_dates(start_date: tuple = None, end_date: tuple = None):
    # initialization for pagination of query results
    if not start_date:
        start_date = datetime.now() - timedelta(1)
    if not end_date:
        end_date = datetime.now()
    
    # Date formatting
    start_date = query_utils.timestamp_tuple_to_unix(start_date)
    end_date = query_utils.timestamp_tuple_to_unix(end_date)
    return start_date, end_date

def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]

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
        # TODO write setters and getters
        self.pair_addresses = None
        self.pair_symbols = None
        self.pools_summary = None

    def get_pools(
            self, 
            min_daily_volume: int = 100000, 
            min_daily_txns: int = 200,
            min_txns_per_token: int = 50000, 
            start_date: tuple = None, 
            end_date: tuple = None
            ) -> pd.DataFrame:

        start_date, end_date = format_dates(start_date, end_date)

        baseobjects = 'pairDayDatas'

        query = '''
            query($max_rows: Int $skip: Int $dailyVolumeUSD_gt:Int, $dailyTxns_gt:Int, $start_date:Int, $end_date:Int)
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
                        date_lte: $end_date
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
                reserveUSD
                }
            }
            '''

        variables = {
            'dailyVolumeUSD_gt' : min_daily_volume,
            'dailyTxns_gt' : min_daily_txns,
            'start_date': start_date,
            'end_date' : end_date
            }

        full_df = requests_utils.df_from_queries(self.subgraph_url, query, variables, baseobjects)
        
        numeric_cols = [
            'dailyTxns', 
            'reserveUSD', 
            'dailyVolumeUSD', 
            'token0.totalLiquidity', 
            'token0.txCount', 
            'token1.totalLiquidity',
            'token1.txCount']
        
        # Formatting
        full_df = df_cols_to_numeric(full_df, numeric_cols)
        full_df['date'] = pd.to_datetime(full_df['date'], unit='s')
        
        
        pools_summary = full_df.groupby('pairAddress').agg(
            {'dailyTxns': np.mean, 
            'dailyVolumeUSD':np.mean,
            'reserveUSD' : np.mean,
            'token0.totalLiquidity': np.mean,
            'token1.totalLiquidity': np.mean,
            'token0.txCount' : np.mean,
            'token1.txCount' : np.mean
            })

        pool_id_cols = ['pairAddress','token0.symbol','token1.symbol','token0.id','token1.id']
        pools_id_df = full_df[pool_id_cols].drop_duplicates('pairAddress')

        pools_summary = pools_summary.merge(pools_id_df, how='left', on='pairAddress').sort_values('reserveUSD', ascending=False)  
        # Discarding tokens with low total txns
        pools_summary = pools_summary[(pools_summary['token0.txCount'] > min_txns_per_token) & (pools_summary['token1.txCount'] > min_txns_per_token)]
        pools_summary.reset_index(drop=True, inplace=True)
        self.pair_addresses = pools_summary['pairAddress'].values.tolist()
        self.pair_symbols = pools_summary[['token0.symbol','token1.symbol']].values

        self.pools_summary = pools_summary
        return pools_summary, full_df


class Trades():
    def __init__(self, symbols):
        self.symbol = symbols

        self.data = pd.DataFrame()

class TradesDEX(Trades):
    def __init__(self, symbol:str = None):
        Trades.__init__(self, symbol)
    #headers = {"Authorization": "token <token_here>"} # might serve later
        
#    """docstring for TradesDEX"""
#    def __init__(self):
#        super(graphQL, self).__init__() # Ineresting this use of super, dont fully understand why he would do that
#        print(self.variables["subgraph_url"],' ',self.variables["pair_address"])
    def get_pools_events(
            self, 
            pair_addresses: list = None,             
            start_date: tuple = None, 
            end_date: tuple = None
            ) -> pd.DataFrame:

        baseobjects = ['mints', 'burns', 'swaps']

        query = '''
        query($allPairs: [String!], $timestamp_start:BigInt $timestamp_end:BigInt) {
            mints(first: 30, 
                where: { pair_in: $allPairs timestamp_gt: $timestamp_start}, orderBy: timestamp, orderDirection: desc) {
                
            first: $max_rows
            skip: $skip
            orderBy: timestamp
            orderDirection: desc
            where: {
                pair_in: $pair_addresses
                timestamp_gte: $timestamp_start
                timestamp_lt: $timestamp_end
                }

            transaction {
                id
                timestamp
            }
            to
            liquidity
            amount0
            amount1
            amountUSD
            }
            burns(first: $max_rows, where: { pair_in: $allPairs }, orderBy: timestamp, orderDirection: desc) {
            transaction {
                id
                timestamp
            }
            to
            liquidity
            amount0
            amount1
            amountUSD
            }
            swaps(first: 30, where: { pair_in: $allPairs }, orderBy: timestamp, orderDirection: desc) {
            transaction {
                id
                timestamp
            }
            amount0In
            amount0Out
            amount1In
            amount1Out
            amountUSD
            to
            
            }
            }
            '''

        variables = {
            'pair_addresses' : pair_addresses,
            'start_date': start_date,
            'end_date' : end_date
            }

        full_df = requests_utils.df_from_queries(self.subgraph_url, query, variables, baseobjects)
        
        numeric_cols = [
            'dailyTxns', 
            'reserveUSD', 
            'dailyVolumeUSD', 
            'token0.totalLiquidity', 
            'token0.txCount', 
            'token1.totalLiquidity',
            'token1.txCount']
        
        # Formatting
        full_df = df_cols_to_numeric(full_df, numeric_cols)
        full_df['date'] = pd.to_datetime(full_df['date'], unit='s')

        return full_df

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
        
        

    def get_swaps(
            self,
            pair_addresses: list = None, 
            min_amoutUSD: int = 2000, 
            start_date: tuple = None, 
            end_date: tuple = None,
            pairs_batch_size = 1,
            days_batch_size = 10,
            verbose = True
            ) -> pd.DataFrame:

        if isinstance(pair_addresses, str):
            pair_addresses = [pair_addresses]

        # TODO implement days_batches
        # TODO adapt day batches to nb of txns by pair
        start_date, end_date = format_dates(start_date, end_date)

        # if dex == 'uni2':
        subgraph_url = 'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2'

        baseobjects = 'swaps'

        # Construct variable query
        query = """
        query($max_rows: Int $skip: Int $pair_addresses: [String!] $min_amoutUSD: Int $start_date: BigInt $end_date: BigInt)
        {
        swaps(
            first: $max_rows
            skip: $skip
            orderBy: timestamp
            orderDirection: desc
            where: {
                amountUSD_gte: $min_amoutUSD
                pair_in: $pair_addresses
                timestamp_gte: $start_date
                timestamp_lte: $end_date
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

        for pairs_batch in batch(pair_addresses, pairs_batch_size):

            variables = {
                'pair_addresses' : pairs_batch,
                'min_amoutUSD' : min_amoutUSD,
                'start_date': start_date,
                'end_date' : end_date
                }

            batch_df = requests_utils.df_from_queries(subgraph_url, query, variables, baseobjects)
            full_df = pd.concat([full_df, batch_df])
            if verbose:
                print('swaps collected so far:', full_df.shape[0])
        
        numeric_cols = [
            'amount0In',
            'amount1In',
            'amount0Out',
            'amount1Out',
            'amountUSD'
            ]
        
        # Formatting
        full_df = df_cols_to_numeric(full_df, numeric_cols)
        full_df['timestamp'] = pd.to_datetime(full_df['timestamp'], unit='s')

        print(f'outputs: {full_df.shape[0]} trades')
        
        self.data = full_df
        return full_df



