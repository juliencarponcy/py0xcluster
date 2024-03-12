import pandas as pd
import numpy as np
from decimal import Decimal
import json

class PoolDataProcessor:
    def __init__(self):
        ...

    def transform(self, data: dict):
        pool_days_df = self._normalize_main_data(data)
        
        for col in pool_days_df.columns:
            pool_days_df[col] = self._handle_data_type(pool_days_df[col])
        
        mints_df, burns_df = self.extract_liquidity_events(pool_days_df)
        
        pool_days_df.drop(columns=['pool.mints','pool.burns'], inplace=True)
        
        mints_df = mints_df.apply(lambda x: self._handle_data_type(x))
        burns_df = burns_df.apply(lambda x: self._handle_data_type(x))

        return pool_days_df, mints_df, burns_df
    
    def _normalize_main_data(self, nested_data_dict: dict):
        self.data = nested_data_dict
        # Normalize the main data
        if len(self.data.keys()) > 1:
            raise ValueError('The data has more than one main key') 
         
        pool_days_df = pd.json_normalize(self.data[list(self.data.keys())[0]])
        return pool_days_df
    
    def _handle_data_type(self, series: pd.Series):
        if series.name.__contains__('date'):
            series = pd.to_datetime(series, unit='s')
        elif series.name.__contains__('symbol') or series.name.__contains__('name') or series.name.__contains__('id'):
            series = series.astype('string')
        elif series.name.__contains__('X128'):
            series = series.apply(lambda x: np.float64(Decimal(x))/(2^128))
        elif series.name.__contains__('X96'):
            series = series.apply(lambda x: np.float64(Decimal(x))/(2^96))
        elif series.name.__contains__('Count') or series.name.__contains__('tick'):
            series = series.astype('int64')
        elif series.name in ['sender', 'owner','origin']:
            series = series.astype('string')
        else:
            try:
                series = series.astype('float64')
            except:
                pass
        return series
    
    
    def _normalize_nested_response(self):
        df_dict = {}
        for key, value in self.data.items():
            if isinstance(value, list):
                df_dict[key] = pd.json_normalize(value)
            elif isinstance(value, dict):
                for subkey, subvalue in value.items():
                    df_dict[f'{key}.{subkey}'] = pd.json_normalize(subvalue)
        
        return df_dict

    def extract_liquidity_events(self, pool_df):
        # Extract liquidity events from the data
        all_mints_df = pd.DataFrame()
        all_burns_df = pd.DataFrame()
        for index, row in pool_df.iterrows():
            mints_df = pd.json_normalize(row['pool.mints'])
            burns_df = pd.json_normalize(row['pool.burns'])
            mints_df['pool.id'] = row['id']
            burns_df['pool.id'] = row['id']
            mints_df['pool.token0.symbol'] = row['pool.token0.symbol']
            mints_df['pool.token1.symbol'] = row['pool.token1.symbol']
            burns_df['pool.token0.symbol'] = row['pool.token0.symbol']
            burns_df['pool.token1.symbol'] = row['pool.token1.symbol']

            all_mints_df = pd.concat([all_mints_df, mints_df], axis=0)
            all_burns_df = pd.concat([all_burns_df, burns_df], axis=0)

        all_mints_df.reset_index(drop=True, inplace=True)
        all_burns_df.reset_index(drop=True, inplace=True)

        return all_mints_df, all_burns_df
    
class DataStore:
    def __init__(self):
        # Initialize data storage here
        pass

    def store(self, data):
        # Implement data storage logic here
        pass

    def retrieve(self, criteria):
        # Implement data retrieval logic here
        pass

class DataPipeline:
    def __init__(self, normalizer, store):
        self.normalizer = normalizer
        self.store = store

    def process(self, data):
        normalized_data = self.normalizer.normalize(data)
        self.store.store(normalized_data)

    def get_data(self, criteria):
        return self.store.retrieve(criteria)


# Normalize nested data
# pool_df = pd.json_normalize([pool for pool in data['poolDayDatas']['pool']])
# pool.token0_df = pd.json_normalize([pool.token for pool.token in pool_df['pool.token0']])
# pool.token1_df = pd.json_normalize([pool.token for pool.token in pool_df['pool.token1']])
# burns_df = pd.json_normalize([burn for burn in pool_df['burns']])
# mints_df = pd.json_normalize([mint for mint in pool_df['mints']])



# df_dict = normalize_nested_data(data['poolDayDatas']['pool'], ['pool.token0', 'pool.token1', 'burns', 'mints'])