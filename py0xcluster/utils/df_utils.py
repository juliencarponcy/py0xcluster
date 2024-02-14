import pandas as pd

def normalize_pools_data(pools_data: list):
    # first level of normalization
    df = pd.json_normalize(pools_data, meta=['pool.inputTokens'])
    df_input_tokens = pd.json_normalize(df['pool.inputTokens'])

    # normalize all columns from df_input_tokens
    df_list = list()

    for col in df_input_tokens.columns:
        v = pd.json_normalize(df_input_tokens[col])
        v.columns = [f'token{col}.{c}' for c in v.columns]
        df_list.append(v)

        # combine into one dataframe
        # TODO do not force index, implement specifcally for pool.inputTokens
        df_normalized = pd.concat([df.iloc[:, 0:8]] + df_list, axis=1)

    return df_normalized
