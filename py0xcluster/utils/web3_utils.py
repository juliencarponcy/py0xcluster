from web3 import Web3
from pandas import read_csv

infura_API_path = r'C:\Users\phar0732\Documents\ID\infura\infura_api.csv'
infura_token = str(read_csv(infura_API_path, header=None).values[0][0])

infura_token
infura_url = 'https://mainnet.infura.io/v3/' + infura_token
web3 =  Web3(Web3.HTTPProvider(infura_url))

def get_nonce(address) -> int:
    nonce = web3.eth.getTransactionCount(Web3.toChecksumAddress(address))
    return nonce

def is_contract(address) -> bool:
    code = web3.eth.get_code(Web3.toChecksumAddress(address))
    if code.__len__() > 0:
        return True
    else:
        return False
        