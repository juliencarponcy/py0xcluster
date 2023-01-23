from web3 import Web3
from pandas import read_csv

# Uncomment the following if you use a 3rd party API
# Adapt the following path to where you have stored your API key
# infura_API_path = r'/home/fujiju/Documents/ID/infura_api.csv'
# infura_token = str(read_csv(infura_API_path, header=None).values[0][0])

# infura_token
# infura_url = 'https://mainnet.infura.io/v3/' + infura_token

# web3 =  Web3(Web3.HTTPProvider(infura_url))

# And comment the following line if you're not using a local node

web3 =  Web3(Web3.HTTPProvider('http://127.0.0.1:8545'))

def get_eth_balance(address, block_number: int) -> float:
    # balance = web3.eth.get_balance(Web3.toChecksumAddress(address), int(block_number))
    balance = web3.eth.get_balance(Web3.toChecksumAddress(address))
    balance = Web3.fromWei(balance, 'ether')
    return balance

def get_nonce(address) -> int:
    nonce = web3.eth.getTransactionCount(Web3.toChecksumAddress(address))
    return nonce

def is_contract(address) -> bool:
    code = web3.eth.get_code(Web3.toChecksumAddress(address))
    if code.__len__() > 0:
        return True
    else:
        return False
        