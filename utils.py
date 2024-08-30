from web3 import Web3
from config import ETHEREUM_NODE_URL, LIDO_TOKEN_ADDRESS

def get_web3():
    """Initialize and return a Web3 instance."""
    return Web3(Web3.HTTPProvider(ETHEREUM_NODE_URL))

def get_lido_contract(web3):
    """Return the Lido stETH token contract."""
    abi = [{"constant":True,"inputs":[{"name":"_owner","type":"address"}],"name":"balanceOf","outputs":[{"name":"balance","type":"uint256"}],"type":"function"}]
    return web3.eth.contract(address=LIDO_TOKEN_ADDRESS, abi=abi)

def get_balance(contract, address, block):
    """Get the balance of an address at a specific block."""
    return contract.functions.balanceOf(address).call(block_identifier=block)

def wei_to_ether(wei_value):
    """Convert wei to ether."""
    return Web3.from_wei(wei_value, 'ether')
