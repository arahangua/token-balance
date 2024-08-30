from web3 import Web3
from web3.exceptions import ContractLogicError
from config import ETHEREUM_NODE_URL, TOKEN_ADDRESS

# Ethereum mainnet Multicall contract address
MULTICALL_ADDRESS = '0xeefBa1e63905eF1D7ACbA5a8513c70307C1cE441'

def get_web3():
    """Initialize and return a Web3 instance."""
    return Web3(Web3.HTTPProvider(ETHEREUM_NODE_URL))

def get_token_contract(web3):
    """Return the token contract."""
    abi = [{"constant":True,"inputs":[{"name":"_owner","type":"address"}],"name":"balanceOf","outputs":[{"name":"balance","type":"uint256"}],"type":"function"}]
    return web3.eth.contract(address=TOKEN_ADDRESS, abi=abi)

def get_balances_batch(web3, contract, addresses, block):
    """Get balances for multiple addresses at a specific block using multicall."""
    multicall_abi = [{"constant":False,"inputs":[{"components":[{"name":"target","type":"address"},{"name":"callData","type":"bytes"}],"name":"calls","type":"tuple[]"}],"name":"aggregate","outputs":[{"name":"blockNumber","type":"uint256"},{"name":"returnData","type":"bytes[]"}],"type":"function"}]
    multicall = web3.eth.contract(address=MULTICALL_ADDRESS, abi=multicall_abi)
    calls = [
        (contract.address, contract.encodeABI('balanceOf', [address]))
        for address in addresses
    ]
    try:
        aggregate = multicall.functions.aggregate(calls).call(block_identifier=block)
        return [Web3.to_int(result) for result in aggregate[1]]
    except ContractLogicError:
        # Fallback to individual calls if multicall fails
        return [contract.functions.balanceOf(address).call(block_identifier=block) for address in addresses]

def wei_to_ether(wei_value):
    """Convert wei to ether."""
    return Web3.from_wei(wei_value, 'ether')
