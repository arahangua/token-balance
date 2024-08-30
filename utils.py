from web3 import Web3
from web3.exceptions import ContractLogicError
from config import ETHEREUM_NODE_URL, LIDO_TOKEN_ADDRESS

def get_web3():
    """Initialize and return a Web3 instance."""
    return Web3(Web3.HTTPProvider(ETHEREUM_NODE_URL))

def get_lido_contract(web3):
    """Return the Lido stETH token contract."""
    abi = [{"constant":True,"inputs":[{"name":"_owner","type":"address"}],"name":"balanceOf","outputs":[{"name":"balance","type":"uint256"}],"type":"function"}]
    return web3.eth.contract(address=LIDO_TOKEN_ADDRESS, abi=abi)

def get_balances_batch(web3, contract, addresses, block):
    """Get balances for multiple addresses at a specific block using multicall."""
    multicall = web3.eth.contract(address=web3.eth.chain_id, abi=[])
    calls = [
        (contract.address, contract.encodeABI('balanceOf', [address]))
        for address in addresses
    ]
    try:
        aggregate = multicall.functions.aggregate(calls).call(block_identifier=block)
        return [Web3.to_int(result) for success, result in zip(aggregate[0], aggregate[1]) if success]
    except ContractLogicError:
        # Fallback to individual calls if multicall fails
        return [contract.functions.balanceOf(address).call(block_identifier=block) for address in addresses]

def wei_to_ether(wei_value):
    """Convert wei to ether."""
    return Web3.from_wei(wei_value, 'ether')
