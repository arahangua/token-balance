import csv
import asyncio
import aiohttp
from web3 import Web3
from config import START_BLOCK, END_BLOCK, BATCH_SIZE, OUTPUT_FILE, LIDO_TOKEN_ADDRESS
from utils import get_web3, get_lido_contract, get_balances_batch, wei_to_ether

async def fetch_block(session, web3, block_number):
    """Fetch a single block asynchronously."""
    return await web3.eth.get_block(block_number, full_transactions=True)

async def fetch_blocks(web3, start_block, end_block):
    """Fetch multiple blocks concurrently."""
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_block(session, web3, block_number) for block_number in range(start_block, end_block + 1)]
        return await asyncio.gather(*tasks)

def get_participants(web3, start_block, end_block, batch_size=100):
    """Get unique addresses that interacted with the Lido contract using batched processing."""
    participants = set()
    for batch_start in range(start_block, end_block + 1, batch_size):
        batch_end = min(batch_start + batch_size - 1, end_block)
        blocks = asyncio.run(fetch_blocks(web3, batch_start, batch_end))
        for block in blocks:
            for tx in block['transactions']:
                if tx['to'] == LIDO_TOKEN_ADDRESS:
                    participants.add(tx['from'])
    return list(participants)

def process_batch(web3, contract, participants, start_block, end_block):
    """Process a batch of blocks and return balance data."""
    balances = []
    for block in range(start_block, end_block + 1):
        block_balances = get_balances_batch(web3, contract, participants, block)
        non_zero_balances = {
            address: wei_to_ether(balance)
            for address, balance in zip(participants, block_balances)
            if balance > 0
        }
        balances.append((block, non_zero_balances))
    return balances

def main():
    web3 = get_web3()
    contract = get_lido_contract(web3)
    
    participants = get_participants(web3, START_BLOCK, END_BLOCK)
    print(f"Found {len(participants)} participants")

    with open(OUTPUT_FILE, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Block', 'Address', 'Balance'])

        for batch_start in range(START_BLOCK, END_BLOCK + 1, BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE - 1, END_BLOCK)
            print(f"Processing blocks {batch_start} to {batch_end}")
            
            batch_balances = process_batch(web3, contract, participants, batch_start, batch_end)
            
            for block, balances in batch_balances:
                for address, balance in balances.items():
                    writer.writerow([block, address, balance])

    print(f"Pipeline completed. Results written to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
