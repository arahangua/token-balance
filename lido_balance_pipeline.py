import csv
import asyncio
from concurrent.futures import ThreadPoolExecutor
from web3 import Web3
from config import START_BLOCK, END_BLOCK, BATCH_SIZE, OUTPUT_FILE, LIDO_TOKEN_ADDRESS
from utils import get_web3, get_lido_contract, get_balances_batch, wei_to_ether

async def fetch_block(web3, block_number):
    """Fetch a single block asynchronously."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, web3.eth.get_block, block_number, True)

async def fetch_blocks(web3, start_block, end_block):
    """Fetch multiple blocks concurrently."""
    tasks = [fetch_block(web3, block_number) for block_number in range(start_block, end_block + 1)]
    return await asyncio.gather(*tasks)

async def get_participants_async(web3, start_block, end_block, batch_size=100):
    """Get unique addresses that interacted with the Lido contract using batched processing."""
    participants = set()
    for batch_start in range(start_block, end_block + 1, batch_size):
        batch_end = min(batch_start + batch_size - 1, end_block)
        blocks = await fetch_blocks(web3, batch_start, batch_end)
        for block in blocks:
            for tx in block['transactions']:
                if tx['to'] == LIDO_TOKEN_ADDRESS:
                    participants.add(tx['from'])
    return list(participants)

def get_participants(web3, start_block, end_block, batch_size=100):
    """Wrapper function to run the async get_participants_async function."""
    return asyncio.run(get_participants_async(web3, start_block, end_block, batch_size))

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
