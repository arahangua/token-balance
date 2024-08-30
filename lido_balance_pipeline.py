import csv
from web3 import Web3
from config import START_BLOCK, END_BLOCK, BATCH_SIZE, OUTPUT_FILE
from utils import get_web3, get_lido_contract, get_balance, wei_to_ether

def get_participants(web3, start_block, end_block):
    """Get unique addresses that interacted with the Lido contract."""
    participants = set()
    for block in range(start_block, end_block + 1):
        block_data = web3.eth.get_block(block, full_transactions=True)
        for tx in block_data['transactions']:
            if tx['to'] == LIDO_TOKEN_ADDRESS:
                participants.add(tx['from'])
    return list(participants)

def process_batch(web3, contract, participants, start_block, end_block):
    """Process a batch of blocks and return balance data."""
    balances = []
    for block in range(start_block, end_block + 1):
        block_balances = {}
        for address in participants:
            balance = get_balance(contract, address, block)
            if balance > 0:
                block_balances[address] = wei_to_ether(balance)
        balances.append((block, block_balances))
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
