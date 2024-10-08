import asyncio
import csv

import pandas as pd
from rich.progress import Progress, TextColumn, BarColumn, TaskProgressColumn

from config import (BATCH_SIZE, BLOCK_STEP_SIZE, END_BLOCK, OUTPUT_FILE,
                    START_BLOCK, TOKEN_ADDRESS, USE_PRECOMPUTED_LIST,
                    WEI_THRESHOLD)
from utils import (get_balances_batch, get_token_contract, get_web3,
                   wei_to_ether)


async def fetch_block(web3, block_number):
    """Fetch a single block asynchronously."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, web3.eth.get_block, block_number, True)


async def fetch_blocks(web3, start_block, end_block):
    """Fetch multiple blocks concurrently using BLOCK_STEP_SIZE intervals."""
    tasks = []
    for block_number in range(start_block, end_block + 1, BLOCK_STEP_SIZE):
        if block_number <= end_block:
            tasks.append(fetch_block(web3, block_number))
    return await asyncio.gather(*tasks)


async def get_participants_async(web3, start_block, end_block, batch_size=100):
    """
    Get unique addresses that interacted with the token contract
    using batched processing.
    """
    participants = set()
    for batch_start in range(start_block, end_block + 1, batch_size):
        batch_end = min(batch_start + batch_size - 1, end_block)
        blocks = await fetch_blocks(web3, batch_start, batch_end)
        for block in blocks:
            for tx in block["transactions"]:
                if tx["to"] == TOKEN_ADDRESS:
                    participants.add(tx["from"])
    return list(participants)


def get_participants(web3, start_block, end_block, batch_size=100):
    """Wrapper function to run the async get_participants_async function."""
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(
        get_participants_async(web3, start_block, end_block, batch_size)
    )


async def process_batch_async(web3, contract, participants, start_block, end_block):
    """Process a batch of blocks asynchronously and return balance data."""
    balances = []
    blocks = await fetch_blocks(web3, start_block, end_block)
    for block in blocks:
        block_number = block["number"]
        block_balances = await asyncio.to_thread(
            get_balances_batch, web3, contract, participants, block_number
        )
        non_zero_balances = {
            address: wei_to_ether(balance)
            for address, balance in zip(participants, block_balances)
            if balance >= WEI_THRESHOLD
        }
        balances.append((block_number, non_zero_balances))
    return balances


async def main():
    web3 = get_web3()
    contract = get_token_contract(web3)

    if USE_PRECOMPUTED_LIST:
        participants_df = pd.read_csv("uniq_addrs.csv")
        participants = participants_df["address"].tolist()
        print(f"Using precomputed list with {len(participants)} participants")
    else:
        participants = await get_participants_async(web3, START_BLOCK, END_BLOCK)
        print(f"Found {len(participants)} participants")

    total_blocks = (END_BLOCK - START_BLOCK) // BLOCK_STEP_SIZE + 1

    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TextColumn("{task.completed}/{task.total} blocks"),
    ) as progress:
        task = progress.add_task("Processing blocks", total=total_blocks)

        async with asyncio.Lock():
            with open(OUTPUT_FILE, "w", newline="") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(["Block", "Address", "Balance"])

                tasks = []
                for batch_start in range(START_BLOCK, END_BLOCK + 1, BATCH_SIZE):
                    batch_end = min(batch_start + BATCH_SIZE - 1, END_BLOCK)

                    task_async = asyncio.create_task(
                        process_batch_async(
                            web3, contract, participants, batch_start, batch_end
                        )
                    )
                    tasks.append(task_async)

                results = await asyncio.gather(*tasks)

                for batch_balances in results:
                    for block, balances in batch_balances:
                        for address, balance in balances.items():
                            writer.writerow([block, address, balance])
                        if (block - START_BLOCK) % BLOCK_STEP_SIZE == 0:
                            progress.update(task, advance=1)
    print(f"Pipeline completed. Results written to {OUTPUT_FILE}")


if __name__ == "__main__":
    asyncio.run(main())
