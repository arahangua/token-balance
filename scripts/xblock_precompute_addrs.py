"""
Get unique addresses interacting with a target address using Dask.

This script uses Dask to retrieve a list of unique addresses that have
interacted with a specified target address within a given block range.

Args:
    block_start (int): The starting block number for the analysis.
    block_end (int): The ending block number for the analysis.
    target_contract (str): The address of the target contract.

Returns:
    None

Outputs:
    uniq_addrs.csv: A single-column CSV file containing unique addresses
                    that interacted with the target contract. The column
                    is named "address".
"""

import os,sys
import glob
import numpy as np
import pandas as pd
import dask.dataframe as dd
from dask.delayed import delayed
from zipfile import ZipFile
from rich.progress import Progress

# block range
block_start = 12000000
block_end = 19000000

# target contract (e.g., stETH)
target_contract = "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84"

# get xblock external transaction files 
xb_root = '/local/scratch/XBlock_dataset'

# get exported block data
bl_info_paths_zipped = glob.glob(f'{xb_root}/*to*_BlockTransaction.zip')

# get valid files 
st_bls = np.array(list(map(lambda x:x.split('/')[-1].split('_')[0].split('to')[0], bl_info_paths_zipped))).astype(int)
ed_bls = np.array(list(map(lambda x:x.split('/')[-1].split('_')[0].split('to')[1], bl_info_paths_zipped))).astype(int)

valid_zip_files = np.array(bl_info_paths_zipped)[(st_bls>=block_start) & (ed_bls<=block_end)]

# dask delayed function to get all unique addresses that call target contract 
def process_path(valid_zip_file):
    with ZipFile(valid_zip_file) as zf:
        # zf.namelist()
        csv_file = [file for file in zf.namelist() if file.endswith('.csv')]
        df = pd.read_csv(zf.open(csv_file[0]))
        #get all rows that has target_contract
        lowered = target_contract.lower()
        df = df[df['to']==lowered]
    return pd.DataFrame(df['from'].unique(), columns=['address'])


# run it
task_len = len(valid_zip_files)
result_list = []
with Progress() as progress:
    task = progress.add_task("[green]Processing...", total=task_len)
    for ii, valid_zip_file in enumerate(valid_zip_files):
        result_list.append(process_path(valid_zip_file))
        progress.update(task, advance=1)

# concat and save
res_df = pd.concat(result_list)
res_df.to_csv('uniq_addrs.csv', index=False)
print(f'getting unique addresses based on xblock data done: n_addrs: {len(res_df)}, start_block: {block_start}, end_block: {block_end}')
