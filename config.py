# Ethereum network configuration
ETHEREUM_NODE_URL = "http://127.0.0.1:8547"

# Lido stETH token contract address
LIDO_TOKEN_ADDRESS = "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84"

# Block range for the pipeline
START_BLOCK = 12000000  # Replace with your desired start block
END_BLOCK = 12000100  # Replace with your desired end block

# Number of blocks to process in each batch
BATCH_SIZE = 1000

# Output file path
OUTPUT_FILE = "lido_token_balances.csv"

# Pipeline configuration
USE_PRECOMPUTED_LIST = False
WEI_THRESHOLD = 1
BLOCK_STEP_SIZE = 100  # Interval between blocks to collect data
