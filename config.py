# Ethereum network configuration
ETHEREUM_NODE_URL = "http://127.0.0.1:8547"

# Token contract address (currently set to Lido stETH)
TOKEN_ADDRESS = "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84"

# Block range for the pipeline
START_BLOCK = 12000000  # Replace with your desired start block
END_BLOCK = 12010000  # Replace with your desired end block
BLOCK_STEP_SIZE = 1000

# Number of blocks to process in each batch
BATCH_SIZE = 1000

# Output file path
OUTPUT_FILE = "token_balances.csv"

# Pipeline configuration
USE_PRECOMPUTED_LIST = False  #
WEI_THRESHOLD = 1
