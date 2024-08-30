# Ethereum network configuration
ETHEREUM_NODE_URL = "https://mainnet.infura.io/v3/YOUR-PROJECT-ID"

# Lido stETH token contract address
LIDO_TOKEN_ADDRESS = "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84"

# Block range for the pipeline
START_BLOCK = 12000000  # Replace with your desired start block
END_BLOCK = 12100000    # Replace with your desired end block

# Number of blocks to process in each batch
BATCH_SIZE = 1000

# Output file path
OUTPUT_FILE = "lido_token_balances.csv"
