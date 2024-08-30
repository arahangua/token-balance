# lido-balance# Lido Balance Pipeline

This project implements a pipeline to fetch and process Lido staking balances on the Ethereum network.

## Files in the Repository

- `lido_balance_pipeline.py`: Main script for fetching and processing Lido balances.
- `utils.py`: Utility functions for web3 interactions and balance calculations.
- `config.py`: Configuration settings for the project.
- `.gitignore`: Specifies intentionally untracked files to ignore.
- `LICENSE`: License information for the project.

## Setup

1. Clone the repository
2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```
3. Set up your environment variables (if necessary)

## Usage

Run the main script:

```
python lido_balance_pipeline.py
```

## Development

This project uses pre-commit hooks to maintain code quality. To set up the pre-commit hooks:

1. Install pre-commit:
   ```
   pip install pre-commit
   ```
2. Install the git hook scripts:
   ```
   pre-commit install
   ```

## Code Quality

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)
[![Linting: ruff](https://img.shields.io/badge/linting-ruff-purple)](https://github.com/astral-sh/ruff)

This project follows the Black code style, uses isort for import sorting, and Ruff for linting.

## License

See the [LICENSE](LICENSE) file for details.
