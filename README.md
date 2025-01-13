# ERC20 Token Snapshot Tool

A tool to create token holder snapshots for ERC20 tokens on any EVM network.  A lot of the tools I found stored transfer data in json files then read each one to generate a CSV.  This was not useful when dealing with blockchains and tokens that had a lot of Transfer events as it would take a long time, with a lot of resources, and usually OOM.  This Project stores all the data in a Postgres DB and then uses that to generate the CSV's.

## Project Structure

```
├── db_operations.py      # Database operations and snapshot generation
├── web3_operations.py    # Web3 interactions and event processing
├── snapshot.py          # Main script
├── config.py           # Configuration settings
├── config-example.py   # Example configuration template
├── run_snapshot.sh     # Executable shell script
├── .gitignore         # Git ignore file
└── Output files:
    ├── DEGEN-Base.csv  # Generated snapshot for DEGEN token
    └── AERO-BASE.csv   # Generated snapshot for AERO token
```

## Prerequisites

- Python 3.8+
- PostgreSQL
- Homebrew (for macOS users)

## Installation

1. Clone the repository:

    ```
    git clone <repository-url>
    cd <repository-name>
    ```

2. Create and activate a virtual environment:
   ```
   python3 -m venv venv
   source venv/bin/activate
   ```
3. Install required packages:

    ```
    pip install web3 psycopg2-binary
    ```

4. Set up PostgreSQL:

   ```
    # Install PostgreSQL (if not already installed)
    brew install postgresql@14
    brew services start postgresql@14

    # Create database and user
    createdb erc20_db
    createuser -s user
    psql postgres -c "ALTER USER \"user\" WITH PASSWORD 'password';"
   ```

5. Configure the application:

    ```
    cp config-example.py config.py
    ```

Edit config.py with your:

- RPC URL
- Token configurations
- Database settings

## Usage

1. Make the run script executable:

    ```
    chmod +x run_snapshot.sh
    ```

2. Run the snapshot tool:

   ```
    # Normal run
    ./run_snapshot.sh

    # Reset database and run
    ./run_snapshot.sh --reset

    # Process specific token
    ./run_snapshot.sh --token AERO
   ```

## Configuration

### Adding New Tokens

Add new tokens to config.py:

```
TOKEN_CONFIG = {
    'TOKEN_NAME': {
        'address': '0x...',  # Token contract address
        'start_block': 1000000,  # First block to scan
        'end_block': 2000000     # Last block to scan
    }
}
```

### Batch Size

Adjust BATCH_SIZE in config.py to control how many blocks are processed at once. Default is 10000.

### Database Settings

Modify DB_CONFIG in config.py or set environment variables:

- DB_NAME
- DB_USER
- DB_PASSWORD
- DB_HOST

## Output Files

The tool generates CSV files in the following format:

- snapshot_TOKEN.csv (e.g., snapshot_AERO.csv)
  Each CSV contains:
- address: Token holder's address
- balance: Current token balance

## Development

### Key Files

- db_operations.py: Handles all database operations including table creation, data insertion, and snapshot generation
- web3_operations.py: Manages Web3 interactions, including fetching transfer events
- snapshot.py: Main script coordinating the snapshot process
- run_snapshot.sh: Shell script for easy execution

### Error Handling

- The script includes automatic retry with reduced batch size on failures
- Database operations are wrapped in transactions
- Duplicate transfers are automatically skipped

