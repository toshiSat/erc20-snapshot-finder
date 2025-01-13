import os

# Token configuration
TOKEN_CONFIG = {
    'AERO': { # BASE blockchain
        'address': '0x940181a94a35a4569e4529a3cdfb74e38fd98631',
        'start_block': 3200550,
        'end_block': 24450125
    }
    # Add more tokens as needed:
    # 'TOKEN2': {
    #     'address': '0x...',
    #     'start_block': 1000000,
    #     'end_block': 2000000
    # }
}

# RPC configuration
RPC_URL = '' 

# Database configuration
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME', 'erc20_db'),
    'user': os.getenv('DB_USER', 'user'),
    'password': os.getenv('DB_PASSWORD', 'password'),
    'host': os.getenv('DB_HOST', 'localhost')
} 