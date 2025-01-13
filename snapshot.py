from db_operations import DatabaseOperations
from web3_operations import Web3Operations
from config import TOKEN_CONFIG
import argparse

def fetch_and_store_transfer_events(db_ops, web3_ops, start_block, end_block, batch_size=100):
    current_block = start_block
    
    while current_block <= end_block:
        batch_end = min(current_block + batch_size - 1, end_block)
        print(f"Fetching blocks {current_block} to {batch_end}")
        
        try:
            batch_values = web3_ops.get_transfer_events(current_block, batch_end)
            if batch_values:
                inserted = db_ops.store_transfers(batch_values)
                print(f"Inserted {inserted} transfers")

        except Exception as e:
            print(f"Error processing blocks {current_block}-{batch_end}: {e}")
            batch_size = max(1, batch_size // 2)
            continue
            
        current_block = batch_end + 1

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--reset', action='store_true', help='Reset the database before starting')
    parser.add_argument('--token', default='AERO', help='Token to process (default: AERO)')
    args = parser.parse_args()

    if args.token not in TOKEN_CONFIG:
        print(f"Error: Token {args.token} not found in config")
        return

    token_data = TOKEN_CONFIG[args.token]
    db_ops = DatabaseOperations(reset=args.reset)
    web3_ops = Web3Operations(token_data['address'])

    try:
        start_block = db_ops.get_last_processed_block(token_data['start_block'], args.reset)
        print(f"Last processed block: {start_block}")
        end_block = token_data['end_block']
        print(f"Processing {args.token} from block {start_block} to {end_block}")
        
        fetch_and_store_transfer_events(db_ops, web3_ops, start_block, end_block, batch_size=1000)
        db_ops.generate_snapshot(args.token)
    
    finally:
        db_ops.close()

if __name__ == "__main__":
    main()
