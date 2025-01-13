from web3 import Web3
import psycopg2
from config import DB_CONFIG, RPC_URL, BATCH_SIZE

class NFTLockScanner:
    def __init__(self, contract_address, token_address, reset=False):
        # Connect to Ethereum node
        self.w3 = Web3(Web3.HTTPProvider(RPC_URL))
        self.contract_address = Web3.to_checksum_address(contract_address)
        self.token_address = Web3.to_checksum_address(token_address)
        
        # Event signatures
        self.created_topic = self.w3.keccak(
            text="NFTCreated(uint256,address,uint256,address,uint256)").hex()
        self.redeemed_topic = self.w3.keccak(
            text="NFTRedeemed(uint256,address,uint256,address,uint256)").hex()
        
        # Database connection
        self.db_conn = psycopg2.connect(**DB_CONFIG)
        self.cursor = self.db_conn.cursor()
        print(self.created_topic)
        print(self.redeemed_topic)
        if reset:
            self.reset_database()
        self.setup_database()

    def setup_database(self):
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS nft_locks (
                block_number BIGINT,
                transaction_hash TEXT,
                log_index BIGINT,
                holder_address TEXT,
                amount NUMERIC,
                unlock_date NUMERIC,
                is_staked BOOLEAN,
                UNIQUE(block_number, log_index)
            );
            
            CREATE INDEX IF NOT EXISTS idx_holder ON nft_locks(holder_address);
        ''')
        self.db_conn.commit()

    def reset_database(self):
        print("Resetting database...")
        self.cursor.execute('DROP TABLE IF EXISTS nft_locks CASCADE;')
        self.db_conn.commit()

    def parse_event(self, log, is_stake):
        try:
            # Parse data field - all parameters are in data, not topics
            data = log['data'].hex()  # Convert HexBytes to string
            if not data.startswith('0x'):
                data = '0x' + data
            
            # Data format:
            # [0:64]    - nft_id (uint256)
            # [64:128]  - holder (address)
            # [128:192] - amount (uint256)
            # [192:256] - token (address)
            # [256:320] - unlock_date (uint256)
            
            data = data[2:]  # Remove '0x'
            nft_id = int(data[:64], 16)
            holder = '0x' + data[64:128][-40:]
            holder = Web3.to_checksum_address(holder)
            amount = int(data[128:192], 16)
            # Make amount negative for redemptions
            if not is_stake:
                amount = -amount
            token = '0x' + data[192:256][-40:]
            token = Web3.to_checksum_address(token)
            unlock_date = int(data[256:320], 16)
            
            # Only process events for our target token
            if token != self.token_address:
                return None
            
            return (
                log['blockNumber'],
                log['transactionHash'].hex(),
                log['logIndex'],
                holder,
                amount,  # This will now be negative for redemptions
                unlock_date,
                is_stake
            )
            
        except Exception as e:
            print(f"Error parsing event: {e}")
            print(f"Full log data: {log}")
            return None

    def scan_blocks(self, start_block, end_block):
        current_block = start_block
        batch_size = BATCH_SIZE
        
        while current_block <= end_block:
            batch_end = min(current_block + batch_size - 1, end_block)
            print(f"Scanning blocks {current_block:,} to {batch_end:,} (batch size: {batch_size:,})")
            
            try:
                # Get both types of events in one call with proper topic formatting
                logs = self.w3.eth.get_logs({
                    'fromBlock': current_block,
                    'toBlock': batch_end,
                    'address': self.contract_address,
                    'topics': [
                        [
                            "0x" + self.created_topic,
                            "0x" + self.redeemed_topic
                        ]
                    ]
                })
                
                # Process all events
                batch_values = []
                for log in logs:
                    # Determine if it's a stake or unstake based on the topic
                    print(log['topics'][0].hex())
                    is_stake = log['topics'][0].hex() == self.created_topic
                    parsed = self.parse_event(log, is_stake)
                    if parsed:
                        batch_values.append(parsed)
                
                if batch_values:
                    self.cursor.executemany('''
                        INSERT INTO nft_locks 
                        (block_number, transaction_hash, log_index, 
                         holder_address, amount, unlock_date, is_staked)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (block_number, log_index) 
                        DO NOTHING
                    ''', batch_values)
                    self.db_conn.commit()
                    print(f"Inserted {len(batch_values):,} events")
                
                current_block = batch_end + 1
                
            except Exception as e:
                print(f"Error processing blocks {current_block:,}-{batch_end:,}: {e}")
                batch_size = max(100, batch_size // 2)
                print(f"Reducing batch size to {batch_size:,}")
                continue

    def generate_balances(self, output_file='nft_locks.csv'):
        print("Calculating balances and generating CSV...")
        self.cursor.execute('''
            SELECT 
                holder_address,
                SUM(amount) as locked_amount  -- Just sum the amounts since they're already positive/negative
            FROM nft_locks
            GROUP BY holder_address
            HAVING SUM(amount) > 0
            ORDER BY locked_amount DESC;
        ''')
        
        total_balance = 0
        records = self.cursor.fetchall()
        print(f"Found {len(records)} holders with positive balances")
        
        print(f"Writing to {output_file}...")
        with open(output_file, 'w') as f:
            f.write('holder,locked_amount\n')
            for record in records:
                f.write(f'{record[0]},{record[1]}\n')
                total_balance += record[1]
        
        print(f"Total locked amount: {total_balance}")

    def close(self):
        self.cursor.close()
        self.db_conn.close() 