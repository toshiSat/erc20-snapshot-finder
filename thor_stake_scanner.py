from web3 import Web3
import psycopg2
from config import DB_CONFIG, RPC_URL, BATCH_SIZE

class ThorStakeScanner:
    def __init__(self, contract_address, reset=False):
        # Connect to Ethereum node
        self.w3 = Web3(Web3.HTTPProvider(RPC_URL))
        self.contract_address = Web3.to_checksum_address(contract_address)
        
        # Event signatures
        self.stake_topic = self.w3.keccak(
            text="Stake(address,uint256,string)").hex()
        self.unstake_topic = self.w3.keccak(
            text="Unstake(address,uint256,uint256)").hex()
        
        # Database connection
        self.db_conn = psycopg2.connect(**DB_CONFIG)
        self.cursor = self.db_conn.cursor()

        if reset:
            self.reset_database()
        self.setup_database()

    def setup_database(self):
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS thor_stakes (
                block_number BIGINT,
                transaction_hash TEXT,
                log_index BIGINT,
                holder_address TEXT,
                amount NUMERIC,
                is_staked BOOLEAN,
                UNIQUE(block_number, log_index)
            );
            
            CREATE INDEX IF NOT EXISTS idx_thor_holder ON thor_stakes(holder_address);

            -- Track scanning progress
            CREATE TABLE IF NOT EXISTS thor_scan_progress (
                id INTEGER PRIMARY KEY,
                last_block BIGINT
            );
        ''')
        self.db_conn.commit()

    def reset_database(self):
        print("Resetting database...")
        self.cursor.execute('DROP TABLE IF EXISTS thor_stakes CASCADE;')
        self.cursor.execute('DROP TABLE IF EXISTS thor_scan_progress CASCADE;')
        self.db_conn.commit()

    def parse_event(self, log, is_stake):
        try:
            # Get account from first indexed parameter
            account = Web3.to_checksum_address('0x' + log['topics'][1].hex()[-40:])
            
            # Get the raw hex data without '0x' prefix
            raw_data = log['data'].hex()[2:]
            
            # First 32 bytes (64 chars) is always the amount
            amount_hex = raw_data[:64]
            
            # For unstake events, remove the extra two zeros if they exist
            if not is_stake and amount_hex.endswith('00'):
                amount_hex = amount_hex[:-2]
            
            amount = int(amount_hex, 16)
            
            # Debug print
            print(f"Event type: {'Stake' if is_stake else 'Unstake'}")
            print(f"Raw amount hex: {raw_data[:64]}")
            print(f"Normalized hex: {amount_hex}")
            print(f"Parsed amount: {amount}")
            
            if not is_stake:
                amount = -amount  # Make unstake amounts negative
                
            print(f"Final amount: {amount}")
            
            return (
                log['blockNumber'],
                log['transactionHash'].hex(),
                log['logIndex'],
                account,
                amount,
                is_stake
            )
            
        except Exception as e:
            print(f"Error parsing event: {e}")
            print(f"Full log data: {log}")
            print(f"Raw data hex: {log['data'].hex()}")
            return None

    def get_last_processed_block(self):
        self.cursor.execute('''
            SELECT last_block FROM thor_scan_progress WHERE id = 1
        ''')
        result = self.cursor.fetchone()
        if result:
            return result[0]
        
        self.cursor.execute('''
            SELECT MAX(block_number) FROM thor_stakes
        ''')
        result = self.cursor.fetchone()
        return result[0] if result and result[0] is not None else None

    def update_progress(self, block_number):
        self.cursor.execute('''
            INSERT INTO thor_scan_progress (id, last_block)
            VALUES (1, %s)
            ON CONFLICT (id) DO UPDATE 
            SET last_block = EXCLUDED.last_block
        ''', (block_number,))
        self.db_conn.commit()

    def scan_blocks(self, start_block, end_block):
        last_processed = self.get_last_processed_block()
        if last_processed:
            print(f"Resuming from last processed block: {last_processed:,}")
            start_block = last_processed + 1

        current_block = start_block
        batch_size = BATCH_SIZE
        
        while current_block <= end_block:
            batch_end = min(current_block + batch_size - 1, end_block)
            print(f"Scanning blocks {current_block:,} to {batch_end:,} (batch size: {batch_size:,})")
            
            try:
                logs = self.w3.eth.get_logs({
                    'fromBlock': current_block,
                    'toBlock': batch_end,
                    'address': self.contract_address,
                    'topics': [
                        [
                            "0x" + self.stake_topic,
                            "0x" + self.unstake_topic
                        ]
                    ]
                })
                
                batch_values = []
                for log in logs:
                    is_stake = log['topics'][0].hex() == self.stake_topic
                    parsed = self.parse_event(log, is_stake)
                    if parsed:
                        batch_values.append(parsed)
                
                if batch_values:
                    self.cursor.executemany('''
                        INSERT INTO thor_stakes 
                        (block_number, transaction_hash, log_index, 
                         holder_address, amount, is_staked)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (block_number, log_index) 
                        DO NOTHING
                    ''', batch_values)
                    self.update_progress(batch_end)
                    self.db_conn.commit()
                    print(f"Inserted {len(batch_values):,} events")
                
                current_block = batch_end + 1
                
            except Exception as e:
                print(f"Error processing blocks {current_block:,}-{batch_end:,}: {e}")
                batch_size = max(100, batch_size // 2)
                print(f"Reducing batch size to {batch_size:,}")
                continue

    def generate_balances(self, output_file='thor_stakes.csv'):
        print("Calculating balances and generating CSV...")
        self.cursor.execute('''
            SELECT 
                holder_address,
                SUM(amount) as staked_amount
            FROM thor_stakes
            GROUP BY holder_address
            HAVING SUM(amount) > 0
            ORDER BY staked_amount DESC;
        ''')
        
        total_balance = 0
        records = self.cursor.fetchall()
        print(f"Found {len(records)} holders with positive balances")
        
        print(f"Writing to {output_file}...")
        with open(output_file, 'w') as f:
            f.write('holder,staked_amount\n')
            for record in records:
                f.write(f'{record[0]},{record[1]}\n')
                total_balance += record[1]
        
        print(f"Total staked amount: {total_balance}")

    def close(self):
        self.cursor.close()
        self.db_conn.close() 