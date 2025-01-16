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
        
        # Add Transfer event signature
        self.transfer_topic = self.w3.keccak(
            text="Transfer(address,address,uint256)").hex()
        
        # Database connection
        self.db_conn = psycopg2.connect(**DB_CONFIG)
        self.cursor = self.db_conn.cursor()

        if reset:
            self.reset_database()
        self.setup_database()

    def setup_database(self):
        self.cursor.execute('''
            -- Table for NFT token IDs and their amounts
            CREATE TABLE IF NOT EXISTS nft_tokens (
                token_id NUMERIC PRIMARY KEY,
                amount NUMERIC,
                current_holder TEXT
            );

            -- Table for all events (creates, redeems, transfers)
            CREATE TABLE IF NOT EXISTS nft_locks (
                block_number BIGINT,
                transaction_hash TEXT,
                log_index BIGINT,
                event_type TEXT,  -- 'create', 'redeem', or 'transfer'
                token_id NUMERIC,
                holder_address TEXT,
                from_address TEXT,   -- Added for transfers
                to_address TEXT,     -- Added for transfers
                amount NUMERIC,
                unlock_date NUMERIC,
                UNIQUE(block_number, log_index)
            );
            
            CREATE INDEX IF NOT EXISTS idx_holder ON nft_locks(holder_address);
            CREATE INDEX IF NOT EXISTS idx_token_id ON nft_locks(token_id);
            CREATE INDEX IF NOT EXISTS idx_from ON nft_locks(from_address);
            CREATE INDEX IF NOT EXISTS idx_to ON nft_locks(to_address);
        ''')
        self.db_conn.commit()

    def reset_database(self):
        print("Resetting database...")
        self.cursor.execute('DROP TABLE IF EXISTS nft_locks CASCADE;')
        self.cursor.execute('DROP TABLE IF EXISTS nft_tokens CASCADE;')
        self.db_conn.commit()

    def parse_create_event(self, log):
        try:
            data = log['data'].hex()
            if not data.startswith('0x'):
                data = '0x' + data
            
            data = data[2:]
            token_id = int(data[:64], 16)
            holder = '0x' + data[64:128][-40:]
            holder = Web3.to_checksum_address(holder)
            amount = int(data[128:192], 16)
            token = '0x' + data[192:256][-40:]
            token = Web3.to_checksum_address(token)
            unlock_date = int(data[256:320], 16)
            
            if token != self.token_address:
                return None

            # Store token ID and amount
            self.cursor.execute('''
                INSERT INTO nft_tokens (token_id, amount, current_holder)
                VALUES (%s, %s, %s)
                ON CONFLICT (token_id) DO NOTHING
            ''', (token_id, amount, holder))
            
            return (
                log['blockNumber'],
                log['transactionHash'].hex(),
                log['logIndex'],
                'create',
                token_id,
                holder,      # holder_address
                None,       # from_address (not used for creates)
                None,       # to_address (not used for creates)
                amount,
                unlock_date
            )
            
        except Exception as e:
            print(f"Error parsing create event: {e}")
            print(f"Full log data: {log}")
            return None

    def parse_transfer_event(self, log):
        try:
            token_id = int(log['topics'][3].hex(), 16)
            
            # Get addresses
            from_address = '0x' + log['topics'][1].hex()[-40:]
            to_address = '0x' + log['topics'][2].hex()[-40:]
            from_address = Web3.to_checksum_address(from_address)
            to_address = Web3.to_checksum_address(to_address)
            
            # Ignore transfers from or to zero address
            if from_address == "0x0000000000000000000000000000000000000000" or \
               to_address == "0x0000000000000000000000000000000000000000":
                return None
            
            # Check if this token_id exists in our database
            self.cursor.execute('SELECT amount FROM nft_tokens WHERE token_id = %s', (token_id,))
            result = self.cursor.fetchone()
            if not result:
                return None
                
            amount = result[0]
            
            # Update current holder
            self.cursor.execute('''
                UPDATE nft_tokens 
                SET current_holder = %s 
                WHERE token_id = %s
            ''', (to_address, token_id))
            
            return (
                log['blockNumber'],
                log['transactionHash'].hex(),
                log['logIndex'],
                'transfer',
                token_id,
                None,  # holder_address not used for transfers
                from_address,
                to_address,
                amount,
                0  # no unlock date for transfers
            )
            
        except Exception as e:
            print(f"Error parsing transfer event: {e}")
            print(f"Full log data: {log}")
            return None

    def parse_redeem_event(self, log):
        try:
            data = log['data'].hex()
            if not data.startswith('0x'):
                data = '0x' + data
            
            data = data[2:]
            token_id = int(data[:64], 16)
            holder = '0x' + data[64:128][-40:]
            holder = Web3.to_checksum_address(holder)
            amount = int(data[128:192], 16)
            token = '0x' + data[192:256][-40:]
            token = Web3.to_checksum_address(token)
            unlock_date = int(data[256:320], 16)
            
            if token != self.token_address:
                return None

            # Remove token from tracking since it's been redeemed
            self.cursor.execute('DELETE FROM nft_tokens WHERE token_id = %s', (token_id,))
            
            return (
                log['blockNumber'],
                log['transactionHash'].hex(),
                log['logIndex'],
                'redeem',
                token_id,
                holder,      # holder_address
                None,       # from_address (not used for redeems)
                None,       # to_address (not used for redeems)
                amount,
                unlock_date
            )
            
        except Exception as e:
            print(f"Error parsing redeem event: {e}")
            print(f"Full log data: {log}")
            return None

    def scan_blocks(self, start_block, end_block):
        current_block = start_block
        batch_size = BATCH_SIZE
        max_retries = 3
        min_batch_size = 1000
        
        while current_block <= end_block:
            batch_end = min(current_block + batch_size - 1, end_block)
            print(f"\nScanning blocks {current_block:,} to {batch_end:,} (batch size: {batch_size:,})")
            
            retries = 0
            while retries < max_retries:
                try:
                    # Get create/redeem events
                    create_logs = self.w3.eth.get_logs({
                        'fromBlock': current_block,
                        'toBlock': batch_end,
                        'address': self.contract_address,
                        'topics': [[
                            "0x" + self.created_topic,
                            "0x" + self.redeemed_topic
                        ]]
                    })
                    print(f"Found {len(create_logs)} create/redeem events")
                    
                    # Get transfer events
                    transfer_logs = self.w3.eth.get_logs({
                        'fromBlock': current_block,
                        'toBlock': batch_end,
                        'address': self.contract_address,
                        'topics': ["0x" + self.transfer_topic]
                    })
                    print(f"Found {len(transfer_logs)} transfer events")
                    
                    # Debug log each transfer
                    for log in transfer_logs:
                        block_num = log['blockNumber']
                        tx_hash = log['transactionHash'].hex()
                        token_id = int(log['topics'][3].hex(), 16)
                        from_addr = '0x' + log['topics'][1].hex()[-40:]
                        to_addr = '0x' + log['topics'][2].hex()[-40:]
                        print(f"\nTransfer at block {block_num}")
                        print(f"  TX Hash: {tx_hash}")
                        print(f"  Token ID: {token_id}")
                        print(f"  From: {from_addr}")
                        print(f"  To: {to_addr}")
                        
                        # Check if token exists in our DB
                        self.cursor.execute('SELECT amount FROM nft_tokens WHERE token_id = %s', (token_id,))
                        result = self.cursor.fetchone()
                        if not result:
                            print(f"  WARNING: Token ID {token_id} not found in database")
                        else:
                            print(f"  Amount: {result[0]}")
                    
                    batch_values = []
                    
                    # Process create/redeem events
                    for log in create_logs:
                        event_topic = log['topics'][0].hex()
                        is_create = event_topic == self.created_topic
                        if is_create:
                            parsed = self.parse_create_event(log)
                            if parsed:
                                print(f"\nCreated token {parsed[4]} with amount {parsed[8]}")
                        else:
                            parsed = self.parse_redeem_event(log)
                            if parsed:
                                print(f"\nRedeemed token {parsed[4]} with amount {parsed[8]}")
                        if parsed:
                            batch_values.append(parsed)
                    
                    # Process transfer events
                    for log in transfer_logs:
                        parsed = self.parse_transfer_event(log)
                        if parsed:
                            print(f"\nProcessed transfer of token {parsed[4]} with amount {parsed[8]}")
                            print(f"  From: {parsed[6]}")
                            print(f"  To: {parsed[7]}")
                            batch_values.append(parsed)
                        else:
                            print(f"\nSkipped transfer at block {log['blockNumber']}")
                    
                    if batch_values:
                        self.cursor.executemany('''
                            INSERT INTO nft_locks 
                            (block_number, transaction_hash, log_index, 
                             event_type, token_id, holder_address, from_address, to_address, amount, unlock_date)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (block_number, log_index) 
                            DO NOTHING
                        ''', batch_values)
                        self.db_conn.commit()
                        print(f"\nInserted {len(batch_values):,} events")
                    
                    current_block = batch_end + 1
                    batch_size = min(BATCH_SIZE, batch_size * 2)
                    break
                    
                except Exception as e:
                    print(f"Error processing blocks {current_block:,}-{batch_end:,}: {e}")
                    retries += 1
                    if retries < max_retries:
                        batch_size = max(min_batch_size, batch_size // 2)
                        batch_end = min(current_block + batch_size - 1, end_block)
                        print(f"Retry {retries}/{max_retries} with reduced batch size: {batch_size:,}")
                    else:
                        print(f"Failed after {max_retries} retries, skipping to next batch")
                        current_block = batch_end + 1
                        batch_size = min_batch_size

    def generate_balances(self, output_file='nft_locks.csv'):
        print("Calculating balances and generating CSV...")
        self.cursor.execute('''
            WITH balance_changes AS (
                -- Initial creates
                SELECT 
                    holder_address as address,
                    amount as balance_change
                FROM nft_locks
                WHERE event_type = 'create'
                
                UNION ALL
                
                -- Redeems (negative amounts)
                SELECT 
                    holder_address as address,
                    -amount as balance_change
                FROM nft_locks
                WHERE event_type = 'redeem'
                
                UNION ALL
                
                -- Transfer FROM addresses (subtract amount)
                SELECT 
                    from_address as address,
                    -amount as balance_change
                FROM nft_locks
                WHERE event_type = 'transfer'
                
                UNION ALL
                
                -- Transfer TO addresses (add amount)
                SELECT 
                    to_address as address,
                    amount as balance_change
                FROM nft_locks
                WHERE event_type = 'transfer'
            )
            SELECT 
                address,
                SUM(balance_change) as balance
            FROM balance_changes
            WHERE address IS NOT NULL
            GROUP BY address
            HAVING SUM(balance_change) > 0
            ORDER BY balance DESC
        ''')
        
        records = self.cursor.fetchall()
        print(f"Found {len(records):,} holders with positive balances")
        
        print(f"Writing to {output_file}...")
        with open(output_file, 'w') as f:
            f.write('holder,locked_amount\n')
            for record in records:
                f.write(f'{record[0]},{record[1]}\n')
            
        print(f"Finished writing {len(records):,} records to {output_file}")

    def close(self):
        self.cursor.close()
        self.db_conn.close() 

    def debug_blocks(self, problem_block, range=1):
        """Debug a specific block range around the problem block"""
        start_block = problem_block - range
        end_block = problem_block + range
        
        print(f"\n=== DEBUG MODE: Scanning blocks {start_block} to {end_block} ===\n")
        
        try:
            # Get create/redeem events
            create_logs = self.w3.eth.get_logs({
                'fromBlock': start_block,
                'toBlock': end_block,
                'address': self.contract_address,
                'topics': [[
                    "0x" + self.created_topic,
                    "0x" + self.redeemed_topic
                ]]
            })
            print(f"Found {len(create_logs)} create/redeem events")
            
            # Get transfer events
            transfer_logs = self.w3.eth.get_logs({
                'fromBlock': start_block,
                'toBlock': end_block,
                'address': self.contract_address,
                'topics': ["0x" + self.transfer_topic]
            })
            print(f"Found {len(transfer_logs)} transfer events")
            
            # Show all events chronologically
            all_events = []
            
            for log in create_logs:
                event_type = "CREATE" if log['topics'][0].hex() == self.created_topic else "REDEEM"
                all_events.append((log['blockNumber'], log['logIndex'], event_type, log))
                
            for log in transfer_logs:
                all_events.append((log['blockNumber'], log['logIndex'], "TRANSFER", log))
            
            # Sort by block number and log index
            all_events.sort(key=lambda x: (x[0], x[1]))
            
            # Print all events in order
            for block_num, log_index, event_type, log in all_events:
                print(f"\nBlock {block_num}, LogIndex {log_index}: {event_type}")
                print(f"  TX Hash: {log['transactionHash'].hex()}")
                
                if event_type == "TRANSFER":
                    token_id = int(log['topics'][3].hex(), 16)
                    from_addr = '0x' + log['topics'][1].hex()[-40:]
                    to_addr = '0x' + log['topics'][2].hex()[-40:]
                    print(f"  Token ID: {token_id}")
                    print(f"  From: {from_addr}")
                    print(f"  To: {to_addr}")
                    
                    # Check if token exists in DB
                    self.cursor.execute('SELECT amount FROM nft_tokens WHERE token_id = %s', (token_id,))
                    result = self.cursor.fetchone()
                    if not result:
                        print(f"  WARNING: Token ID {token_id} not found in database")
                    else:
                        print(f"  Amount: {result[0]}")
                else:
                    # Parse create/redeem event data
                    data = log['data'].hex()
                    if not data.startswith('0x'):
                        data = '0x' + data
                    data = data[2:]
                    token_id = int(data[:64], 16)
                    holder = '0x' + data[64:128][-40:]
                    amount = int(data[128:192], 16)
                    token = '0x' + data[192:256][-40:]
                    print(f"  Token ID: {token_id}")
                    print(f"  Holder: {holder}")
                    print(f"  Amount: {amount}")
                    print(f"  Token: {token}")
                
                # Check if this event is in our database
                self.cursor.execute('''
                    SELECT event_type, transaction_hash 
                    FROM nft_locks 
                    WHERE block_number = %s AND log_index = %s
                ''', (block_num, log_index))
                db_record = self.cursor.fetchone()
                if db_record:
                    print(f"  IN DATABASE: {db_record[0]} (TX: {db_record[1]})")
                else:
                    print("  NOT IN DATABASE")
            
        except Exception as e:
            print(f"Error during debug: {e}") 
