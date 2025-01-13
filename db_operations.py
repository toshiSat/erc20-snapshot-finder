import psycopg2
from config import DB_CONFIG

class DatabaseOperations:
    def __init__(self, reset=False):
        # Connect to PostgreSQL database
        self.db_conn = psycopg2.connect(**DB_CONFIG)
        self.cursor = self.db_conn.cursor()
        
        if reset:
            print("Resetting database...")
            self.cursor.execute('''
                DROP INDEX IF EXISTS idx_to_address;
                DROP INDEX IF EXISTS idx_from_address;
                TRUNCATE TABLE erc20_transfers;
            ''')
            self.db_conn.commit()
            print("Database reset complete")
            
        self.setup_database()

    def setup_database(self):
        # Create table for transfer events
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS erc20_transfers (
                            token_address TEXT,
                            block_number BIGINT,
                            transaction_hash TEXT,
                            log_index BIGINT,
                            from_address TEXT,
                            to_address TEXT,
                            value NUMERIC,
                            UNIQUE(token_address, block_number, log_index)
                          );''')
        
        # Create indexes
        self.cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_token_address ON erc20_transfers(token_address);
            CREATE INDEX IF NOT EXISTS idx_to_address ON erc20_transfers(to_address);
            CREATE INDEX IF NOT EXISTS idx_from_address ON erc20_transfers(from_address);
        ''')
        self.db_conn.commit()

    def store_transfers(self, batch_values):
        self.cursor.executemany('''
            INSERT INTO erc20_transfers 
            (token_address, block_number, transaction_hash, log_index, from_address, to_address, value)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (token_address, block_number, log_index) 
            DO NOTHING
        ''', batch_values)
        self.db_conn.commit()
        return len(batch_values)

    def get_last_processed_block(self, start_block, reset=False):
        if reset:
            return start_block
            
        self.cursor.execute('SELECT MAX(block_number) FROM erc20_transfers')
        last_block = self.cursor.fetchone()[0]
        return last_block if last_block is not None else start_block

    def generate_snapshot(self, token_name, token_address):
        print(f"Calculating balances for {token_name} ({token_address}) and generating CSV...")
        self.cursor.execute('''
            -- Create temporary table with balances
            CREATE TEMP TABLE address_balances AS
            SELECT 
                address,
                SUM(balance_change) as balance
            FROM (
                SELECT to_address as address, value as balance_change 
                FROM erc20_transfers 
                WHERE token_address = %s
                UNION ALL
                SELECT from_address as address, -value as balance_change 
                FROM erc20_transfers 
                WHERE token_address = %s
            ) balance_changes
            GROUP BY address
            HAVING SUM(balance_change) > 0;

            -- Select from temporary table
            SELECT address, balance 
            FROM address_balances 
            ORDER BY balance DESC;
        ''', (token_address, token_address))

        filename = f'snapshot_{token_name}.csv'
        print(f"Writing to {filename}...")
        with open(filename, 'w') as f:
            f.write('address,balance\n')
            batch_size = 1000
            records_written = 0
            
            while True:
                records = self.cursor.fetchmany(batch_size)
                if not records:
                    break
                    
                for record in records:
                    f.write(f'{record[0]},{record[1]}\n')
                
                records_written += len(records)
                print(f"Wrote {records_written:,} records to CSV")

    def close(self):
        self.cursor.close()
        self.db_conn.close() 