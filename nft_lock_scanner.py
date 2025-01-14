def setup_database(self):
    self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS nft_locks (
            block_number BIGINT,
            transaction_hash TEXT,
            log_index BIGINT,
            holder_address TEXT,
            amount NUMERIC,
            token_address TEXT,
            unlock_date NUMERIC,
            is_staked BOOLEAN,
            UNIQUE(block_number, log_index)
        );
        
        CREATE INDEX IF NOT EXISTS idx_holder ON nft_locks(holder_address);
        CREATE INDEX IF NOT EXISTS idx_token ON nft_locks(token_address);
    ''')
    self.db_conn.commit() 

def generate_balances(self, output_file='nft_locks.csv'):
    print("Calculating balances and generating CSV...")
    self.cursor.execute('''
        SELECT 
            holder_address,
            token_address,
            SUM(amount) as locked_amount
        FROM nft_locks
        GROUP BY holder_address, token_address
        HAVING SUM(amount) > 0
        ORDER BY token_address, locked_amount DESC;
    ''')
    
    total_balance = 0
    records = self.cursor.fetchall()
    print(f"Found {len(records)} holders with positive balances")
    
    print(f"Writing to {output_file}...")
    with open(output_file, 'w') as f:
        f.write('holder,token,locked_amount\n')
        for record in records:
            f.write(f'{record[0]},{record[1]},{record[2]}\n')
            total_balance += record[2]
    
    print(f"Total locked amount: {total_balance}") 