from thor_stake_scanner import ThorStakeScanner
import argparse

# rFOX staking contract address
CONTRACT_ADDRESS = '0xaC2a4fD70BCD8Bab0662960455c363735f0e2b56'

# Block range for scanning
START_BLOCK = 222913582  # Contract deployment block
END_BLOCK = 290686137    # Adjust as needed, or use w3.eth.block_number for latest

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Scan rFOX staking contract events')
    parser.add_argument('--reset', action='store_true', help='Reset the database before scanning')
    args = parser.parse_args()

    try:
        # Initialize scanner with reset parameter
        scanner = ThorStakeScanner(CONTRACT_ADDRESS, reset=args.reset)
        
        # Scan blocks
        scanner.scan_blocks(START_BLOCK, END_BLOCK)
        
        # Generate balance snapshot
        scanner.generate_balances('rfox_stakes.csv')
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        scanner.close()

if __name__ == "__main__":
    main()
