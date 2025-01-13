from nft_lock_scanner import NFTLockScanner
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--reset', action='store_true', help='Reset the database before starting')
    parser.add_argument('--contract', required=True, help='NFT Lock contract address')
    parser.add_argument('--token', required=True, help='Token address to track')
    parser.add_argument('--start', type=int, required=True, help='Start block')
    parser.add_argument('--end', type=int, required=True, help='End block')
    args = parser.parse_args()

    scanner = NFTLockScanner(args.contract, args.token, reset=args.reset)
    try:
        scanner.scan_blocks(args.start, args.end)
        scanner.generate_balances()
    finally:
        scanner.close()

if __name__ == "__main__":
    main()