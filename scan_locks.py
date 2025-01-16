from nft_lock_scanner import NFTLockScanner
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--reset', action='store_true', help='Reset the database before starting')
    parser.add_argument('--contract', required=True, help='NFT Lock contract address')
    parser.add_argument('--token', required=True, help='Token address to track')
    parser.add_argument('--start', type=int, required=True, help='Start block')
    parser.add_argument('--end', type=int, required=True, help='End block')
    parser.add_argument('--debug', type=int, help='Debug a specific block number')
    parser.add_argument('--range', type=int, default=1, help='Number of blocks before/after for debug (default: 1)')
    args = parser.parse_args()

    scanner = NFTLockScanner(args.contract, args.token, reset=args.reset)
    try:
        scanner.scan_blocks(args.start, args.end)
        scanner.generate_balances()
        if args.debug:
            scanner.debug_blocks(args.debug, args.range)
            scanner.close()
            return
    finally:
        scanner.close()

if __name__ == "__main__":
    main()