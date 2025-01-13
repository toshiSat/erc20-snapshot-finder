from web3 import Web3
from config import RPC_URL

class Web3Operations:
    def __init__(self, token_address):
        # Connect to Ethereum node
        self.w3 = Web3(Web3.HTTPProvider(RPC_URL))
        self.contract_address = Web3.to_checksum_address(token_address)
        self.transfer_topic = self.w3.keccak(text="Transfer(address,address,uint256)").hex()
        print(f"Checksummed address: {self.contract_address}")

    def get_transfer_events(self, start_block, end_block):
        logs = self.w3.eth.get_logs({
            'fromBlock': start_block,
            'toBlock': end_block,
            'address': self.contract_address,
            'topics': [self.transfer_topic]
        })

        batch_values = []
        for log in logs:
            from_address = '0x' + log['topics'][1].hex()[-40:]
            to_address = '0x' + log['topics'][2].hex()[-40:]
            value = int.from_bytes(log['data'], byteorder='big')
            
            from_address = Web3.to_checksum_address(from_address)
            to_address = Web3.to_checksum_address(to_address)
            
            batch_values.append((
                log['blockNumber'],
                log['transactionHash'].hex(),
                log['logIndex'],
                from_address,
                to_address,
                value
            ))

        return batch_values 