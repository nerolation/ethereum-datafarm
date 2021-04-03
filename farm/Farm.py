import requests
import time
import json
import sys
from farm.helpers.Contract import Contract
from farm.helpers.ContractHelper import load_contracts, animation
from farm.helpers.EventHelper import from_hex



#
# Farm
# A farm instance is used to take an array of contracts to loop through it and execute a contract's function
# 
class Farm:
    def __init__(self, contracts, keyPath=".apikey/key.txt", aws_bucket=None):
        # Load API KEY
        with open(keyPath) as k:
            self.KEY = str(k.read().strip())
        # Set latest block
        self.latestBlock = self.get_latest_block()
        
        
        # Contracts objs.
        self.contracts = contracts
        
        # AWS Bucket name
        self.aws_bucket = aws_bucket
        
        # Block Number delay to not risk invalid blocks
        self.lag = 24  
        print("\n")
        animation("Initiating Farm Instance with {} Contracts/Methods".format(len(contracts)))
                
    
    # Main function
    def start_farming(self):
        # Endless == True if end.txt == False => allows to safely end the program at the beginning of an iteration
        endless = True
        self.log_header()
        while(endless):
            endless = self.safe_end()
            # Update latestBlock
            self.latestBlock = self.get_latest_block()
            # Load or remove new contracts
            self.contracts = load_contracts(
                                            self.contracts, 
                                            start=False, 
                                            config_location=self.contracts[0].path, 
                                            aws_bucket=self.aws_bucket
                                           )
            
            # Loop over the list of contracts
            for i in self.contracts: 
                # If latestBlock is reached => wait
                if self.not_wait(i):
                    # API request
                    query = i.query_API(self.KEY)
                    # Try to increase the chunksize
                    if i.chunksize_could_be_larger() and i.chunksizeLock == False:
                        i.increase_chunksize()
                    if query:
                        # Prepare raw request for further processing
                        chunk = i.mine(query, i.method.id)
                        print(i.log_to_console(chunk))
                        result = i.DailyResults.enrich_daily_results_with_day_of_month(chunk)
                        
                        # Try to safe results
                        i.DailyResults.try_to_save_day(result, i, self.aws_bucket)
                        
                else:
                    print("Waiting for {}".format(i.name))
                    self.wait(i)
    
    # Get latest mined block from Etherscan
    def get_latest_block(self):
        q = 'https://api.etherscan.io/api?module=proxy&action=eth_blockNumber&apikey={}'
        try:
            return from_hex(json.loads(requests.get(q.format(self.KEY)).content)['result'])
        except:
            print("Except latest block")
            q = q.format(self.KEY)
            if "Bad Gateway" in str(q):
                print("Bad Gateway - latest Block")
                time.sleep(10)
                return self.latestBlock
            q = q.content
            q = requests.get(q)
            print(q)                
            q = json.loads(q)['result']
            lB = from_hex(q)
            return lB            
    
    # Wait if getting very close (self.lag) to the latestBlock
    def not_wait(self, contract):
        if contract.fromBlock + self.lag + contract.chunksize >= self.latestBlock:
            return False
        return True  
    
    # Wait and adapt/lock chunksize
    def wait(self, contract):
        if contract.chunksize > 1000:
                contract.chunksize = round(contract.chunksize/2)
        else:
            contract.chunksize = len(self.contracts)
            contract.chunksizeLock = True   
    
    # Print status of the current instance, including its contracts 
    def status(self):
        string = ""
        for s in self.contracts:
            string += s.__repr__() + "\n"
        print("Farm instance initiated with the following contracts\n\n{}".format(string))
        time.sleep(1)
        return self
    
    # Header of output
    def log_header(self):
        header = ("Timestamp", "Contract", "Current Chunk", "Chunk Timestamp", "Events", "Chsz", "Fc")
        log = "\033[4m{:^23}-{:^18}|{:^21}| {:^20}|{:^6}|{:^6}|{:^6}\033[0m".format(*header)
        print(log)
    
    # Check end.txt file if the program should stop
    def safe_end(self):
        try:
            with open("config/end.txt") as endfile:
                return False if endfile.read().strip() == "True" else True
        except:
            return True
