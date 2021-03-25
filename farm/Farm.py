import requests
import time
import json


from farm.helpers.classContainer import *
from farm.helpers.load_contract_helper import load_contracts
from farm.helpers.prepare_event_helper import from_hex

# Load API KEY
with open("apikey/key.txt") as k:
    KEY = str(k.read().strip())

    
class Farm:
    def __init__(self, contracts):
        self.latestBlock = self.get_latest_block()
        self.contracts = contracts
        self.lag = 24  # Block Number delay to not risk invalid blocks
        print("\nInitiating Farm Instance with {} Contract/Methods...".format(len(contracts)))
    
    
    def start_farming(self):
        global query, chunk, ab
        endless = True
        self.log_header()
        while(endless):
            self.contracts = load_contracts(self.contracts, start=False)
            endless = self.safe_end()
            self.latestBlock = self.get_latest_block()
            for i in self.contracts: 
                if self.not_wait(i):
                    query = i.query_API()
                    if i.chunksize_could_be_larger() and i.chunksizeLock == False:
                        i.increase_chunksize()
                    if query:
                        chunk = i.mine(query, i.method.id)
                        print(i.log_to_console(chunk))
                        result = i.DailyResults.enrich_daily_results_with_day_of_month(chunk)
                        i.lastScrapedBlock = result.iloc[-1][1] # last block mined
                        i.DailyResults.try_to_save_day(result, i)
                        
                else:
                    print("Waiting for {}".format(i.name))
                    self.wait(i)
    
    def get_latest_block(self):
        q = 'https://api.etherscan.io/api?module=proxy&action=eth_blockNumber&apikey={}'
        return from_hex(json.loads(requests.get(q.format(KEY)).content)['result'])
    
    # Wait if getting very close (self.lag) to the latestBlock
    def not_wait(self, contract):
        if contract.fromBlock + self.lag + contract.chunksize >= self.latestBlock:
            return False
        return True  
    
    def wait(self, contract):
        if contract.chunksize > 1000:
                contract.chunksize = round(contract.chunksize/2)
        else:
            contract.chunksize = len(self.contracts)
            contract.chunksizeLock = True   
    
    def status(self):
        string = ""
        for s in self.contracts:
            string += s.__repr__() + "\n"
        print("Farm instance initiated with the following contracts\n\n{}".format(string))
        return self
        
    def log_header(self):
        header = ("Timestamp", "Contract", "Current Chunk", "Chunk Timestamp", "Events", "Chsz", "Fc")
        log = "\033[4m{:^23}-{:^18}|{:^21}| {:^18}  |{:^6}|{:^6}|{:^6}\033[0m".format(*header)
        print(log)
    
    def safe_end(self):
        try:
            with open("config/end.txt") as endfile:
                return False if endfile.read().strip() == "True" else True
        except:
            return True
