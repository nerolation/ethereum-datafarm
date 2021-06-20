import requests
import time
import json
import glob
import re
from farm.helpers.Contract import Contract
from farm.helpers.ContractLoader import load_contracts
from farm.helpers.EventHelper import from_hex
from farm.helpers.DailyResults import s3_res, datetime, timedelta
from farm.helpers.Logger import globalLogger as gl 

#
# Farm
# A farm instance is used to take an array of contracts to loop through it and execute a contract's function
# 
class Farm:
    def __init__(self, 
                 contracts, 
                 keyPath=".apikey/key.txt", 
                 logging=True,
                 aws_bucket=None, 
                 useBigQuery=False, 
                 canSwitch=False,
                 secureSwitch=True
                ):
        self.contracts = contracts                 # Contracts objs.
        self.contract_length = len(contracts)      # Number of contracts
        self.waitingMonitor = 0                    # Helper to slow down scraping
        with open(keyPath) as k:                   # Load API KEY
            self.KEY = str(k.read().strip())
        self.latestBlock = self.get_latest_block() # Set latest block
        self.aws_bucket = aws_bucket               # AWS Bucket name
       
        self.lag = 4000                            # Block delay to not risk invalid blocks
        self.useBigQuery = useBigQuery             # BigQuery Upload
        self.canSwitch = canSwitch                 # Specify if the contracts.csv file can be switched
        self.secureSwitch = secureSwitch           # no confirmation needed after config-file switch
        self.currentContractPath = self.contracts[0].path
        gl("\nInitiating Farm Instance with {} Contracts/Methods".format(len(contracts)), animated=True)
                
    
    # Main function
    def start_farming(self):
        # Endless == True if end.txt == False => allows to safely end the program at the beginning of an iteration
        endless = True
        self.log_header()
        while(endless):
            endless = self.safe_end()
            # Slow down program if the latest block is reached for every token
            self.adjust_speed()
            # Update latestBlock
            self.latestBlock = self.get_latest_block()
            
            if self.canSwitch:
                print("START SWITCH")
                gl("Monitor Count: {}\nContracts: {}".format(self.waitingMonitor,
                                                            self.contract_length 
                                                           ))
                gl("Switch contract.csv config file", animated=True)
                self.currentContractPath = self.get_next_file()
                self.contracts=[]
                gl("File switched", animated=True)
                gl("Waiting until {} to proceed".format(self.get_future_startTime()), animated=True)
                time.sleep(86400/2)
                start=True
                self.secureSwitch=False
                self.waitingMonitor=0 # Reset
                self.canSwitch=False  # Reset
            else:
                self.currentContractPath = self.contracts[0].path
                start=False
            # Load or remove new contracts
            self.contracts = load_contracts(
                                            self.contracts, 
                                            start, 
                                            config_location=self.currentContractPath, 
                                            aws_bucket=self.aws_bucket,
                                            secureStart=self.secureSwitch
                                           )
            if start:
                self.contract_length=len(self.contracts)
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
                        chunk = i.mine(query, i.method.id, self.KEY)
                        gl(i.log_to_console(chunk))
                        result = i.DailyResults.enrich_daily_results_with_day_of_month(chunk)
                        
                        # Try to safe results
                        i.DailyResults.try_to_save_day(result, i, self.aws_bucket, self.useBigQuery)
                    else:
                        gl("No records for {} with method {}".format(i.name, i.method.simpleExp))
                        
                else:
                    gl("Waiting for {} with method {}".format(i.name, i.method.simpleExp))
                    if i.shouldWait == False:
                        i.shouldWait = True
                        self.waitingMonitor += 1
                        gl("Switch activated: {}".format((str(self.canSwitch))
                        gl("Monitor Count: {}\nContracts: {}".format(self.waitingMonitor, self.contract_length))
                    self.wait(i)
                      
    def get_future_startTime(self):
        return datetime.strftime(datetime.now()+timedelta(hours=12), "%H:%M:%S")
        
                      
    
    # Wait some time if every contract reached the latest block or try to switch file
    def adjust_speed(self):
        if self.contract_length == self.waitingMonitor and self.waitingMonitor != 0:
            self.try_activate_contract_change()
            time.sleep(10)
    
    # Activate the looping over the contracts.csv files
    def try_activate_contract_change(self):
        contractPaths = self.get_config_files()
        if len(contractPaths) > 1:
            self.canSwitch = True
    
    # Get next contracts.csv configuration file
    def get_next_file(self):
        contractPaths = self.get_config_files()
        currentIndex = contractPaths.index(self.currentContractPath)
        try:
            return contractPaths[currentIndex+1]
        except:
            return contractPaths[0]
     
    
    def get_config_files(self):   
        allAWSFiles = s3_res.Bucket("ethereum-datahub")
        allAWSFiles0 = allAWSFiles.objects.filter(Prefix = 'config/contracts/contracts.csv').all()
        allAWSFiles1 = allAWSFiles.objects.filter(Prefix = 'config/contracts1/contracts.csv').all()
        allAWSFiles2 = allAWSFiles.objects.filter(Prefix = 'config/contracts2/contracts.csv').all()
        allAWSFiles3 = allAWSFiles.objects.filter(Prefix = 'config/contracts3/contracts.csv').all()
        allAWSFiles4 = allAWSFiles.objects.filter(Prefix = 'config/contracts4/contracts.csv').all()
        try:
            c0 = [i.key for i in allAWSFiles0][0].split("/")[-2]
        except:
            c0 = None
        try:
            c1 = [i.key for i in allAWSFiles1][0].split("/")[-2]
        except:
            c1 = None
        try:
            c2 = [i.key for i in allAWSFiles2][0].split("/")[-2]
        except:
            c2 = None
        try:
            c3 = [i.key for i in allAWSFiles3][0].split("/")[-2]
        except:
            c3 = None
        try:
            c4 = [i.key for i in allAWSFiles4][0].split("/")[-2]
        except:
            c4 = None
        c = []
        for i in [c0,c1,c2,c3,c4]:
            if i != None:
                c.append(i)
        return c

    # Get latest mined block from Etherscan
    def get_latest_block(self):
        q = 'https://api.etherscan.io/api?module=proxy&action=eth_blockNumber&apikey={}'
        try:
            return from_hex(json.loads(requests.get(q.format(self.KEY)).content)['result'])
        except:
            gl("Something failed, while getting the latest block:")
            q = q.format(self.KEY)
            if "Bad Gateway" in str(q):
                gl("Bad Gateway - latest Block")
                time.sleep(10)
                return self.latestBlock
            gl(q)
            try:
                q = q.content
                q = requests.get(q)
                gl(q)                
                q = json.loads(q)['result']
                lB = from_hex(q)
            except:
                lB = self.latestBlock
            time.sleep(10)
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
        # If every contract reached the latest mined block then wait

    
    # Print status of the current instance, including its contracts 
    def status(self):
        string = ""
        for s in self.contracts:
            string += s.__repr__() + "\n"
        gl("Farm instance initiated with the following contracts\n\n{}".format(string))
        time.sleep(1)
        return self
    
    # Header of output
    def log_header(self):
        header = ("Timestamp", "Contract", "Current Chunk", "Chunk Timestamp", "Events", "Chsz", "Fc")
        log = "\033[4m{:^23}-{:^18}|{:^21}| {:^20}|{:^6}|{:^6}|{:^6}\033[0m".format(*header)
        gl(log)
    
    # Check end.txt file if the program should stop
    def safe_end(self):
        try:
            with open("config/end.txt") as endfile:
                run = False if endfile.read().strip() == "True" else True
                if not run:
                    gl("Termination initiated through `config/end.txt` file")
                return run
        except:
            return True
