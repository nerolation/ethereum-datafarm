import numpy as np
from datetime import datetime
import pandas as pd
import re
import requests
import time
import json
import csv
from json.decoder import JSONDecodeError

from farm.helpers.prepare_event_helper import from_hex, prepare_event
from farm.helpers.DailyResults import DailyResults
from farm.helpers.Method import Method

# Etherscan API endpoint
API = 'https://api.etherscan.io/api?module=logs&action=getLogs' 
# Query for the API call, specifying topic0 (Method ID) and topic0_1 (Contract Address)
APIQuery = '{}&fromBlock={}&toBlock={}&topic0={}&topic0_1_opr=and&address={}&apikey={}'

def from_unix(time):
    try:
        return time.strftime("%d/%m/%Y  %H:%M:%S")
    except:
        return datetime.utcfromtimestamp(time).strftime("%d/%m/%Y %H:%M:%S")

#
# Contract Object
#
# Contracts are loaded into an array which is then passed to the Farm object. A Contract 
#   consists consists of an address, a name and a method, which are the parameters used
#   to differentiate contracts. This opens up is the possibility to mine data from multiple
#   events from  the same contract
class Contract:
    def __init__(self, addr, name, method, startblock, chunksize = 2000):
        self.addr = addr # Contract address
        self.name = name.lower() # Lowercase name
        self.method = Method(method) # Contract Method
        self.startBlock = startblock # Genesis Block of Contract
        self.chunksize = int(chunksize) # Chunksize to query API
        self.chunksizeLock = False # Gets locks when the latest mined block is reached
        self.fromBlock = int(self.startBlock) # Query API "from" Block
        self.chunksizeAdjuster = np.array([400*1.3]*10) # list of len 10 as indicator to incr chunzsize
        self.fileCounter = 0 # Files safed since start of the Farm
        self.DailyResults = DailyResults(self.name, datetime.now()) # DailyResults obj.
        self.path = None # Path to the contract.csv file
    
    # Check if the average number of results of the the last 10 request has to 
    # less elements and adjust the chunksize if applicable
    def chunksize_could_be_larger(self):
        if np.mean(self.chunksizeAdjuster) < 400 and self.chunksize < 10000:
            return True
        return False
    
    
    # Increase Chunksize
    def increase_chunksize(self):
        self.chunksize = round(self.chunksize*2) if self.chunksize < 10000 else self.chunksize
        #print('... increasing chunksize for {} to {}'.format(self.name,self.chunksize))
        return
    
    # Prepare raw request data to be safed as csv
    def mine(self, query, methodId):
        chunk = []
        for e in query:
            toChunk = prepare_event(e, methodId)
            chunk.append(toChunk)
        return chunk
    
    # Execute query request
    def query_API(self, KEY):
        res = None
        # Create the actual API Request
        queryString = APIQuery.format(API, 
                                       self.fromBlock, 
                                       int(self.fromBlock)+int(self.chunksize), 
                                       self.method.id, 
                                       self.addr, 
                                       KEY)
        
        
        # Submit Request
        try:
            res = json.loads(requests.get(queryString).content) 
        except JSONDecodeError:
            print(requests.get(queryString).content)
            print("Some strange JSONDecodeError")
            return None
        except:
            print("Some other strange error")
            return None
        
        # Catch fails
        # If nothing is found, then the requestes blockrange didn't contain any relevant events
        if res['message'] == 'No records found':
            self.fromBlock += self.chunksize + 1
            self.chunksizeAdjuster = np.append(self.chunksizeAdjuster,[0])[-10:]
            return None
        
        # If API endpoint blocks request, then wait and try again next iteration (within contract array in farm)
        if (res['status'] == '0' or not res):
            print('... request failed for {}'.format(self.addr))
            time.sleep(10)
            return
        
        # Check if len of returned results is the maximum of 1000
        # If so, enter recursive mode with a smaller chunksize - try again
        if (len(res['result']) >= 1000): #Request to large
            self.chunksize -= round(self.chunksize / 3)
            #print('... decreasing chunksize for {} to {}'.format(self.name,self.chunksize))
            return self.query_API(KEY) # Recursive bby
        
        # Add len of result to chunksizeAdjuster list and remove first element
        self.chunksizeAdjuster = np.append(self.chunksizeAdjuster,[len(res['result'])])[-10:]
        
        #Set new fromBlock for the next round
        self.fromBlock += self.chunksize + 1
        
        return res['result']
    
    
    # Continous logger
    def log_to_console(self, res):
        ts = from_unix(datetime.now())
        log = "{:^23}-{:^18}|{:^10}-{:^10}| {:^18} |{:^6}|{:^6}|{:^6}".format(
                                                                              ts, # Timestamp
                                                                              self.name, # Contract Name
                                                                              res[0][1], # First block of result
                                                                              res[-1][1],  # Last block of result
                                                                              from_unix(res[-1][0]), # ts of last block in res
                                                                              len(res), # Len of result
                                                                              self.chunksize, # Contract's chunksize
                                                                              self.fileCounter # Contract's fileCounter
                                                                             )
        return log

    
    def __repr__(self):
        method = re.search("(.*)\(.*", self.method.canonicalExpression).group(1)
        return "Contract @ {} » Method: '{}' » Startblock: {:,}".format(self.addr, method, self.fromBlock)




