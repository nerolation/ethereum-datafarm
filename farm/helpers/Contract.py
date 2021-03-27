import numpy as np
from datetime import datetime
import pandas as pd
import re
import requests
import time
import json
import csv
from farm.helpers.prepare_event_helper import from_hex, prepare_event
from farm.helpers.DailyResults import DailyResults
from farm.helpers.Method import Method

API = 'https://api.etherscan.io/api?module=logs&action=getLogs'  # API endpoint
APIQuery = '{}&fromBlock={}&toBlock={}&topic0={}&topic0_1_opr=and&address={}&apikey={}'

def from_unix(time):
    try:
        return time.strftime("%d/%m/%Y  %H:%M:%S")
    except:
        return datetime.utcfromtimestamp(time).strftime("%d/%m/%Y %H:%M:%S")

class Contract:
    def __init__(self, addr, name, method, startblock, chunksize = 2000):
        self.addr = addr
        self.name = name
        self.method = Method(method)
        self.startBlock = startblock
        self.chunksize = int(chunksize)
        self.chunksizeLock = False
        self.fromBlock = int(self.startBlock)
        self.chunksizeAdjuster = np.array([400*1.3]*10)
        self.fileCounter = 0
        self.DailyResults = DailyResults(self.name, datetime.now())
        self.lastScrapedBlock = 0
        self.path = None
    
    # Check if the average number of results of the the last 10 request has to 
    # less elements and adjust the chunksize if applicable
    def chunksize_could_be_larger(self):
        if np.mean(self.chunksizeAdjuster) < 400 and self.chunksize < 5000:
            return True
        return False
    
    
    # Increase Chunksize
    def increase_chunksize(self):
        self.chunksize = round(self.chunksize*2) if self.chunksize < 5000 else self.chunksize
        #print('... increasing chunksize for {} to {}'.format(self.name,self.chunksize))
        return
    
    def mine(self, query, methodId):
        chunk = []
        for e in query:
            toChunk = prepare_event(e, methodId)
            chunk.append(toChunk)
        return chunk
    
    
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
        res = json.loads(requests.get(queryString).content) 
        
        # Catch fails
        
        if res['message'] == 'No records found':
            self.fromBlock += self.chunksize + 1
            self.chunksizeAdjuster = np.append(self.chunksizeAdjuster,[0])[-10:]
            return None
        
        if (res['status'] == '0' or not res):
            print('... request failed for {}'.format(self.addr))
            time.sleep(10)
            return
        
        # Check if len of returned results is the maximum of 1000
        # If so, enter recursive mode with a smaller chunksize - try again
        if (len(res['result']) >= 1000): #Request to large
            self.chunksize -= round(self.chunksize / 3)
            #print('... decreasing chunksize for {} to {}'.format(self.name,self.chunksize))
            return self.query_API() # Recursive bby
        
        self.chunksizeAdjuster = np.append(self.chunksizeAdjuster,[len(res['result'])])[-10:]
        self.fromBlock += self.chunksize + 1
        
        return res['result']
    
    
    def log_to_console(self, res):
        ts = from_unix(datetime.now())
        log = "{:^23}-{:^18}|{:^10}-{:^10}| {:^18} |{:^6}|{:^6}|{:^6}".format(
                                                                              ts, 
                                                                              self.name,
                                                                              res[0][1],
                                                                              res[-1][1], 
                                                                              from_unix(res[-1][0]), 
                                                                              len(res), 
                                                                              self.chunksize, 
                                                                              self.fileCounter
                                                                             )
        return log

    
    def __repr__(self):
        method = re.search("(.*)\(.*", self.method.canonicalExpression).group(1)
        return "Contract @ {} » Method: '{}' » Startblock: {:,}".format(self.addr, method, self.fromBlock)




