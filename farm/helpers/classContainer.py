import sha3
import numpy as np
from datetime import datetime
import pandas as pd
import re
import requests
import time
import json
import io
import csv
import boto3
from farm.helpers.prepare_event_helper import from_hex, prepare_event

API = 'https://api.etherscan.io/api?module=logs&action=getLogs'  # API endpoint
APIQuery = '{}&fromBlock={}&toBlock={}&topic0={}&topic0_1_opr=and&address={}&apikey={}'
# Load API KEY
with open("apikey/key.txt") as k:
    KEY = str(k.read().strip())

# AWS Stuff
with open(".aws/credentials") as creds:
    reader = csv.reader(creds)
    creds = [i for i in reader]
s3_res = boto3.resource('s3', aws_access_key_id=creds[0][0], aws_secret_access_key=creds[1][0])
bucket = s3_res.Bucket("ethereum-datahub")
s3 = boto3.client('s3', aws_access_key_id=creds[0][0], aws_secret_access_key=creds[1][0])
# END AWS Stuff



def from_unix(time):
    try:
        return time.strftime("%d/%m/%Y  %H:%M:%S")
    except:
        return datetime.utcfromtimestamp(time).strftime("%d/%m/%Y %H:%M:%S")
                
def get_method_from_canonical_expression(method):
    return '0x' + sha3.keccak_256(method.encode('utf-8')).hexdigest()


class DailyResults():
    def __init__(self, name, init, deltaToEnd = 100):
        self.name = name
        self.init = init
        self.results = pd.DataFrame()
    
    # Add column with day of the month to the dataFrame
    # This is needed to split the dataFrame between daily chunks to save them separately
    def enrich_daily_results_with_day_of_month(self, chunk):
        chunk = pd.DataFrame(chunk)
        chunk["day"] = chunk.apply(lambda x: datetime.utcfromtimestamp(x[0]).strftime("%d"), axis=1)
        return chunk
        
    # Try to save the file / sync to AWS if there are two different days of month in the results
    # ...this means that got a transition from one day to another
    def try_to_save_day(self, results, contract):
        # This helps for entering recursive mode
        # When the dataFrame is split, it can happen that nothing remains but the empty dataFrame
        if results.empty == True:
            print("Empty Dataframe...")
            return False
        
        # Get day of month (ex. 04) for the first entry in the results
        if self.results.empty:
            firstEntry = results.iloc[0]["day"]
        else:
            firstEntry = self.results.iloc[0]["day"]
  
        # Get day of month (ex. 04) for the last entry in the results
        lastEntry = results.iloc[-1]["day"]
        
        # No save cause day isn't over...probably
        if firstEntry == lastEntry:
            self.results = self.results.append(results)
            return True
        
        # Different days in the results
        else:
            # First day in the dataFrame will be saved
            res = results[results['day'] == firstEntry]
            self.results = self.results.append(res)
            self.save_results(self.results, contract)
            contract.fileCounter += 1
            self.results = pd.DataFrame()
            # Second part including other days than the one save will be split and 
            # are thrown into the function again
            rest = results[results['day'] != firstEntry]
            return self.try_to_save_day(rest, contract)
            
    def save_results(self, chunk, contract):
        del chunk['day']
        filename = datetime.utcfromtimestamp(chunk.iloc[0][0]).strftime("%d_%m_%Y")
        transfer_columns = ['timestamp','blocknumber','txhash','txindex','logindex',
                            'txfrom','txto', 'txvalue', 'gas_price', 'gas_used']
        chunk.columns = transfer_columns
        
        csv_buf = io.StringIO()
        pickle_buf = io.BytesIO()
        chunk.to_csv(csv_buf, index = False)
        chunk.to_pickle(pickle_buf)
        s3.put_object(Body = csv_buf.getvalue(), 
                      Bucket = "ethereum-datahub", 
                      Key = '{}_{}/csv/{}.csv'.format(contract.name, 
                                                   contract.method.canonicalExpression.split("(")[0].lower(),
                                                   filename))
        s3.put_object(Body = pickle_buf.getvalue(), 
                      Bucket = "ethereum-datahub", 
                      Key = '{}_{}/pickle/{}.pickle'.format(contract.name, 
                                                         contract.method.canonicalExpression.split("(")[0].lower(),
                                                         filename))
        print("Saved Chunk to AWS S3 as `{}_{}/{}.(csv|pickle)`".format(contract.name, 
                                                                        contract.method.canonicalExpression.split("(")[0].lower(),
                                                                        filename))
        with open('contracts/lastSafedBlock/{}_{}.txt'.format(contract.name, contract.method.canonicalExpression.split("(")[0].lower()), 'w') as handle:
            handle.write(str(chunk.iloc[-1]['blocknumber']))

        return True

class Method:
    def __init__(self, method):
        self.canonicalExpression = method
        self.id = get_method_from_canonical_expression(method)

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
    
    # Check if the average number of results of the the last 10 request has to 
    # less elements and adjust the chunksize if applicable
    def chunksize_could_be_larger(self):
        if np.mean(self.chunksizeAdjuster) < 400 and self.chunksize < 5000:
            return True
        return False
    
    
    # Increase Chunksize
    def increase_chunksize(self):
        self.chunksize = round(self.chunksize*2) if self.chunksize < 5000 else self.chunksize
        print('... increasing chunksize for {} to {}'.format(self.name,self.chunksize))
        return
    
    def mine(self, query, methodId):
        chunk = []
        for e in query:
            toChunk = prepare_event(e, methodId)
            chunk.append(toChunk)
        return chunk
    
    
    def query_API(self):
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
            print('... decreasing chunksize for {} to {}'.format(self.name,self.chunksize))
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




