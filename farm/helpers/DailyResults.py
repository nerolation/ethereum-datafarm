from datetime import datetime, timedelta
import pandas as pd
import pandas_gbq
import io
import os 
import csv
import re
import boto3
from google.cloud import bigquery
from pandas_gbq.gbq import InvalidSchema
from farm.helpers.Logger import globalLogger as gl

# AWS Stuff
s3_res = boto3.resource('s3')
s3 = boto3.client('s3')
# END AWS Stuff

class DailyResults():
    def __init__(self, name, init):
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
    def try_to_save_day(self, results, contract, aws_bucket, useBigQuery):
        # This helps for entering recursive mode
        # When the dataFrame is split, it can happen that nothing remains but the empty dataFrame
        if results.empty == True and contract.endAtBlock == None:
            gl("Empty Dataframe...")
            return False
        
        # Get day of month (ex. 04) for the first entry in the results
        if self.results.empty:
            firstEntry = results.iloc[0]["day"]
        else:
            firstEntry = self.results.iloc[0]["day"]
  
        # Get day of month (ex. 04) for the last entry in the results
        lastEntry = results.iloc[-1]["day"]
        
        # No save cause day isn't over...except endAtBlock is set
        if firstEntry == lastEntry:
            if contract.endAtBlock != None:
                if contract.fromBlock >= contract.endAtBlock:
                    print("\n\n\n --------2222222222--------- \n\n\n")
                    self.results = self.results.append(results)
                    self.save_results(self.results, contract, aws_bucket, useBigQuery)
                    contract.fileCounter += 1
                    self.results = pd.DataFrame()
                    return True
            self.results = self.results.append(results)
            return True
        
        # Different days in the results
        else:
            # First day in the dataFrame will be saved
            res = results[results['day'] == firstEntry]
            self.results = self.results.append(res)
            self.save_results(self.results, contract, aws_bucket, useBigQuery)
            contract.fileCounter += 1
            self.results = pd.DataFrame()
            # Second part including other days than the one save will be split and 
            # are thrown into the function again
            rest = results[results['day'] != firstEntry]
            return self.try_to_save_day(rest, contract, aws_bucket, useBigQuery)
            
    def save_results(self, chunk, contract, aws_bucket, useBigQuery, sync=False):
        del chunk['day']
        filename = datetime.utcfromtimestamp(chunk.iloc[0][0]).strftime("%Y_%m_%d")
        
        chunk.columns = contract.headerColumn
        
        csv_buf = io.StringIO()
        chunk.to_csv(csv_buf, index = False)
        
        # BigQuery Upload
        if useBigQuery:
            try:
                client
            except:
                # Google Stuff
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'tokendata.json'
                client = bigquery.Client()
            table_id = '{}.{}'.format(contract.name, contract.method.simpleExp)
            ts = self.get_table_schema(contract)
            if ts:
                chunk.to_gbq(table_id, if_exists="append", chunksize=10000000, table_schema=ts)
                sync=True
            else:
                try:
                    chunk.to_gbq(table_id, if_exists="append", chunksize=10000000)
                    sync=True
                except InvalidSchema:
                    ts = self.get_table_schema(contract, None, True)
                    chunk.to_gbq(table_id, if_exists="append", chunksize=10000000, table_schema=ts)
                    sync=True
            assert(sync == True)
            gl(" -- BigQuery Sync successfull --")
        
        res=s3.put_object(Body = csv_buf.getvalue(), 
                      Bucket = aws_bucket, 
                      Key = 'contracts/{}_{}/csv/{}.csv'.format(contract.name, 
                                                                contract.method.canonicalExpression.split("(")[0].lower(),
                                                                filename))
        assert(res["ResponseMetadata"]["HTTPStatusCode"]==200)                
                
        fK = 'config/{}/lastSafedBlock/{}_{}.txt'.format("contracts",
                                                         contract.name,
                                                         contract.method.canonicalExpression.split("(")[0].lower())
        res = s3.put_object(Body=str(chunk.iloc[-1]['blocknumber']),Bucket=aws_bucket,Key=fK)
        assert(res["ResponseMetadata"]["HTTPStatusCode"]==200)
        gl(" -- AWS Sync successfull --")
        return True
    
    # This can be used to provide a fixed table schema for specific contracts or methods
    def get_table_schema(self, contract, schema=None, basicSchema=None):
        if contract.method.simpleExp.lower() == "approval":
            schema = [  {'name': 'timestamp', 'type': 'INTEGER'},
                        {'name': 'blocknumber', 'type': 'INTEGER'},
                        {'name': 'txhash', 'type': 'STRING'},
                        {'name': 'txindex', 'type': 'INTEGER'},
                        {'name': 'logindex', 'type': 'INTEGER'},
                        {'name': 'txfrom', 'type': 'STRING'},
                        {'name': 'txto', 'type': 'STRING'},
                        {'name': 'txvalue', 'type': 'STRING'},
                        {'name': 'gas_price', 'type': 'INTEGER'},
                        {'name': 'gas_used', 'type': 'INTEGER'}
                     ]

        if contract.method.simpleExp.lower() == "transfer" and contract.name == "bnb":
            schema = [ {'name': 'timestamp', 'type': 'INTEGER'},
                       {'name': 'blocknumber', 'type': 'INTEGER'},
                       {'name': 'txhash', 'type': 'STRING'},
                       {'name': 'txindex', 'type': 'INTEGER'},
                       {'name': 'logindex', 'type': 'INTEGER'},
                       {'name': 'txfrom', 'type': 'STRING'},
                       {'name': 'txto', 'type': 'STRING'},
                       {'name': 'txvalue', 'type': 'STRING'},
                       {'name': 'gas_price', 'type': 'INTEGER'},
                       {'name': 'gas_used', 'type': 'INTEGER'}
                     ]
            
        if contract.method.simpleExp.lower() == "transfer" and contract.name == "dai":
            schema = [ {'name': 'timestamp', 'type': 'INTEGER'},
                       {'name': 'blocknumber', 'type': 'INTEGER'},
                       {'name': 'txhash', 'type': 'STRING'},
                       {'name': 'txindex', 'type': 'INTEGER'},
                       {'name': 'logindex', 'type': 'INTEGER'},
                       {'name': 'txfrom', 'type': 'STRING'},
                       {'name': 'txto', 'type': 'STRING'},
                       {'name': 'txvalue', 'type': 'BIGNUMERIC'},
                       {'name': 'gas_price', 'type': 'INTEGER'},
                       {'name': 'gas_used', 'type': 'INTEGER'}
                     ]

        if contract.method.simpleExp.lower() == "swap" and contract.name == "torn":
            schema = [ {'name': 'timestamp', 'type': 'INTEGER'},
                       {'name': 'blocknumber', 'type': 'INTEGER'},
                       {'name': 'txhash', 'type': 'STRING'},
                       {'name': 'txindex', 'type': 'INTEGER'},
                       {'name': 'logindex', 'type': 'INTEGER'},
                       {'name': 'recipient', 'type': 'STRING'},
                       {'name': 'ptorn', 'type': 'INTEGER'},
                       {'name': 'torn', 'type': 'BIGNUMERIC'},
                       {'name': 'gas_price', 'type': 'INTEGER'},
                       {'name': 'gas_used', 'type': 'INTEGER'}
                     ]
        if contract.method.simpleExp.lower() == "swap" and re.search("uniswappoolv2",contract.name):
            schema = [ {'name': 'timestamp', 'type': 'INTEGER'},
                       {'name': 'blocknumber', 'type': 'INTEGER'},
                       {'name': 'txhash', 'type': 'STRING'},
                       {'name': 'txindex', 'type': 'INTEGER'},
                       {'name': 'logindex', 'type': 'INTEGER'},
                       {'name': 'sender', 'type': 'STRING'},
                       {'name': 'recipient', 'type': 'STRING'},
                       {'name': 'amount0In', 'type': 'BIGNUMERIC'},
                       {'name': 'amount1In', 'type': 'BIGNUMERIC'},
                       {'name': 'amount0Out', 'type': 'BIGNUMERIC'},
                       {'name': 'amount1Out', 'type': 'BIGNUMERIC'},
                       {'name': 'gas_price', 'type': 'INTEGER'},
                       {'name': 'gas_used', 'type': 'INTEGER'}
                     ]
            
        if contract.method.simpleExp.lower() == "swap" and re.search("uniswappoolv3",contract.name):
            schema = [ {'name': 'timestamp', 'type': 'INTEGER'},
                       {'name': 'blocknumber', 'type': 'INTEGER'},
                       {'name': 'txhash', 'type': 'STRING'},
                       {'name': 'txindex', 'type': 'INTEGER'},
                       {'name': 'logindex', 'type': 'INTEGER'},
                       {'name': 'sender', 'type': 'STRING'},
                       {'name': 'recipient', 'type': 'STRING'},
                       {'name': 'amount0', 'type': 'STRING'},
                       {'name': 'amount1', 'type': 'STRING'},
                       {'name': 'sqrtPriceX96', 'type': 'STRING'},
                       {'name': 'liquidity', 'type': 'BIGNUMERIC'},
                       {'name': 'tick', 'type': 'INTEGER'},
                       {'name': 'gas_price', 'type': 'INTEGER'},
                       {'name': 'gas_used', 'type': 'INTEGER'}
                     ]
            
        if basicSchema != None:
            schema = [ {'name': 'timestamp', 'type': 'INTEGER'},
                       {'name': 'blocknumber', 'type': 'INTEGER'},
                       {'name': 'txhash', 'type': 'STRING'},
                       {'name': 'txindex', 'type': 'INTEGER'},
                       {'name': 'logindex', 'type': 'INTEGER'},
                       {'name': 'txfrom', 'type': 'STRING'},
                       {'name': 'txto', 'type': 'STRING'},
                       {'name': 'txvalue', 'type': 'STRING'},
                       {'name': 'gas_price', 'type': 'INTEGER'},
                       {'name': 'gas_used', 'type': 'INTEGER'}
                     ]
        return schema
                



