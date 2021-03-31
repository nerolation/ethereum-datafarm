from datetime import datetime
import pandas as pd
import io
import csv
import boto3


# AWS Stuff
with open(".aws/credentials") as creds:
    reader = csv.reader(creds)
    creds = [i for i in reader]
s3_res = boto3.resource('s3', aws_access_key_id=creds[0][0], aws_secret_access_key=creds[1][0])
s3 = boto3.client('s3', aws_access_key_id=creds[0][0], aws_secret_access_key=creds[1][0])
# END AWS Stuff



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
    def try_to_save_day(self, results, contract, aws_bucket):
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
            self.save_results(self.results, contract, aws_bucket)
            contract.fileCounter += 1
            self.results = pd.DataFrame()
            # Second part including other days than the one save will be split and 
            # are thrown into the function again
            rest = results[results['day'] != firstEntry]
            return self.try_to_save_day(rest, contract, aws_bucket)
            
    def save_results(self, chunk, contract, aws_bucket):
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
                      Bucket = aws_bucket, 
                      Key = 'contracts/{}_{}/csv/{}.csv'.format(contract.name, 
                                                                contract.method.canonicalExpression.split("(")[0].lower(),
                                                                filename))
        s3.put_object(Body = pickle_buf.getvalue(), 
                      Bucket = aws_bucket, 
                      Key = 'contracts/{}_{}/pickle/{}.pickle'.format(contract.name, 
                                                                      contract.method.canonicalExpression.split("(")[0].lower(),
                                                                      filename))
        # print("Saved Chunk to AWS S3 as `{}_{}/{}.(csv|pickle)`".format(contract.name, 
                                                                        # contract.method.canonicalExpression.split("(")[0].lower(),
                                                                      #  filename))
                
                
        fK = 'config/{}/lastSafedBlock/{}_{}.txt'.format(contract.path,
                                                         contract.name,
                                                         contract.method.canonicalExpression.split("(")[0].lower())
        s3.put_object(Body=str(chunk.iloc[-1]['blocknumber']),Bucket=aws_bucket,Key=fK)
      

        return True

