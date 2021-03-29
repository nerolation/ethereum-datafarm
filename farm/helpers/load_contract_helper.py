import re
import os.path
import datetime
import pandas as pd
from farm.helpers.Contract import Contract
from farm.helpers.DailyResults import s3_res, s3
from botocore.exceptions import ClientError


def restore_fromBlock_from_AWS(contract):
    for date in (datetime.datetime.now() - datetime.timedelta(days=i) for i in range(1000)):
        fileKey = "{}_{}/csv/{}.csv".format(contract.name,
                                            contract.method.canonicalExpression.split("(")[0].lower(),
                                            date.strftime("%d_%m_%Y"))
        try:
            obj = s3.get_object(Bucket = "ethereum-datahub", Key = fileKey)
            objB = obj['Body']
            df = pd.read_csv(objB)   
            contract.fromBlock = df.iloc[-1]['blocknumber']+1
            print("'fromBlock' successfully loaded from AWS...")
            if not os.path.exists('{}/lastSafedBlock/'.format(contract.path)):
                os.mkdir('{}/lastSafedBlock/'.format(contract.path))
            with open('{}/lastSafedBlock/{}_{}.txt'.format(contract.path, 
                                                           contract.name, 
                                                           contract.method.canonicalExpression.split("(")[0].lower()), 'w') as handle:
                handle.write(str(contract.fromBlock))
            return
            
        except ClientError as ex:
            if ex.response['Error']['Code'] == 'NoSuchKey':
                continue
            else:
                print(ex)
                break
        except:
            print("FAIL")
            break

        



    
    
def load_contracts(contracts=[], start=True, location="contracts"):
    if start:
        print("Loading new contracts ...\n")
    cont=[]
    for contract in contracts:
        cont.append(contract.addr)

    cont = "".join(cont)
    with open(location+"/contracts.csv") as c:
        for contract in [x.strip() for x in c]:
            if contract.split(",")[0] == "remove":
                for i in contracts:
                    if contract.split(",")[1] == i.name:
                        print("\n ---Contract of `{}` REMOVED---\n".format(i.name))
                        del contracts[contracts.index(i)]

            elif contract.split(",")[0] in cont:
                continue
            else:
                contracts.append(Contract(*tuple(re.split("\,(?=.*\()|\,(?!.*\))", contract))))
                contracts[-1].path = location
                if start:
                    print("Contract loaded @  {}".format(contracts[-1].addr))
                    print("  |--- Name        {}".format(contracts[-1].name))
                    print("  |--- Method      {}".format(contracts[-1].method.canonicalExpression))
                    print("  |--- Method ID   {}".format(contracts[-1].method.id))
                    print("  |--- StartBlock  {:,}".format(int(contracts[-1].startBlock)))
                    print("  |--- Chunksize   {:,}\n".format(int(contracts[-1].chunksize)))
                    

                try:
                    filename =location+"/lastSafedBlock/"+contracts[-1].name+"_"+contracts[-1].method.canonicalExpression.split("(")[0].lower()
                    if os.path.exists('{}.txt'.format(filename)):
                        with open('{}.txt'.format(filename), 'r') as loadBlock:
                            contracts[-1].fromBlock = int(loadBlock.read())+1
                            print("`Startblock` overwritten for {} to Block {:,}\n".format(contracts[-1].name,
                                                                                           contracts[-1].fromBlock))
                    else:
                        restore_fromBlock_from_AWS(contracts[-1])
                except:
                    input("fromBlock not overwritten. Please confirm")
                    pass
    return contracts
