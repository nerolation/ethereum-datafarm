import re
import os.path
import datetime
import pandas as pd
from farm.helpers.Contract import Contract
from farm.helpers.DailyResults import s3_res, s3
from botocore.exceptions import ClientError


# Returns true if there exists already data about the respective contract
def existing_aws_results(contract):
    allAWSFiles = s3_res.Bucket("ethereum-datahub")
    allAWSFiles = allAWSFiles.objects.filter(Prefix = 'contracts/{}_{}/'.format(contract.name,
                                                                                contract.method.simpleExp)).all()
    if len(list(allAWSFiles)) > 0:
            return True
    return False

# Get last safed block and set it as `fromBlock` as starting point
def restore_fromBlock_from_AWS(contract):
    print("...restoring last safed Block from AWS")
    
    # Loop over dates backwards, starting from today
    for date in (datetime.datetime.now() - datetime.timedelta(days=i) for i in range(1000)):
        fileKey = "contracts/{}_{}/csv/{}.csv".format(contract.name,
                                                    contract.method.simpleExp,
                                                    date.strftime("%d_%m_%Y"))
        
        # get last row of file and set last mined block to the contract's `fromBlock`
        try:
            df = pd.robjead_csv(s3.get_object(Bucket = "ethereum-datahub", Key = fileKey)['Body'])  
            contract.fromBlock = df.iloc[-1]['blocknumber']+1
            print("...'fromBlock' successfully loaded from AWS")
            
            # Create config file on AWS
            fK = 'config/{}/lastSafedBlock/{}_{}.txt'.format(contract.path,
                                                             contract.name,
                                                             contract.method.simpleExp)
            s3.put_object(Body=str(contract.fromBlock-1),Bucket="ethereum-datahub",Key=fK)
            print("...'fromBlock' stored on AWS\n")
            return True

            
            
        except ClientError as ex:
            if ex.response['Error']['Code'] == 'NoSuchKey':
                continue
            else:
                print(ex)
                break
        except:
            print("FAIL")
            break
    print("...nothing loaded from AWS\n")
    return False
        


#
# Create `Contract` instances that are used on the farm   
#    
def load_contracts(contracts=[], start=True, location="contracts"):
    # If first call of function => print header
    if start:
        print("Loading new contracts ...\n")
    # Create dict of contracts to check for newly appended ones
    cont={}
    for contract in contracts:
        if contract.addr in cont.keys():
            cont[contract.addr].append(contract.method.simpleExp)
        else:
            cont[contract.addr] = [contract.method.simpleExp]

    #
    # Load Contracts
    #
    # AWS storage location
    fileKey = "config/{}/contracts.csv".format(location)
    # Load file
    objB = s3.get_object(Bucket="ethereum-datahub", Key = fileKey)['Body']
    contractArray = objB.read().decode("utf-8").strip().split("\n")
    #Loop over list of contract entries
    for contract in contractArray:
        contAddr=contract.split(",")[0]
        # remove contracts whos address is set to `remove`
        if contAddr == "remove":
            for i in contracts:
                if contract.split(",")[1] == i.name:
                    print("\n ---Contract of `{}` REMOVED---\n".format(i.name))
                    del contracts[contracts.index(i)]
                    
        # if nothing change and contract remains in the farm           
        elif contAddr in cont.keys() and contract.split(",")[2].split("(")[0].lower() in cont[contAddr]:
            continue
        # new contract => Contract object initiated and provided to the farm
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
            
            # Manage `startBlock`

            filename ="config/"+location+"/lastSafedBlock/"+contracts[-1].name+"_"+ contracts[-1].method.simpleExp
            # try to load config file with startBlock from AWS
            try :
                awsfile = s3.get_object(Bucket="ethereum-datahub", Key = filename+".txt")
            except:
                awsfile = False
            # if config file => set `startBlock`
            if awsfile:
                print("loading `startBlock` from AWS config file")
                newStartBlock = s3.get_object(Bucket="ethereum-datahub", Key = filename+".txt")['Body'].read()
                newStartBlock = int(newStartBlock.decode("utf-8"))
                input("...overwritting `startBlock` to {} - please confirm".format(newStartBlock))
                contracts[-1].fromBlock = newStartBlock+1
                print("`Startblock` overwritten for {} to Block {:,}\n".format(contracts[-1].name,
                                                                               contracts[-1].fromBlock))
            # else, if already safed results in AWS => tset `startBlock` to  last safed Block   
            elif existing_aws_results(contracts[-1]):
                restore_fromBlock_from_AWS(contracts[-1])
                
            # else, take `startBlock`from contracts file
            else:
                input("fromBlock not overwritten. Please confirm")
                pass
    return contracts
