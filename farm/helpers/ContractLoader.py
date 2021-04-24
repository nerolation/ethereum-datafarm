import re
import datetime
import pandas as pd
import sys
import time
from farm.helpers.Contract import Contract
from farm.helpers.DailyResults import s3_res, s3
from botocore.exceptions import ClientError


def animation(string=None):
    if string:
        sys.stdout.write(string)
        sys.stdout.flush()
    sys.stdout.write(".")
    sys.stdout.flush()
    time.sleep(0.8)
    sys.stdout.write(".")
    sys.stdout.flush()
    time.sleep(0.8)
    sys.stdout.write(".")
    sys.stdout.flush()
    time.sleep(1)
    print("\n")

# Returns true if there exists already data about the respective contract
def existing_aws_results(contract, aws_bucket=None):
    allAWSFiles = s3_res.Bucket(aws_bucket)
    allAWSFiles = allAWSFiles.objects.filter(Prefix = 'contracts/{}_{}/'.format(contract.name,
                                                                                contract.method.simpleExp)).all()
    if len(list(allAWSFiles)) > 0:
            return True
    return False

# Get last safed block and set it as `fromBlock` as starting point
def restore_fromBlock_from_AWS(contract, aws_bucket=None):
    animation("Restoring last safed Block from AWS")
    
    # Loop over dates backwards, starting from today
    for date in (datetime.datetime.now() - datetime.timedelta(days=i) for i in range(1000)):
        fileKey = "contracts/{}_{}/csv/{}.csv".format(contract.name,
                                                    contract.method.simpleExp,
                                                    date.strftime("%d_%m_%Y"))
        
        # get last row of file and set last mined block to the contract's `fromBlock`
        try:
            df = pd.read_csv(s3.get_object(Bucket=aws_bucket, Key=fileKey)['Body'])  
            contract.fromBlock = df.iloc[-1]['blocknumber']+1
            print("'FromBlock' successfully loaded from AWS")
            if secureStart == True:
                ip = input("Overwritting `startBlock` for {} to {} - please verify (y/n)".format(contract.name, contract.fromBlock))
            assert(ip != "n")

            # Create config file on AWS
            fK = 'config/{}/lastSafedBlock/{}_{}.txt'.format("contracts",
                                                             contract.name,
                                                             contract.method.simpleExp)
            s3.put_object(Body=str(contract.fromBlock-1),Bucket=aws_bucket,Key=fK)
            print("FromBlock' stored on AWS\n")
            time.sleep(1)
            return True

        except ClientError as ex:
            if ex.response['Error']['Code'] == 'NoSuchKey':
                continue
            else:
                print(ex)
                break
        except:
            continue
    print("--- Nothing loaded from AWS ---\n")
    return False
        

#
# Create `Contract` instances that are used in the farm   
#    
def load_contracts(contracts=[],start=True,config_location="contracts",aws_bucket=None,secureStart=True):
    # If first call of function => print header
    if start:
        animation("Loading new contracts")
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
    fileKey = "config/{}/contracts.csv".format(config_location)
    # Load file
    objB = s3.get_object(Bucket=aws_bucket, Key = fileKey)['Body']
    contractArray = objB.read().decode("utf-8").strip().split("\n")
    #Loop over list of contract entries
    for contract_string in contractArray:
        contAddr=contract_string.split(",")[0]
        # remove contracts whos address is set to `remove`
        if contAddr == "remove":
            for i in contracts:
                if i.name == contract_string.split(",")[1] and contract_string.split(",")[2].split("(")[0].lower() == i.method.simpleExp:
                    print("\n ---Contract of `{}` with method {} removed---\n".format(i.name, i.method.simpleExp))
                    del contracts[contracts.index(i)]
                    
        # if nothing change and contract remains in the farm           
        elif contAddr in cont.keys() and contract_string.split(",")[2].split("(")[0].lower() in cont[contAddr]:
            continue
        # new contract => Contract object initiated and provided to the farm
        else:
            contracts.append(Contract(*tuple(re.split("\,(?=.*\()|\,(?!.*\))", contract_string))))
            contracts[-1].path = config_location
            if start:
                print("Contract loaded @  {}".format(contracts[-1].addr))
                print("  |--- Name        {}".format(contracts[-1].name))
                print("  |--- Method      {}".format(contracts[-1].method.canonicalExpression))
                print("  |--- Method ID   {}".format(contracts[-1].method.id))
                print("  |--- StartBlock  {:,}".format(int(contracts[-1].startBlock)))
                print("  |--- Chunksize   {:,}\n".format(int(contracts[-1].chunksize)))
            
            # Manage `startBlock`

            filename ="config/contracts/lastSafedBlock/"+contracts[-1].name+"_"+contracts[-1].method.simpleExp
            # try to load config file with startBlock from AWS
            try :
                awsfile = s3.get_object(Bucket=aws_bucket, Key = filename+".txt")
            except:
                awsfile = False
            # if config file => set `startBlock`
            if awsfile:
                print("Loading `startBlock` for {} from AWS config file".format(contracts[-1].name))
                newStartBlock = s3.get_object(Bucket=aws_bucket, Key = filename+".txt")['Body'].read()
                newStartBlock = int(newStartBlock.decode("utf-8"))
                contracts[-1].fromBlock = newStartBlock+1
                if secureStart == True:
                    ip = input("Overwritting `startBlock` for {} to {} - please verify (y/n)".format(contracts[-1].name, contracts[-1].fromBlock))
                assert(ip != "n")
                print("`Startblock` overwritten for {} to Block {:,}\n".format(contracts[-1].name,
                                                                               contracts[-1].fromBlock))
                time.sleep(1)
            # else, if already safed results in AWS => tset `startBlock` to  last safed Block   
            elif existing_aws_results(contracts[-1], aws_bucket=aws_bucket):
                restore_fromBlock_from_AWS(contracts[-1],aws_bucket=aws_bucket)
                
            # else, take `startBlock`from contracts file
            else:
                if secureStart == True:
                    ip = input("FromBlock not overwritten for {}. Please verify (y/n)\n".format(contracts[-1].name))
                assert(ip != "n")
                time.sleep(1)
    return contracts