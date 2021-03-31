from farm.Farm import *

aws_bucket = "ethereum-datahub"
keyPath = ".apikey/key.txt"



if __name__=="__main__":
    contracts = load_contracts(config_location="contracts", aws_bucket=aws_bucket)

    farm = Farm(contracts=contracts, keyPath=keyPath, aws_bucket=aws_bucket).status()
    farm.start_farming()

