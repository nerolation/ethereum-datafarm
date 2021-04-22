from farm.Farm import *

aws_bucket = "ethereum-datahub" # Your AWS bucket

# Run
if __name__=="__main__":
    
    # Load contracts
    contracts = load_contracts(aws_bucket=aws_bucket, config_location="contracts2")

    # Initialize Farm and get status
    farm = Farm(contracts=contracts,aws_bucket=aws_bucket, useBigQuery=True).status()
    farm.start_farming()
