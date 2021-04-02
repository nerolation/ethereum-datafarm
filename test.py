from farm.Farm import *

# Your AWS bucket
aws_bucket = "ethereum-datahub"

# Path to Etherscan API key
keyPath = ".apikey/key2.txt"


# Run
if __name__=="__main__":
    
    # Load contracts
    # @param config_location Location of config file (contracts.csv)
    # @param aws_bucket Aws bucket name
    contracts = load_contracts(config_location="contracts2", aws_bucket=aws_bucket)

    # Initialize Farm and get status
    #
    # @param contracts Loaded contracts array
    # @param keyPath Path to API key
    farm = Farm(contracts=contracts, keyPath=keyPath, aws_bucket=aws_bucket).status()
    farm.start_farming()
