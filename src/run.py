from ethereum_datafarm import *

if __name__=="__main__":
    
    # Initialize Farm
    farm = Farm()
    
    # Load Contracts
    farm.load_contracts()
    
    # Start parsing
    farm.farm()
