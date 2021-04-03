# ethereum-datafarm

The ethereum-datafarm aims to provide researches quick access to Ethereum blockchain data by offering an easy-to-use interface to scrap event logs from contracts and save them in csv and pickle format.

Features:
* Scrap every type of event data from pre-defined contracts
* No local or Infura node needed => Etherscan.io API is used
* Add or remove contracts to/from farm during runtime
* Already pre-configured for multiple events used by various contracts
* Low CPU, RAM and SSD requirements (AWS S3 is used)
* .Csv and .pickle support

### Install from source
```
$ git clone https://github.com/Nerolation/ethereum-datafarm
$ cd ethereum-datafarm
$ virtualenv ./venv
$ . ./venv/bin/activate
$ pip3 install -r requirements.txt
$ pip3 install -e .
```
#### Requirements:

* Python 3.5 or higher
* AWS S3 bucket and the related credentials
* Etherscan API key (for free at [etherscan.io](https://etherscan.io))
* Right filesystem structure (see below)


### Usage
```python
from farm.Farm import *

# Your AWS bucket
aws_bucket = "ethereum-datafarm"

# Path to Etherscan API key
keyPath = ".apikey/key.txt"


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
```

### Demo
[![asciicast](https://asciinema.org/a/404795.svg)](https://asciinema.org/a/404795)


#### Required Filesystem structure
```
ethereum-datafarm/
|-- farm/
|   |-- helpers/
|   |   |-- __init__.py
|   |   |-- Contract.py
|   |   |-- DailyResult.py
|   |   |-- Method.py
|   |   |-- EventHelper.py
|   |   |-- ContractHelper.py
|   |   
|   |-- __init__.py
|   |-- Farm.py
|
|-- .aws/
|   |-- credentials.txt
|   
|-- .apikey/
|   |-- apikey.txt
|
|-- config/
|   |-- end.txt
|
|-- README
```


