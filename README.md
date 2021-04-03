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
```bash
$ git clone https://github.com/Nerolation/ethereum-datafarm
$ cd ethereum-datafarm
$ virtualenv ./venv
$ . ./venv/bin/activate
$ pip3 install -r requirements.txt
$ mkdir .apikey .aws
$ nano .apikey/key.txt      => <API_Key>
$ nano .aws/credentials.txt => <AWS_credentials>

.aws/credentials.tx has the following structure (example):
AKIAIOSFODNN7EXAMPLE
wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```
#### Requirements:

* Python 3.5 or higher
* AWS S3 bucket and the related credentials
* Etherscan API key (for free at [etherscan.io](https://etherscan.io))
* Right filesystem structure (see below)
##### Required Filesystem structure
```console
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
<br />

## Usage
---
```python
from farm.Farm import *


aws_bucket = "ethereum-datahub" # Your AWS bucket
keyPath = ".apikey/key2.txt"    # Path to Etherscan API key
config_location="contracts"     # Path to location of config file (contracts.csv)


# Run
if __name__=="__main__":
    
    # Load contracts
    contracts = load_contracts(config_location=config_location, aws_bucket=aws_bucket)

    # Initialize Farm and get status
    farm = Farm(contracts=contracts, keyPath=keyPath, aws_bucket=aws_bucket).status()
    farm.start_farming()
```

<br />

## Demo
---
Initialize farm and starts scraping data:
* Loads contracts from config/contracts.csv file and created Contract objects
* Starts farm instance
* Loops over contracts and safes data into .csv and .pickle files <br /><br />
[![asciicast](https://asciinema.org/a/404795.svg)](https://asciinema.org/a/404795)
```console
Logging Output:
$ Timestamp -       Current timestamp
$ Contract -        Address of contract being processed
$ Current Chunk -   Block range being processed
$ Chunk Timestamp - Timestamp of processed block range
$ Events -          Number of events found in last chunk
$ Chsz -            Current Chunksize
$ Fc -              Filecounter - Files safed since initialization
```




