# ethereum-datafarm

The ethereum-datafarm aims to provide researches quick access to Ethereum blockchain data by offering an easy-to-use interface to scrap event logs from contracts and save them in .csv and .pickle format and/or directly sync to Google BigQuery.

###### 29-04-2021 The ethereum-datafarm is still a project in progress, the latest feature added was BigQuery support and a flexible switch between contract config files, which allows to run the application with extremely low memory requirements.

## Features:
* Scraps every type of event data from pre-defined contracts
* Saves each day in a separate file and/or syncs to Google BigQuery
* No local or [Infura](https://infura.io/?utm_source=Nerolation_Github&utm_medium=ethereum-datafarm) node needed => Etherscan.io API is used
* Possibility to add or remove contracts to/from the queue during runtime
* Possibility to securely terminate execution (config/end.txt file)
* Already pre-configured for multiple events used by various contracts
* Low CPU, RAM and SSD requirements (AWS S3 is used)
* .Csv, .pickle and SQL (GBQ) support
* Throttles down the number of requests to the API when the latest block is reached
* Logging (optional)
<br />

## Example data output
![image](sample_output/png/sample_output.png)
Or check out [this sample output file](sample_output/csv/13_11_2019.csv) of dai transfers
<br />

## Example dashboard
[Stablecoin Dashboard](https://toniwahrstaetter.com/ethereum-stablecoin-dashboard.html)

[Tornado Cash Dasboard](https://toniwahrstaetter.com/tornadocash.html) (mobile only)

[ERC-20s Dasboard](https://toniwahrstaetter.com/token-transfers.html) (mobile only)



## Usage

```python
from farm.Farm import *

aws_bucket = "ethereum-datahub" # Your AWS bucket

# Run
if __name__=="__main__":
    
    # Load contracts
    contracts = load_contracts(aws_bucket=aws_bucket)

    # Initialize Farm and get status
    farm = Farm(contracts=contracts, aws_bucket=aws_bucket).status()
    farm.start_farming()
```

<br />

### Install from source
```bash
$ git clone https://github.com/Nerolation/ethereum-datafarm
$ cd ethereum-datafarm
$ virtualenv ./venv
$ . ./venv/bin/activate
$ pip3 install -r requirements.txt
$ nano .apikey/key.txt   => <API_Key>
$ nano .aws/credentials  => <AWS_credentials>
$ nano .aws/config       => <Configs>
```


##### The .aws/credentials file might look like the following (replace the *aws_access_key_id* and *aws_secret_access_key* with your own details): <br />
```
[default]
aws_access_key_id=AKIAIOSFODNN7EXAMPLE
aws_secret_access_key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```
##### The config file:
```
[default]
region=eu-central-1
output=json
```
Check out the [official documentation](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html) for further infos

#### Requirements:

* Python 3.5 or higher
* AWS S3 bucket and the related credentials
* Etherscan API key (for free at [etherscan.io](https://etherscan.io))
* Right filesystem structure (see below)

##### Required Local Filesystem structure:
```data
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
|   |-- credentials        // Not in repo
|   |-- config             // Not in repo
|   
|-- .apikey/             
|   |-- apikey.txt         // Not in repo
|
|-- config/
|   |-- end.txt
|
|-- logs/                  // optional, if there, then logging is activated
|   |-- datafarm.log
|
|-- README
```
##### Required S3 Bucket structure:
```data
s3://
|-- <Your Bucket's name>/
|   |-- config/
|   |   |-- contracts/
|   |   |   |-- contracts.csv
|   |   |-- contracts2/    // optional, if there, then fileswitch is activated
|   |   |   |-- contracts.csv
```
##### Make sure that contracts.csv has the following structure: (Contract address, custom name, canonical Event, start block, chunksize)
```js
0x35A18000230DA775CAc24873d00Ff85BccdeD550,compound_uni,Approval(address,address,uint256),10921410,20000
0x35A18000230DA775CAc24873d00Ff85BccdeD550,compound_uni,Transfer(address,address,uint256),10921410,20000
0xf650C3d88D12dB855b8bf7D11Be6C55A4e07dCC9,compound_tether,Approval(address,address,uint256),9879363,20000
```
If you'd like to remove a contract during runtime, just switch the contracts address in the contracts.csv file with the word "remove":
```js
remove,compound_uni,Approval(address,address,uint256),10921410,20000
```
#### config/end.txt
The *end.txt* file is used to securely terminate the programme at a known position in the loop.
False means "no end", True equals "end, stop the programme"
<br />


## Logging
Make usage of logging by creating a directory named "logs" (see above @ local file structure)
This will automatically enable the logging module.
<br />

## Demo

Initialize farm and starts scraping data:
* Loads contracts from config/contracts.csv file and creates Contract objects
* Starts farm instance
* Loops over contracts and saves data into .csv and .pickle files <br /><br />
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
<br />

## Examples

Comparison of the number of Transfers of the largest Ethereum-based Stablecoins

![image_example](https://ethereum-datahub.s3.eu-central-1.amazonaws.com/graphs/stablecoin_transfers_for-git.png)


[Click here](https://toniwahrstaetter.com/example_usage.html) for more examples using the data (more coming...)

## Next Steps
* Safely terminated execution with KeyboardInterrupt commands
* Fix table-schema-bug that occurs for certain contracts

<br />


Visit [toniwahrstaetter.com](https://toniwahrstaetter.com/) for further details!
<br/><br/>

Anton Wahrstätter, 03.04.2021 
