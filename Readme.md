# ethereum-datafarm 2.0

### Parse Smart Contract event data without requiring an archive/full node. 

The ethereum-datafarm aims to provide quick access to historical Ethereum event data by offering an easy-to-use interface to parse event logs from contracts and save them in .csv format.

#### The ethereum-datafarm uses the [Etherscan.io API](https://docs.etherscan.io/), which can be used for free up to fairly generous limits.


![](https://github.com/Nerolation/ethereum-datafarm/blob/main/pic/archive_node.gif)



## Features:
* Scraps every type of event data from pre-defined contracts
* Fetches Abis from contracts to detect events
* No local or [Infura](https://infura.io/?utm_source=Nerolation_Github&utm_medium=ethereum-datafarm) node required
* Low CPU and RAM requirements 
* Multiprocessing support
* Custom storage location using the `-loc` or `--location` flag: E.g. `python3 run.py -loc ./myfolder
<br />

## Example data output
![image](sample_output/png/sample_output.png)
Or check out [this sample output file](sample_output/csv/13_11_2019.csv) of dai transfers
<br />


## Usage

```python
from ethereum_datafarm import *


if __name__=="__main__":
    
    # Initialize Farm
    farm = Farm()
    
    # Load Contracts
    farm.load_contracts()
    
    # Start parsing
    farm.farm()
```

###### NOTE: If the event-emitting contract is a proxie contract (e.g. upgradable contracts) then the abi detection may fail. In such cases, take the right abi from Etherscan and add the .abi file manually.

<br />

### Install from source
```bash
$ git clone https://github.com/Nerolation/ethereum-datafarm
$ cd ethereum-datafarm
$ python3 -m venv .
$ source bin/activate
$ pip install -r requirements.txt
```



#### Requirements:

* Python 3.5 or higher
* Etherscan API key (for free at [etherscan.io](https://etherscan.io))


##### Make sure that contracts.csv has the following structure: (Contract address, custom name, canonical Event, start block, chunksize)
```js
0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48,usdc,Transfer(address,address,uint256),6082465,500 
0x6B175474E89094C44Da98b954EedeAC495271d0F,dai,Transfer(address,address,uint256),8928158,500 
0x5ef30b9986345249bc32d8928B7ee64DE9435E39,makerdao,NewCdp(address,address,uint256),8928198,5000
```

## Demo

Initialize farm and starts parsing data:
* Loads contracts from contracts.csv file
* Starts farm instance
* Loops over contracts and saves data into .csv <br /><br />
[![asciicast](https://asciinema.org/a/gjRIuU2LmCa6VlS0reWHSKUV5.svg)](https://asciinema.org/a/gjRIuU2LmCa6VlS0reWHSKUV5)

<br />


## Example dashboard
[Stablecoin Dashboard](https://toniwahrstaetter.com/ethereum-stablecoin-dashboard.html)

[Tornado Cash Dasboard](https://toniwahrstaetter.com/tornadocash.html) (mobile only)

<br />

Visit [toniwahrstaetter.com](https://toniwahrstaetter.com/) for further details!
<br/><br/>

Anton Wahrstätter, 18.08.2022
