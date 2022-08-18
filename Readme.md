# ethereum-datafarm v2.0

### Parse Smart Contract event data without requiring an archive/full node. 

The ethereum-datafarm aims to provide quick access to historical Ethereum event data by offering an easy-to-use interface to parse event logs from contracts and save them in .csv format.

#### The ethereum-datafarm uses the [Etherscan.io API](https://docs.etherscan.io/), which can be used for free up to fairly generous limits.


![](https://github.com/Nerolation/ethereum-datafarm/blob/main/pic/data_pic.gif)



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
0x30f938fED5dE6e06a9A7Cd2Ac3517131C317B1E7,giveth,Donate(uint64,uint64,address,uint256),5876857,50000
0x30f938fED5dE6e06a9A7Cd2Ac3517131C317B1E7,giveth,DonateAndCreateGiver(address,uint64,address,uint256),5876857,50000
0xDe30da39c46104798bB5aA3fe8B9e0e1F348163F,gitcoin,Transfer(address,address,uint256),12422079,50000
0x1fd169A4f5c59ACf79d0Fd5d91D1201EF1Bce9f1,molochdao,SubmitVote(uint256,address,address,uint8),7218566,50000
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

### Cite as

```
@misc{Wahrstaetter2022,
	title = {Ethereum-datafarm},
	url = {https://github.com/Nerolation/ethereum-datafarm},
	urldate = {2022-08-18},
	publisher = {Github},
	author = {Anton Wahrstätter},
	year = {2022},
}
```

Visit [toniwahrstaetter.com](https://toniwahrstaetter.com/) for further details!
<br/><br/>

Anton Wahrstätter, 18.08.2022
