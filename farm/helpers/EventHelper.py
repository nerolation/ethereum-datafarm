import json, requests, time
from json.decoder import JSONDecodeError

def from_hex(string):
    if str(string) == '0x':
        return 0
    return int(str(string),16)

def get_header_columns(methodId):
    if methodId in ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]:# Transfer(addr,addr,uint256)
        return ['timestamp','blocknumber','txhash','txindex','logindex',
                'txfrom','txto', 'txvalue', 'gas_price', 'gas_used']
    
    elif methodId in ["0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"]: # Approval(addr,addr,uint256)
        return ['timestamp','blocknumber','txhash','txindex','logindex',
                'owner','spender', 'txvalue', 'gas_price', 'gas_used']
                    
    elif methodId in ["0xf5c174d57843e57fea3c649fdde37f015ef08750759cbee88060390566a98797", # SupplyIncreased(address,uint256)
                      "0x0f6798a560793a54c3bcfe86a93cde1e73087d944c0ea20544137d4121396885", # Mint(address,uint256)
                      "0xc65a3f767206d2fdcede0b094a4840e01c0dd0be1888b5ba800346eaa0123c16"  # Issue(address,uint256)
                     ]: 
        return ['timestamp','blocknumber','txhash','txindex','logindex',
                'to', 'value', 'gas_price', 'gas_used']    
    
    elif methodId in ["0x1b7e18241beced0d7f41fbab1ea8ed468732edbcb74ec4420151654ca71c8a63", # SupplyDecreased(address,uint256)
                      "0x222838db2794d11532d940e8dec38ae307ed0b63cd97c233322e221f998767a6"  # Redeem(address,uint256)
                     ]: 
        return ['timestamp','blocknumber','txhash','txindex','logindex',
                'from', 'value', 'gas_price', 'gas_used'] 
    
    elif methodId in ["0xcc16f5dbb4873280815c1ee09dbd06736cffcc184412cf7a71a0fdb75d397ca5"]: # Burn(address,uint256)
        return ['timestamp','blocknumber','txhash','txindex','logindex',
                'burner', 'value', 'gas_price', 'gas_used']
    
    elif methodId in ["0xcb8241adb0c3fdb35b70c24ce35c5eb0c17af7431c99f827d44a445ca624176a"]: # Issue(uint256)
        return ['timestamp','blocknumber','txhash','txindex','logindex','value', 'gas_price', 'gas_used'] 
    
    elif methodId in ["0x702d5967f45f6513a38ffc42d6ba9bf230bd40e8f53b16363c7eb4fd2deb9a44"]: # Redeem(uint256)
        return ['timestamp','blocknumber','txhash','txindex','logindex',
                'value', 'gas_price', 'gas_used']
    
    elif methodId in ["0x61e6e66b0d6339b2980aecc6ccc0039736791f0ccde9ed512e789a7fbdd698c6"]: # DestroyedBlackFunds(addr,uint256)
        return ['timestamp','blocknumber','txhash','txindex','logindex',
                'blackListedUser', 'value', 'gas_price', 'gas_used']
    
    elif methodId in ["0xab8530f87dc9b59234c4623bf917212bb2536d647574c8e7e5da92c2ede0c9f8"]: # Mint(addr,addr,uint256)
        return ['timestamp','blocknumber','txhash','txindex','logindex',
                'minter', "to" , 'value', 'gas_price', 'gas_used']
    
    elif methodId in ["0x649bbc62d0e31342afea4e5cd82d4049e7e1ee912fc0889aa790803be39038c5"]: # DepositEvent(bytes,bytes,bytes,bytes,bytes)
        return ['timestamp','blocknumber','txhash','txindex','logindex',
                'public_key', "withdrawal_credentials" , 'amount', 'signature', 'index', 'gas_price', 'gas_used']
    
    elif methodId in ["0xa945e51eec50ab98c161376f0db4cf2aeba3ec92755fe2fcd388bdbbb80ff196"]: # Deposit TORN ETH
        return ['timestamp','blocknumber','txhash','txindex','logindex',
                'from', "value" , 'nonce', 'gas_price', 'gas_used']
    
    elif methodId in ["0xe9e508bad6d4c3227e881ca19068f099da81b5164dd6d62b2eaf1e8bc6c34931"]: # Withdraw TORN ETH
        return ['timestamp','blocknumber','txhash','txindex','logindex',
                'relayer',"to", "value" , 'nonce', 'gas_price', 'gas_used']


def prepare_event(e, methodId, KEY):
    if methodId in ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef", # Transfer
                    "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925",  # Approval
                    "0xab8530f87dc9b59234c4623bf917212bb2536d647574c8e7e5da92c2ede0c9f8"
                   ]: 
        va = from_hex(e['data'])
        bn = from_hex(e['blockNumber'])
        ts = from_hex(e['timeStamp'])
        th = e['transactionHash']
        ti = from_hex(e['transactionIndex'])
        gp = from_hex(e['gasPrice'])
        gu = from_hex(e['gasUsed'])
        li = from_hex(e['logIndex'])
        tf = '0x' + e['topics'][1][-40:]
        tt = '0x' + e['topics'][2][-40:]
        return [ts,bn,th,ti,li,tf,tt,va,gp,gu]
    
    
    elif methodId in ["0xcb8241adb0c3fdb35b70c24ce35c5eb0c17af7431c99f827d44a445ca624176a", # Issue
                      "0x702d5967f45f6513a38ffc42d6ba9bf230bd40e8f53b16363c7eb4fd2deb9a44", # Redeem
                     ]:            
        va = from_hex(e['data'])
        bn = from_hex(e['blockNumber'])
        ts = from_hex(e['timeStamp'])
        th = e['transactionHash']
        ti = from_hex(e['transactionIndex'])
        gp = from_hex(e['gasPrice'])
        gu = from_hex(e['gasUsed'])
        li = from_hex(e['logIndex'])
        return [ts,bn,th,ti,li,va,gp,gu]
    
    elif methodId in ["0xf5c174d57843e57fea3c649fdde37f015ef08750759cbee88060390566a98797", # SupplyIncreased
                      "0x1b7e18241beced0d7f41fbab1ea8ed468732edbcb74ec4420151654ca71c8a63", # SupplyDecreased
                      "0x0f6798a560793a54c3bcfe86a93cde1e73087d944c0ea20544137d4121396885", # Mint
                      "0xcc16f5dbb4873280815c1ee09dbd06736cffcc184412cf7a71a0fdb75d397ca5", # Burn
                      "0xc65a3f767206d2fdcede0b094a4840e01c0dd0be1888b5ba800346eaa0123c16", # Issue
                      "0x222838db2794d11532d940e8dec38ae307ed0b63cd97c233322e221f998767a6"  # Redeem
                     ]:
                      
        va = from_hex(e['data'])
        bn = from_hex(e['blockNumber'])
        ts = from_hex(e['timeStamp'])
        th = e['transactionHash']
        ti = from_hex(e['transactionIndex'])
        gp = from_hex(e['gasPrice'])
        gu = from_hex(e['gasUsed'])
        li = from_hex(e['logIndex'])
        tf = '0x' + e['topics'][1][-40:]
        return [ts,bn,th,ti,li,tf,va,gp,gu]
    
    
    elif methodId in ["0x61e6e66b0d6339b2980aecc6ccc0039736791f0ccde9ed512e789a7fbdd698c6"]: # DestroyedBlackFunds
        bn = from_hex(e['blockNumber'])
        ts = from_hex(e['timeStamp'])
        th = e['transactionHash']
        ti = from_hex(e['transactionIndex'])
        gp = from_hex(e['gasPrice'])
        gu = from_hex(e['gasUsed'])
        li = from_hex(e['logIndex'])
        da1 = '0x' + e['data'][26:66]
        da2 = from_hex('0x' + e['data'][-20:])
        return [ts,bn,th,ti,li,da1,da2,gp,gu]
    
    elif methodId in ["0x649bbc62d0e31342afea4e5cd82d4049e7e1ee912fc0889aa790803be39038c5"]: # DepositEvent
        bn = from_hex(e['blockNumber'])
        ts = from_hex(e['timeStamp'])
        th = e['transactionHash']
        ti = from_hex(e['transactionIndex'])
        gp = from_hex(e['gasPrice'])
        gu = from_hex(e['gasUsed'])
        li = from_hex(e['logIndex'])
        pk = e['data'][386:482]
        si = e['data'][834:1026]
        cr = e['data'][578:642]
        am = e['data'][706:722]
        ix = e['data'][1090:1106]
        return [ts,bn,th,ti,li,pk,cr,am,si,ix,gp,gu]
    
    elif methodId in ["0xa945e51eec50ab98c161376f0db4cf2aeba3ec92755fe2fcd388bdbbb80ff196"]: # DepositEvent TORN ETH
        bn = from_hex(e['blockNumber'])
        ts = from_hex(e['timeStamp'])
        th = e['transactionHash']
        ti = from_hex(e['transactionIndex'])
        gp = from_hex(e['gasPrice'])
        gu = from_hex(e['gasUsed'])
        li = from_hex(e['logIndex'])
        
        # Extra Query to get the initiator of the tx
        _API = "https://api.etherscan.io/api?{}"
        _QUERY = "module=proxy&action=eth_getTransactionByHash&txhash={}&apikey={}"
        _queryString = _API.format(_QUERY.format(th,KEY))
        while _res == None:
            try:
                _res = json.loads(requests.get(_queryString).content) 
            except JSONDecodeError:
                _res=None
                print(requests.get(_queryString).content)
                print("Some strange JSONDecodeError")
                time.sleep(1)
        
        _res = _res['result']
        time.sleep(0.1)
        tf = _res["from"]
        no = from_hex(_res["nonce"])
        va = from_hex(_res['value'])
        
        return [ts,bn,th,ti,li,tf,va,no,gp,gu]
    
    elif methodId in ["0xe9e508bad6d4c3227e881ca19068f099da81b5164dd6d62b2eaf1e8bc6c34931"]: # WithdrawEvent TORN ETH
        bn = from_hex(e['blockNumber'])
        ts = from_hex(e['timeStamp'])
        th = e['transactionHash']
        ti = from_hex(e['transactionIndex'])
        gp = from_hex(e['gasPrice'])
        gu = from_hex(e['gasUsed'])
        li = from_hex(e['logIndex'])
        
        tt = "0x"+e['data'][26:66]
        
        # Extra Query to get the initiator of the tx
        _API = "https://api.etherscan.io/api?{}"
        _QUERY = "module=proxy&action=eth_getTransactionByHash&txhash={}&apikey={}"
        _queryString = _API.format(_QUERY.format(th,KEY))
        _res=None
        while _res == None:
            try:
                _res = json.loads(requests.get(_queryString).content) 
            except JSONDecodeError:
                _res=None
                print(requests.get(_queryString).content)
                print("Some strange JSONDecodeError")
                time.sleep(1)
        
        _res = _res['result']
        time.sleep(0.1)
        tf = _res["from"]
        no = from_hex(_res["nonce"])
        va = from_hex(_res['value'])
        
        return [ts,bn,th,ti,li,tf,tt,va,no,gp,gu]
    
    else:
        bn = from_hex(e['blockNumber'])
        ts = from_hex(e['timeStamp'])
        th = e['transactionHash']
        ti = from_hex(e['transactionIndex'])
        gp = from_hex(e['gasPrice'])
        gu = from_hex(e['gasUsed'])
        li = from_hex(e['logIndex'])
        da = e['data']
        chunk = [ts,bn,th,ti,li,gp,gu]
        if len(e['topics'])>1:
            to1 = e['topics'][1]
            chunk.append(to1)
        if len(e['topics'])>2:
            to2 = e['topics'][2]
            chunk.append(to2)
        if len(e['topics'])>3:
            to3 = e['topics'][3]
            chunk.append(to3)
        return chunk
