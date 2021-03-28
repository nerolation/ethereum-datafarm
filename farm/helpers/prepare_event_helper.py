def from_hex(string):
    if str(string) == '0x':
        return 0
    return int(str(string),16)

def prepare_event(e, methodId):
    if methodId in ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                    "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"]:
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
    #elif
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
