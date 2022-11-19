import requests
import json
import re
import os
from termcolor import colored
from datetime import datetime
import numpy as np
import time
import sha3
import argparse
from multiprocessing import cpu_count


parser = argparse.ArgumentParser(formatter_class=lambda prog: argparse.HelpFormatter(prog,max_help_position=60))
parser.add_argument('-loc', '--location', help="output location - default: ../data", default="./data")
parser.add_argument('-c', '--cores', help="cores available", default=str(cpu_count()))
parser.add_argument('-log', '--log', help="activate logging", action='store_true')


_args = parser.parse_args()

LOCATION = vars(_args)["location"]
CORES = int(vars(_args)["cores"])
LOGGING = bool(vars(_args)["log"])

with open("../key/key.txt", "r") as file:
    KEY = file.read()
    
if not os.path.isdir("../abis"):
    os.mkdir("../abis")

if not os.path.isdir(f"../{LOCATION}"):
    os.mkdir(f"../{LOCATION}")
    
if not os.path.isdir("../tmp"):
    os.mkdir("../tmp")

PAYLOAD = "https://api.etherscan.io/api" \
             + "?module=logs" \
             + "&action=getLogs" \
             + "&fromBlock={}" \
             + "&toBlock={}" \
             + "&address={}" \
             + "&topic0={}" \
             + "&page={}" \
             + "&offset=1000" \
             + "&apikey={}".format(KEY)

BASIC_HEADER = ['address','blocknumber','timestamp','txhash','txindex','gas_price','gas_used','logindex']

def build_payload(*args):
    return PAYLOAD.format(*args)

def send_payload(payload):
    try:
        _res = requests.get(payload)
        res = json.loads(_res.content)
        if "no records found" in res["message"].lower():
            return "no records found"
        if "result window is too large" in res["message"].lower():
            msg = "result window is too large"
            print(WARN_MSG.format(msg))
            return "page limit reached"
            
        if int(res["status"]) != 1:
            raise 
        
    except:
        msg = "payload failed (fetching event)"
        print(WARN_MSG.format(msg))
        log(msg)
        try:
            print(res) 
            print(res["message"].lower())
        except:
            pass
        print(colored("Waiting for 10 seconds", "red"))
        time.sleep(10)
        return send_payload(payload)
        
    return res["result"]
            
        
def dump_cache_to_disk(df, filename, name, method):
    df = df.loc[:,~df.columns.duplicated()].copy()
    for c in df:
        if df[c].dtype == "float64":
            df[c] = df[c].apply(lambda x: int(x))
        if df[c].dtype == "object":
            if type(df[c][0]) == float:
                try:
                    df[c] = df[c].apply(lambda x: int(x))
                except:
                    pass

    last_row = df.iloc[-1]
    content = "{}-{}".format(last_row["blocknumber"],last_row["txhash"])
    with open(f"../tmp/{name}_{method}_last_stored_tx.txt", "w") as f:
        f.write(content)
    df.to_csv(filename, index=None)

    
def check_custom_start(name, method):
    if os.path.isfile(f"../tmp/{name}_{method}_last_stored_tx.txt"):
        with open(f"../tmp/{name}_{method}_last_stored_tx.txt", "r") as file:
            start = file.read()
            startblock, starttx = start.split("-")
        return int(startblock), starttx
    else:
        return [None, None]
        


def get_method_from_canonical_expression(method):
    return '0x' + sha3.keccak_256(method.encode('utf-8')).hexdigest()

def get_print_name(name):
    if len(name) > 25:
        return name[0:21]+"..."
    else:
        return name
    
def get_print_method(method):
    if len(method) > 25:
        return method[0:21]+"..."
    else:
        return method
    
def from_hex(string):
    if str(string) == '0x':
        return 0
    return int(str(string),16) 

def convert_to(bytes32inHex, toType):
    if toType == "address":
        return '0x' + bytes32inHex[-40:]
    if "int" in toType:
        return from_hex(bytes32inHex)
    else:
        return bytes32inHex
    
def latest_block():
    payload = "https://api.etherscan.io/api" \
             + "?module=block" \
             + "&action=getblocknobytime" \
             + f"&timestamp={round(datetime.timestamp(datetime.now()))}" \
             + "&closest=before" \
             + "&apikey={}".format(KEY)

    time.sleep(np.random.randint(1,3))
    try:
        res = requests.get(payload)
        return int(json.loads(res.content)["result"]) - 6 # 6 blocks to make sure no re-orgs
    except:
        time.sleep(np.random.randint(1,10))
        msg = "payload failed (latest block)"
        print(WARN_MSG.format(msg))
        log(msg)
        return latest_block()
     
def get_event_info(contract):
    inames=[]
    names=[]
    itypes=[]
    types=[]
        
    for i in contract.abi:
        if i["type"] != "event" or contract.simpleMethod != i["name"].lower():
            continue
        for args in i["inputs"]:
            if args["indexed"]:
                inames.append(args["name"])
                itypes.append(args["type"])
            else:
                names.append(args["name"])
                types.append(args["type"])
    return inames, names, itypes, types
    

def verify_abi(abi, address, name, simpleMethod):
    success = True
        
    if "admin" in abi and "proxy" in abi:
        msg = f"contract @ {address} ({name}) contains the word ``admin``"
        print(WARN_MSG.format(msg))
        # success = False -- let user know but dont force interruptions
        
    if f'"name":"{simpleMethod}"'.lower() not in abi.lower():
        print(f'"name":"{simpleMethod}"'.lower())
        msg = f"contract @ {address} ({name}) does not contain method {simpleMethod}; maybe proxie contract?"
        print(WARN_MSG.format(msg))
        success = False
              
    if "not verified" in abi:
        msg = f"contract @ {address} ({name}) not verified;"
        print(WARN_MSG.format(msg))
        success = False
        
    if not abi.endswith("]"):
        msg = f"contract @ {address} ({name}) abi probably broken;"
        print(WARN_MSG.format(msg))
        success = False     
        
    return success


def get_abi(contract):
    try:
        with open(f"../abis/{contract.name}.abi", "r") as file:
            abi = file.read()
            success = verify_abi(abi, contract.address, contract.name, contract.simpleMethod)
            
            if success:
                msg = "abi found locally | " + colored("contract", "green") + f" @ {contract.address} ({contract.name})"
                print(INFO_MSG.format(msg))
            else:
                msg = f"contract @ {contract.address} ({contract.name}) failed to retrieve abi "
                msg2 = f"contract @ {contract.address} ({contract.name}) add abi manually to the contract's abi file"                
                print(WARN_MSG.format(msg))
                print(WARN_MSG.format(msg2))
                input("press any key to continue")
                return get_abi(contract)
        
    except:
        esc = f"https://api.etherscan.io/api?module=contract&action=getabi&address={contract.address}&apikey={KEY}"
        res = requests.get(esc)
        time.sleep(1)
        abi = json.loads(res.content)["result"]
       
        success = verify_abi(abi, contract.address, contract.name, contract.simpleMethod)
        
        if success:    
            msg = f"contract @ {contract.address} ({contract.name}) requesting abi "
            print(INFO_MSG.format(msg))
            with open(f"../abis/{contract.name}.abi", "w") as file:
                file.write(abi)
                msg = f"{contract.name} contract abi stored locally"
                print(INFO_MSG.format(msg))
            return get_abi(contract)
        else:
            msg = f"contract @ {contract.address} ({contract.name}) failed to retrieve right abi "
            msg2 = f"contract @ {contract.address} ({contract.name}) make sure to add abi manually to the contract's abi file"
            if not os.path.isdir(f"../abis/{contract.name}.abi"):
                with open(f"../abis/{contract.name}.abi", "w") as file:
                    file.write("")
            print(WARN_MSG.format(msg))
            print(WARN_MSG.format(msg2))
            input("press any key to continue")
            return get_abi(contract)
        
        

    abi = eval(abi.replace("false", "False").replace("true", "True"))
    return abi
    
def load_all(contracts=[],start=True,config_location="../contracts.csv"):
    with open("../contracts.csv", "r") as file:
        file = file.read().replace(" ", "").split("\n")    
    for f in file:
        if f == "":
            continue
        if f.startswith("#"):
            continue
        yield tuple(re.split("\,(?=.*\()|\,(?!.*\))", f))
        
def set_up_directory(name, simpleMethod):
    fileCounter = 0
    if not os.path.isdir(f"../{LOCATION}/{name}"):
        os.mkdir(f"../{LOCATION}/{name}")

    elif len(os.listdir(f"../{LOCATION}/{name}")) > 0:
        print(WARN_MSG.format(f"non-empty directory ../{LOCATION}/{name}"))

        lastFlNr = max(list(map(int, re.findall("[0-9]+",str(os.listdir(f"../{LOCATION}/{name}")))))+[0])
        fileCounter = lastFlNr + 1
        print(INFO_MSG.format(f"fileCounter set to {fileCounter} for {name} and method {simpleMethod}"))

    return fileCounter
        
def curtime():
    return "["+datetime.strftime(datetime.now(), "%m-%d|%H:%M:%S")+"]"
        
INFO_MSG = colored("[INFO]", "green") + " {}".format(curtime()) + " {}"
WARN_MSG = colored("[WARN]", "red") + " {}".format(curtime()) + " {}"

def log(msg):
    if LOGGING:
        with open("./log.txt", "a") as file:
            file.write(msg+"\n")

def print_start():
    c = """
    ███████╗████████╗██╗  ██╗    ██████╗  █████╗ ████████╗ █████╗ ███████╗ █████╗ ██████╗ ███╗   ███╗    ██████╗     ██████╗ 
    ██╔════╝╚══██╔══╝██║  ██║    ██╔══██╗██╔══██╗╚══██╔══╝██╔══██╗██╔════╝██╔══██╗██╔══██╗████╗ ████║    ╚════██╗   ██╔═████╗
    █████╗     ██║   ███████║    ██║  ██║███████║   ██║   ███████║█████╗  ███████║██████╔╝██╔████╔██║     █████╔╝   ██║██╔██║
    ██╔══╝     ██║   ██╔══██║    ██║  ██║██╔══██║   ██║   ██╔══██║██╔══╝  ██╔══██║██╔══██╗██║╚██╔╝██║    ██╔═══╝    ████╔╝██║
    ███████╗   ██║   ██║  ██║    ██████╔╝██║  ██║   ██║   ██║  ██║██║     ██║  ██║██║  ██║██║ ╚═╝ ██║    ███████╗██╗╚██████╔╝
    ╚══════╝   ╚═╝   ╚═╝  ╚═╝    ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝╚═╝     ╚═╝  ╚═╝╚═╝  ╚═╝╚═╝     ╚═╝    ╚══════╝╚═╝ ╚═════╝
    """
    print(colored(c, "green", attrs=["dark","bold"]))    
    print(colored("?? Anton Wahrst??tter 2022\n", "green", attrs=["dark","bold"]))
    print("Starting datafarm...")
    print(f"Storage location: ../{LOCATION}/<contract>")

class ContractLoadingInterrupted(Exception):
    pass
