from utils import *
import requests
import json
from web3 import Web3
from multiprocessing import Process, cpu_count, connection
import pandas as pd
from eth_abi import decode as abi_decode
import random

SLOW_DOWN = 0 # seconds to wait between api calls
STORAGE_THRESHOLD = 9e3



class Farm():
    def __init__(self):
        print_start()
        log("".join(["="]*50)+ "\nStart new farm instance...")
        self.contracts = list()
        
    def load_contracts(self):
        try:
            for c in load_all():
                self.contracts.append(Contract(*c))
        except:
            msg = colored(f"\nLoading contracts interrupted", "red", attrs=["bold"])
            raise ContractLoadingInterrupted(msg)          
    
    def farm(self):
        
        print(INFO_MSG.format("Start farming..."))
        try:
            if CORES > 1:
                cpus = CORES-1 # CORES form utils.py
            else:
                cpus = 1
            #self.contracts = random.shuffle(self.contracts)
            trs = int(len(self.contracts)/cpus)
            tranches={}
            for i in range(cpus):
                if i == cpus-1: 
                    tranches[i] = self.contracts[trs*i:]
                else: 
                    tranches[i] = self.contracts[trs*i:trs*(i+1)]
            processes = []
            for i in range(cpus):
                p = Process(target = self.split_tasks, args=tuple([tranches[i]]))
                p.start()
                processes.append(p)
            connection.wait(p.sentinel for p in processes)
            
        except KeyboardInterrupt:
            msg = colored("Safely terminating...\n", "green", attrs=["bold"])
            print(INFO_MSG.format(msg))
            if len(processes) > 0:
                for p in processes:
                    p.terminate()               
               
    def split_tasks(self, c): 
            for contract in c:
                msg = colored(f"Start parsing {contract}", "green", attrs=["bold"])
                print(INFO_MSG.format(msg))
                log(f"Start parsing {contract}")
                contract.scrape()
    

class Contract():
    def __init__(self, address, name, method, startBlock, chunksize):
        self.address = Web3.toChecksumAddress(address)
        self.name = name.lower()
        self.method = method
        self.simpleMethod = method.split("(")[0].lower()
        self.topic0 = get_method_from_canonical_expression(self.method)
                
        newStartBlock, newStartTx = check_custom_start(self.name, self.simpleMethod)
        if newStartBlock:
            self.startBlock = newStartBlock
            self.startTx = newStartTx
            
            msg = "{} ({}) {}".format(self.address, self.name, colored("starting at last known location", "green"))
            msg2 = "{} ({}) blockheight set to {:,.0f}".format(self.address, self.name, self.startBlock)
            print(INFO_MSG.format(msg))
            print(INFO_MSG.format(msg2))
            
            if newStartTx == "None":
                self.run = True
                self.startTx = None
                
            else:
                msg3 = "{} ({}) starting after tx {}".format(self.address, self.name, self.startTx[:-56]+"...")
                print(INFO_MSG.format(msg3))
                self.run = False

        else:
            self.startBlock = int(startBlock)
            self.startTx = None
            self.run = True
        
        self.fromblock = self.startBlock
        self.chunksize = int(chunksize)
        
        self.printName = get_print_name(self.name)
        self.printMethod = get_print_method(self.method)
        self.storageLocation = f"../data/{self.name}/{self.simpleMethod}_" + "{}.csv"
        
        self.abi = get_abi(self)
        eventInfo = get_event_info(self)
        self.evINames, self.evNames, self.evITypes, self.evTypes = eventInfo
        
        self.columns = BASIC_HEADER + self.evINames + self.evNames
        
        self.CACHE = pd.DataFrame(columns=self.columns)
        self.LATEST_BLOCK = latest_block()
        self.timeSinceLatestBlock = datetime.now()
        self.avgNrOfPages = [1.5]
        
        self.fileCounter = set_up_directory(self.name, self.simpleMethod)

    
    def scrape(self):

        while self.fromblock < self.LATEST_BLOCK:
           
            self.try_adapting_chunksize()
                        
            self.toblock = self.fromblock + self.chunksize
            
            if self.toblock > self.LATEST_BLOCK:
                self.toblock = self.LATEST_BLOCK 
       
            results = [0]*1001
            page = 1
            while len(results) >= 1000:
                
                payload = build_payload(self.fromblock, self.toblock, self.address, self.topic0, page)
                results = send_payload(payload)
                time.sleep(SLOW_DOWN)
                
                success = True
                if results == "no records found":
                    self.log_nothing_found()
                    self.avgNrOfPages.append(1) 
                    self.avgNrOfPages = self.avgNrOfPages[-10:]
                    results = [0]
                    continue
                
                if results == "page limit reached":
                    msg = "decreasing chunk size and trying again..."
                    print(WARN_MSG.format(msg))
                    self.fromblock, self.startTx = check_custom_start(self.name, self.simpleMethod)
                    self.CACHE = pd.DataFrame(columns=self.columns)
                    self.run = False
                    self.chunksize = int(self.chunksize/10)
                    if self.chunksize < 1: self.chunksize = 1
                    self.log_chunk_size(self.chunksize*10, "decreasing")
                    self.avgNrOfPages.append(1.5)
                    self.avgNrOfPages = self.avgNrOfPages[-10:]
                    results = [0]
                    success = False
                    continue
                    
                self.parse_results(results)
                
                self.log_progress(len(results), page)

                page += 1
            
            if not success:
                continue
                
            if page - 1 >= 1:
                self.avgNrOfPages.append(page - 1) 
                self.avgNrOfPages = self.avgNrOfPages[-10:]
            else:
                self.avgNrOfPages.append(1) 
                self.avgNrOfPages = self.avgNrOfPages[-10:]
            
            # Update latest block ever 600 seconds
            if (datetime.now()-self.timeSinceLatestBlock).total_seconds() > 6e2:
                msg = f"updating latest block for {self.name}"
                print(INFO_MSG.format(msg))
                self.LATEST_BLOCK = latest_block()
                self.timeSinceLatestBlock = datetime.now()
            
            self.fromblock =  self.toblock + 1
            
        if len(self.CACHE) > 0 and self.run:
            self.log_storage()
            dump_cache_to_disk(self.CACHE, self.storageLocation.format(self.fileCounter), self.name, self.simpleMethod)
            self.CACHE = pd.DataFrame(columns=self.columns)
            self.fileCounter += 1
        
        content = "{}-{}".format(self.fromblock,"None")
        with open(f"../tmp/{self.name}_{self.simpleMethod}_last_stored_tx.txt", "w") as f:
            f.write(content)
            
        self.log_end()
    
    def parse_results(self, results):
        for r in results:
            indexed_topics = []
            non_indexed_topics = []
 
            timeStamp = from_hex(r['timeStamp'])
            blockNumber = from_hex(r['blockNumber'])
            transactionHash = r['transactionHash']
            transactionIndex = from_hex(r['transactionIndex'])
            logIndex = from_hex(r['logIndex'])
            gasPrice = from_hex(r['gasPrice'])
            gasUsed = from_hex(r['gasUsed'])
   
            for index, t in enumerate(r["topics"][1:]):
                t = abi_decode([self.evITypes[index]], bytes.fromhex(t[2:]))
                indexed_topics.append(t[0])
            
            data = r["data"][2:]
            non_indexed_topics = list(abi_decode(self.evTypes, bytes.fromhex(data)))

                
            eventInfo = [self.address, blockNumber, timeStamp, 
                         transactionHash, transactionIndex, 
                         gasPrice, gasUsed, logIndex]
            
            for indexedTopic in indexed_topics:
                eventInfo += [indexedTopic]
                
            for nonIndexedTopic in non_indexed_topics:
                eventInfo += [nonIndexedTopic]

            self.make_row(eventInfo)

            if len(self.CACHE) >= STORAGE_THRESHOLD and self.run:
                self.log_storage()
                dump_cache_to_disk(self.CACHE, self.storageLocation.format(self.fileCounter), self.name, self.simpleMethod)
                self.CACHE = pd.DataFrame(columns=self.columns)
                self.fileCounter += 1
            
            if self.startTx == transactionHash:
                if not self.run:
                    msg = f"contract @ {self.address} ({self.name}) last known tx found; start parsing..."
                    print(INFO_MSG.format(msg))
                self.run = True
                self.CACHE = pd.DataFrame(columns=self.columns)
                          
            
    def make_row(self, *args):
        self.CACHE.loc[len(self.CACHE)] = args[0]
        
    def try_adapting_chunksize(self):
        op = None
        if sum(self.avgNrOfPages)/len(self.avgNrOfPages) > 3:
            old_cs = self.chunksize
            self.chunksize = int(self.chunksize/5)
            op = "decreasing"
            self.avgNrOfPages = [3]*10
            
            
        elif sum(self.avgNrOfPages)/len(self.avgNrOfPages) <= 1:
            old_cs = self.chunksize
            if self.chunksize < 5: factor = 1.5 
            else: factor = 1.2
            self.chunksize = int(self.chunksize*factor)
            self.avgNrOfPages.append(1.5)
            self.avgNrOfPages = self.avgNrOfPages[-10:]
            op = "increasing"
            
        if self.chunksize <= 1:
            self.chunksize = 2
        if self.chunksize > 100000:
            self.chunksize = 100000
        if op:
            self.log_chunk_size(old_cs, op)
        
    def log_progress(self, len_result, page):
        _printName = self.printName[:16]+"..." if len(self.printName) >= 16 else self.printName + "              "
        msg = "parsing {0:<19} | ".format(_printName[:19]) \
            + "{:>10,.0f}-{:>10,.0f} | ".format(self.fromblock, self.toblock) \
            + colored("{:>4.0f}/1000".format(len_result), "green") +  f" | Page {page}" \
            + " | cs {:>7,.0f} | cache {:>6,.0f}".format(self.chunksize, len(self.CACHE))
        
        print(INFO_MSG.format(msg))
    
    def log_chunk_size(self, old_size, op):
        avg = sum(self.avgNrOfPages)/len(self.avgNrOfPages)
        msg = "{} chunk size for {}".format(op, self.printName[:17]+"...:") \
            + " {:,.0f} --> {:,.0f} with avg. pages of {:.2f}".format(old_size, self.chunksize, avg) 
        print(INFO_MSG.format(msg))
        
    def log_nothing_found(self):
        _printName = self.printName[:14]+"..." if len(self.printName) >= 14 else self.printName + "              "
        msg = colored("no result", "red") + " {0:<17} | ".format(colored(_printName[:17]), "red") \
              + "{:>10,.0f}-{:>10,.0f} | {:>9}".format(self.fromblock, self.toblock, "cs " + "{:,.0f}".format(self.chunksize))
        print(INFO_MSG.format(msg))
    
    def log_storage(self):
        msg = colored("storing ", "green") + f"{self.printName} with " \
                "{:,.0f} entries @ ".format(len(self.CACHE)) \
                + colored(f"{self.storageLocation.format(self.fileCounter)}", "green")
        print(INFO_MSG.format(msg))
        log(f"storing {self.printName} with {len(self.CACHE)} entries @ {self.storageLocation.format(self.fileCounter)}")
        
    def log_end(self):
        msg = colored(f"terminating {self.printName}", "green", attrs=["bold"])
        print(INFO_MSG.format(msg))
        log(f"terminating {self.printName}")
                
    def __repr__(self):
        return f"<Contract {self.name} @ {self.address}>"
