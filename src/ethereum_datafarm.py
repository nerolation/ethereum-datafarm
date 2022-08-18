from utils import *
import requests
import json
from web3 import Web3
from multiprocessing import Process, cpu_count, connection
import pandas as pd
from eth_abi import decode as abi_decode

SLOW_DOWN = 1 # seconds to wait between api calls
STORAGE_THRESHOLD = 1e4

class Farm():
    def __init__(self):
        print_start()
        self.contracts = list()
        
    def load_contracts(self):
        for c in load_all():
            self.contracts.append(Contract(*c))
    
    def farm(self):
        
        print(INFO_MSG.format("Start farming..."))
        
        try:
            cpus = cpu_count()-1
            trs = int(len(self.contracts)/cpus)
            tranches={}
            for i in range(cpus):
                if i == cpus-1: tranches[i] = self.contracts[trs*i:]
                else: tranches[i] = self.contracts[trs*i:trs*(i+1)]
            processes = []
            for i in range(cpus):
                processes.append(Process(target = self.split_tasks, args=tuple([tranches[i]])))
                processes[-1].start()
            connection.wait(p.sentinel for p in processes)
        except KeyboardInterrupt:
            print("Safely terminating...\n")
            if len(processes) > 0:
                for p in processes:
                    p.terminate()
                    
                
    def split_tasks(self, c):
        for contract in c:
            msg = colored(f"Start parsing {contract}", "green", attrs=["bold"])
            print(INFO_MSG.format(msg))
            contract.scrape()
    
    

class Contract():
    def __init__(self, address, name, method, startBlock, chunksize):
        self.address = Web3.toChecksumAddress(address)
        self.name = name.lower()
        self.method = method
        self.topic0 = get_method_from_canonical_expression(self.method)
                
        newStartBlock, newStartTx = check_custom_start(self.name)
        if newStartBlock:
            self.startBlock = newStartBlock
            self.startTx = newStartTx
            self.run = False
            msg = "{} ({}) {}".format(self.address, self.name, colored("starting at last known location", "green"))
            msg2 = "{} ({}) blockheight set to {:,.0f}".format(self.address, self.name, self.startBlock)
            msg3 = "{} ({}) starting after tx {}".format(self.address, self.name, self.startTx[:-40]+"...")
            
            print(INFO_MSG.format(msg))
            print(INFO_MSG.format(msg2))
            print(INFO_MSG.format(msg3))
            
        else:
            self.startBlock = int(startBlock)
            self.startTx = None
            self.run = True
        
        self.fromblock = self.startBlock
        self.chunksize = int(chunksize)
        
        self.simpleMethod = method.split("(")[0].lower()
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
                if results == "no records found":
                    self.log_nothing_found()
                    results = [0]
                    self.avgNrOfPages.append(1) 
                    self.avgNrOfPages = self.avgNrOfPages[-10:]
                    continue
                
                if results == "page limit reached":
                    self.startBlock, self.startTx = check_custom_start(self.name)
                    self.CACHE = pd.DataFrame(columns=self.columns)
                    self.run = False
                    self.chunksize = int(self.chunksize/10)
                    if self.chunksize < 1: self.chunksize = 1
                    self.avgNrOfPages.append(10)
                    self.avgNrOfPages = self.avgNrOfPages[-10:]
                    results = [0]
                    continue
                    
                self.parse_results(results)
                
                self.log_progress(len(results), page)

                page += 1
                
            if page - 1 >= 1:
                self.avgNrOfPages.append(page - 1) 
                self.avgNrOfPages = self.avgNrOfPages[-10:]
            else:
                self.avgNrOfPages.append(1.5) 
                self.avgNrOfPages = self.avgNrOfPages[-10:]
            
            # Update latest block ever 600 seconds
            if (datetime.now()-self.timeSinceLatestBlock).total_seconds() > 6e2:
                self.LATEST_BLOCK = latest_block()
            
            self.fromblock =  self.toblock + 1
            
        if len(self.CACHE) > 0 and self.run:
            self.log_storage()
            dump_cache_to_disk(self.CACHE, self.storageLocation.format(self.fileCounter), self.name)
            self.CACHE = pd.DataFrame(columns=self.columns)
            self.fileCounter += 1
            
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
                dump_cache_to_disk(self.CACHE, self.storageLocation.format(self.fileCounter), self.name)
                self.CACHE = pd.DataFrame(columns=self.columns)
                self.fileCounter += 1
            
            if self.startTx == transactionHash:
                msg = f"contract @ {self.address} ({self.name}) last known tx found; start parsing..."
                print(INFO_MSG.format(msg))
                self.run = True
                self.CACHE = pd.DataFrame(columns=self.columns)
                
            
            
    def make_row(self, *args):
        self.CACHE.loc[len(self.CACHE)] = args[0]
        
    def try_adapting_chunksize(self):
        if sum(self.avgNrOfPages)/len(self.avgNrOfPages) > 2:
            cs = self.chunksize
            self.chunksize = int(self.chunksize/5)
            self.log_chunk_size(cs, "decreasing")
            
        elif sum(self.avgNrOfPages)/len(self.avgNrOfPages) <= 1:
            cs = self.chunksize
            if self.chunksize < 5: factor = 1.5 
            else: factor = 1.2
            self.chunksize = int(self.chunksize*factor)
            self.log_chunk_size(cs, "increasing")
            self.avgNrOfPages.append(1.5)
            self.avgNrOfPages = self.avgNrOfPages[-10:]
            
        if self.chunksize <= 1:
            self.chunksize = 2  
        
    def log_progress(self, len_result, page):
        msg = "parsing {:<20} | ".format(self.printName[:17]+"...") \
            + "{:>10,.0f}-{:>10,.0f} | ".format(self.fromblock, self.toblock) \
            + colored("{:>4.0f}/1000".format(len_result), "green") +  f" | Page {page}" \
            + " | cs {:>6,.0f} | cache {:>6,.0f}".format(self.chunksize, len(self.CACHE))
        
        print(INFO_MSG.format(msg))
    
    def log_chunk_size(self, old_size, op):
        avg = sum(self.avgNrOfPages)/len(self.avgNrOfPages)
        msg = "{} chunk size        | {}".format(op, self.printName[:17]+"...") \
            + " {:>5,.0f} --> {:<5,.0f} with avg. pages of {:.2f}".format(old_size, self.chunksize, avg) 
        print(INFO_MSG.format(msg))
        
    def log_nothing_found(self):
        msg = colored("no result", "red") + " {:^20}   | ".format(colored(self.printName[:17]+"..."), "red") \
              + "{:>10,.0f}-{:>10,.0f} | {:>9}".format(self.fromblock, self.toblock, "cs " + str(self.chunksize))
        print(INFO_MSG.format(msg))
    
    def log_storage(self):
        msg = colored("storing ", "green") + f"{self.printName} with " \
                "{:,.0f} entries @ ".format(len(self.CACHE)) \
                + colored(f"{self.storageLocation.format(self.fileCounter)}", "green")
        print(INFO_MSG.format(msg))
        
    def log_end(self):
        msg = colored(f"terminating {self.printName}", "green", attrs=["bold"])
        print(INFO_MSG.format(msg))
            
            
    
    def __repr__(self):
        return f"<Contract {self.name} @ {self.address}>"