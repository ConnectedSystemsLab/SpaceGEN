from joint_dst import *
from constants import *
from collections import defaultdict, deque
import os
from joint_dst import CorrelationDistribution
import json
from pfd import *

MIL = 1000000

class CorrelatedTraceGenerator():
    """
    Implementation of correlated trace generator
    1. Load all pFDs
    2. Load Pc
    3. Load everyline of traffic
    4. Input timestamp related stuffs
    """
    def __init__(self, pathCorrelation, generatorMeta, outputPath):
        # Load correlation joint probability
        print("Load correlation probability distribution")
        self.__pcDist = CorrelationDistribution(pathCorrelation, generatorMeta)
        print("Correlation probability distribution loaded")

        # Load all pFDs
        self.iat_gran = 200
        self.sd_gran  = 200000
        self.__pFDs = []
        self.__objIDCounter = 0
        with open(generatorMeta, "r") as f:
            generatorMeta = json.load(f)
        for idx, d in enumerate(generatorMeta):
            print(f"Loading pFD {idx + 1}/{len(generatorMeta)}")
            pfd = PFD()
            with open(d["pfd"], "rb") as f:
                pfd.read_from_file(f, self.iat_gran, self.sd_gran)
            pfd.setupSampling()
            pfd.setupPopularityBasedStackDistance()
            self.__pFDs.append(pfd)
        self.__traceLocNum = len(generatorMeta)
        os.makedirs(outputPath, exist_ok=True)
        self.__outputPath = outputPath
        self.__traceFD = [open(os.path.join(outputPath, f"trace_{i}.txt"),"w") for i in range(self.__traceLocNum)]
        self.__requestRate = np.array([self.__pFDs[i].no_reqs for i in range(self.__traceLocNum)])
        self.__requestRate = self.__requestRate / np.sum(self.__requestRate)
        print(f"Request rate: {self.__requestRate}")
        self.__generationRateCounter = [0 for _ in range(self.__traceLocNum)]
        self.__ketToDel = [[] for _ in range(self.__traceLocNum)]
        self.__mixingTime = 1 * MIL / min(self.__requestRate)
        self.__mixingTime = int(self.__mixingTime)
        print(f"Mixing time: {self.__mixingTime}")
    
    def run(self, traceSize):
        traceSize = int(traceSize / min(self.__requestRate))
        traceList = [[] for _ in range(self.__traceLocNum)]
        sizeList = [{} for _ in range(self.__traceLocNum)]
        popList = [{} for _ in range(self.__traceLocNum)]
        totalSizeList = [0 for _ in range(self.__traceLocNum)]
        maxSD = [int(self.__pFDs[i].sd_keys[-1]) for i in range(self.__traceLocNum)]
        # Initializing
        iterCnt = 0
        maxSDIdx = np.argmax(maxSD)
        while True:
            iterCnt += 1
            distAll = self.__pcDist.sampleForAll(1000)
            for dist in distAll:
                objSize = dist[-1]
                for i in range(self.__traceLocNum):
                    if dist[i] != 0:
                        traceList[i].append(self.__objIDCounter)
                        popList[i][self.__objIDCounter] = dist[i] 
                        sizeList[i][self.__objIDCounter] = objSize 
                        totalSizeList[i] += objSize 
                self.__objIDCounter += 1


            if iterCnt % 5000 == 0:
                print(f"Initializing progress {totalSizeList[maxSDIdx] * 100 / maxSD[maxSDIdx] :2f}%")
            if all([totalSizeList[i] > maxSD[i] for i in range(self.__traceLocNum)]):
                break
        
        # Creating trees
        currs = []
        roots = []
        for idx in range(self.__traceLocNum):
            trace, _ = self.__gen_leaves(traceList[idx], sizeList[idx], None)
            st_tree, lvl = self.__generate_tree(trace)

            root = st_tree[lvl][0]
            root.is_root = True
            curr = st_tree[0][0]
            currs.append(curr)
            roots.append(root)

        # Initialize traces
        c_traces = [[] for _ in range(self.__traceLocNum)]
        req_counts = [defaultdict(int) for _ in range(self.__traceLocNum)]
        debug = open(os.path.join(self.__outputPath, "debug_cache_mix.txt"), "w")
        for i in range(traceSize + self.__mixingTime):
            for locationIdx in range(self.__traceLocNum):
                if self.__generationRateCounter[locationIdx] >= 1:
                    self.__generationRateCounter[locationIdx] -= 1
                    root, curr = self.generateOneTrace(currs[locationIdx], roots[locationIdx], 
                                            popList[locationIdx], self.__pFDs[locationIdx],
                                            c_traces[locationIdx], req_counts[locationIdx],
                                            maxSD[locationIdx], traceList,
                                            popList, sizeList, roots, debug, locationIdx,
                                            i <= self.__mixingTime) 
                    roots[locationIdx] = root
                    currs[locationIdx] = curr
                self.__generationRateCounter[locationIdx] += self.__requestRate[locationIdx]
            # print(c_traces)
            if i % 500000 == 0:
                if i < self.__mixingTime:
                    print(f"Mixing progress {i * 100 / self.__mixingTime:2f}%")
                else:
                    for idx, trace in enumerate(c_traces):
                        for objID in trace:
                            # print(f"objID {objID}")
                            self.__traceFD[idx].write(f"0:{objID}:{sizeList[idx][objID]}\n")
                        trace.clear()
                        for delID in self.__ketToDel[idx]:
                            del popList[idx][delID]
                            del sizeList[idx][delID]
                            del req_counts[idx][delID]
                            self.__ketToDel[idx].clear()
                    print(f"Generation progress {i * 100 / traceSize :2f}%")

        debug.close()
        for idx, trace in enumerate(c_traces):
            for objID in trace:
                self.__traceFD[idx].write(f"0:{objID}:{sizeList[idx][objID]}\n")
            self.__traceFD[idx].close()
            trace.clear()
        # for epoch in range(traceSize):
        #     # For every location generates one trace
        #     for locationidx in range(self.__traceLocNum):
        #         objID, pop, sz = self.__sample(locationidx, self.__pops[locationidx], self.__popszs[locationidx], correlationbuffer)
    
    def __gen_leaves(self, trace, sizes: dict, popularities: dict, printBox = None, items=None, initial_times=None):
        """
        Set up objects in cache as B+ tree leaves
        """
        total_sz = 0    
        st_tree  = defaultdict(list)

        trace_list = []
        seen_ele   = defaultdict()

        trace_length = len(trace)
        
        for i in range(len(trace)):
            oid = int(trace[i])
            n   = node(oid, sizes[oid])
            n.set_b()
            
            if items != None:

                items[oid] = n
                total_sz  += sizes[oid]

                if initial_times != None:                
                    n.last_access = initial_times[oid]
            
            trace_list.append(n)

            if i%100000 == 0:
                print("Representing the cache as leaves of a tree ... ", int((float(i)*100)/trace_length), "% complete")

                if printBox != None:
                    printBox.setText("Representing the cache as leaves of a tree ... " + str(int((float(i)*100)/trace_length)) + "% complete")


                
        return trace_list, total_sz

    def __generate_tree(self, trace_list, printBox=None):

        lvl     = 0
        st_tree = defaultdict(list)
        st_tree[lvl] = trace_list

        while len(st_tree[lvl]) > 1:

            print("Creating tree, parsing level: ", lvl)

            if printBox != None:
                printBox.setText("Creating tree, parsing level: " + str(lvl))
            
            for i in range(int(len(st_tree[lvl])/2)):

                n1  = st_tree[lvl][2*i]
                n2  = st_tree[lvl][2*i+1]        
                p_n = node("nl", (n1.s*n1.b + n2.s*n2.b))

                n1.set_parent(p_n)
                n2.set_parent(p_n)
                
                p_n.add_child(n1)
                p_n.add_child(n2)
                p_n.set_b()        

                st_tree[lvl+1].append(p_n)
                    
            if len(st_tree[lvl]) > 2*i+2:

                n3  = st_tree[lvl][2*i+2]
                n3.set_b()
                p_n = st_tree[lvl+1][-1]
                n3.set_parent(p_n)
                p_n.add_child(n3)
                p_n.s += n3.s * n3.b
                            
            lvl += 1

        return st_tree, lvl
    
    def generateOneTrace(self, curr, root, popularities, pfd, c_trace, req_count, MAX_SD,
                         traceList, popList, sizeList, rootList, debug, locIdx, mixing):
        pp = popularities[curr.obj_id]

        if pp > 1:
            sz = curr.s

            ### modify and generalize this.
            if pp > 50 and pp <= 500:
                pass
            elif pp > 500 and pp <= 5000:
                pp = int(pp/50)*50
            elif pp > 5000:
                pp = int(pp/100)*100
        
            ### modify and generalize this.        
            if sz <= 500:
                sz_ = int(round(sz/10))*10
                if sz <= 0:
                    sz_ = 1
            elif sz <= 1000:
                sz_ = int(round(sz/100))*100
            elif sz <= 10000:
                sz_ = int(round(sz/1000))*1000
            elif  sz <= 100000:
                sz_ = int(round(sz/10000))*10000
            else:
                sz_ = int(round(sz/100000))*100000
            
            req_k = str(pp) + ":" + str(sz_)                
            sd = pfd.sampleStackDistanceGivenPopularity(req_k)                
            if sd >= root.s:
                sd = root.s - 1
            elif sd < 0:
                sd = root.s - 1
            # sampled_fds.append(sd) Never used?
        else:
            sd = 0
            
        n = node(curr.obj_id, curr.s)
        n.set_b()
        if not mixing:
            c_trace.append(curr.obj_id)
        
        req_count[curr.obj_id] += 1
        # obj_seen[curr.obj_id]   = 1 not used?
        
        inserted_at_top = False
        
        if req_count[curr.obj_id] >= popularities[curr.obj_id]:
            self.__ketToDel[locIdx].append(curr.obj_id)
            
            while root.s < MAX_SD:
                distAll = self.__pcDist.sampleForAll(1000)
                for dist in distAll:
                    objSize = dist[-1]
                    for i in range(self.__traceLocNum):
                        if dist[i] != 0:
                            traceList[i].append(self.__objIDCounter)
                            popList[i][self.__objIDCounter] = dist[i] 
                            sizeList[i][self.__objIDCounter] = objSize 
                            n = node(self.__objIDCounter, objSize)
                            n.set_b()       
                            descrepency, x, y = rootList[i].insertAt(rootList[i].s - 1, n, 0, None, debug)
                    
                            if n.parent != None:
                                rootList[i] = n.parent.rebalance(debug)
                    self.__objIDCounter += 1
        else:
            
            try:
                descrepency, land, o_id = root.insertAt(sd, n, 0, curr.id, debug)
                if o_id == curr.obj_id:
                    inserted_at_top = True
            except:
                print("sd : ", sd, root.s)

            if n.parent != None:
                root = n.parent.rebalance(debug)
                                    
        if inserted_at_top == False:
            next, success = curr.findNext()
            while (next != None and next.b == 0) or success == -1:
                next, success = next.findNext()

            del_nodes = curr.cleanUpAfterInsertion(sd, n, debug)
            curr = next
        else:
            del_nodes = curr.cleanUpAfterInsertion(sd, n, debug)
            curr = n
        
        return root, curr