from collections import defaultdict
from random import choices
from util import *
import numpy as np
import bisect
import copy
import json
from kdtree import NearestQueueMap

    
class SZ_dst:
    def __init__(self, i_file, min_val, max_val):
        f = open(i_file, "r")
        self.all_keys = defaultdict(int)
        l = f.readline()
        sum_count = 0

        total_pr = 0
        for l in f:
            l = l.strip().split(" ")

            if len(l) == 1:
                continue
            else:
                key = int(float(l[0]))
                val = float(l[1])
                if key >= min_val and key <= max_val:
                    self.all_keys[key] += val                                
                    total_pr += val
                    
        p_keys = list(self.all_keys.keys())
        vals = []
        for k in p_keys:
            vals.append(self.all_keys[k])
        sum_vals = sum(vals)
        vals = [float(x)/sum_vals for x in vals]        
        self.p_keys = p_keys
        self.pr = vals

                
    def sample_keys(self, n):
        return choices(self.p_keys, weights=self.pr,k=n)


class POPULARITY_dst:

    def __init__(self, i_file, min_val, max_val):
        f = open(i_file, "r")
        self.popularities = defaultdict(float)

        l = f.readline()
        key = int(l.strip())
        sum_count = 0

        for l in f:
            l = l.strip().split(" ")
            if len(l) == 1:
                self.popularities[key] = sum_count
                sum_count = 0
                key = int(l[0])
                if key > max_val:
                    break                
            else:
                if key >= min_val:
                    sum_count += float(l[1])

        p_keys = list(self.popularities.keys())
        p_vals = []
        for k in p_keys:
            p_vals.append(self.popularities[k])

        sum_vals = sum(p_vals)
        p_vals   = [float(x)/sum_vals for x in p_vals]

        self.p_keys        = p_keys
        self.probabilities = p_vals


    def sample_keys(self, n):
        return choices(self.p_keys, weights=self.probabilities,k=n)



class POPULARITY_SZ_dst:

    def __init__(self, i_file):

        pop_sz = defaultdict(lambda : defaultdict(int))        
        f = open(i_file, "r")
        
        key = "-"
        keys_cnt = 0

        for l in f:
            l = l.strip().split(" ")
            if len(l) == 1:
                key = int(float(l[0]))
                continue                                
            objs = float(l[1])
            sz   = int(float(l[0]))             
            pop_sz[key][sz] += objs
                
        f.close()
        
        
        self.pop_sz_vals = defaultdict(lambda : list)
        self.pop_sz_prs  = defaultdict(lambda : list)

        sum_n_key = 0

        for key in pop_sz:
            sizes = list(pop_sz[key].keys())
            n_key = key

            self.pop_sz_vals[n_key] = sizes
            sum_n_key += n_key
            prs = []
            for s in sizes:
                prs.append(pop_sz[key][s])
            sum_prs = sum(prs)
            prs = [float(x)/sum_prs for x in prs]
            self.pop_sz_prs[n_key] = prs

        self.sample_each_popularity()


    def sample_each_popularity(self):
        self.samples = defaultdict(list)
        self.sampled_sizes = defaultdict(list)

        for k in self.pop_sz_prs:
            self.sampled_sizes[k] = choices(self.pop_sz_vals[k], weights=self.pop_sz_prs[k], k=10000)

        self.samples_index = defaultdict(int)
        self.popularities = list(self.pop_sz_prs.keys())
        self.popularities.sort()

    def findnearest(self, k):
        ind = bisect.bisect_left(self.popularities, k)
        if ind >= len(self.popularities):
            ind = len(self.popularities) - 1
        return self.popularities[ind]

    def sample(self, k):

        if k not in self.samples_index:
            k = self.findnearest(k)

        curr_index = self.samples_index[k]
        if curr_index >= len(self.sampled_sizes[k]):
            self.sampled_sizes[k] = choices(self.pop_sz_vals[k], weights=self.pop_sz_prs[k], k=10000)
            self.samples_index[k] = 0
            curr_index = 0

        self.samples_index[k] += 1        
        return int(self.sampled_sizes[k][curr_index])




    
class POPULARITY_SZ_dst_backup:

    def __init__(self, i_file):
        f = open(i_file, "r")
        self.pop_sz = defaultdict(lambda: defaultdict(float))
        popularities = defaultdict(float)
        
        l   = f.readline()
        key = int(l.strip())
        sum_count = 0

        sizes = []
        prs = []
        for l in f:
            l = l.strip().split(" ")
            if len(l) == 1:
                sum_prs = sum(prs)
                for i in range(len(sizes)):
                    self.pop_sz[key][sizes[i]] = float(prs[i])/sum_prs
                    
                key = int(float(l[0]))
                sizes = []
                prs  = []
                continue
            else:
                sz = int(float(l[0]))
                pr = float(l[1])
                sizes.append(sz)
                prs.append(pr)
                #self.pop_sz[key][sz]   += pr
                popularities[key] += pr
        f.close()

        ## Overall popularity distribution
        p_keys = list(popularities.keys())
        p_vals = []
        for k in p_keys:
            p_vals.append(popularities[k])

        sum_vals = sum(p_vals)
        p_vals   = [float(x)/sum_vals for x in p_vals]

        self.p_keys        = p_keys
        self.p_vals        = p_vals

        ## Popularity based size distribution
        self.pop_sz_keys = defaultdict(lambda : list)
        self.pop_sz_prs  = defaultdict(lambda : list)
        
        for key in self.pop_sz:
            sizes = list(self.pop_sz[key].keys())
            self.pop_sz_keys[key] = sizes
            prs = []
            for s in sizes:
                prs.append(self.pop_sz[key][s])
            sum_prs = sum(prs)
            prs = [float(x)/sum_prs for x in prs]
            self.pop_sz_prs[key] = prs
        
        self.sample_each_popularity()


    def print_probability(self, p, k):
        print(self.pop_sz[p][k])
        return

        
    def sample_each_popularity(self):
        self.samples = defaultdict(list)
        self.sampled_sizes = defaultdict(list)

        for k1 in self.pop_sz_prs:
            self.sampled_sizes[k1] = choices(self.pop_sz_keys[k1], weights=self.pop_sz_prs[k1], k=10000)

        self.samples_index = defaultdict(int)
        self.popularities = list(self.pop_sz_prs.keys())
        self.popularities.sort()


    def findnearest(self, k):
        ind = bisect.bisect_left(self.popularities, k)
        if ind >= len(self.popularities):
            ind = len(self.popularities) - 1
        return self.popularities[ind]


    def sample(self, k):
        if k not in self.samples_index:
            k = self.findnearest(k)

        curr_index = self.samples_index[k]
        if curr_index >= len(self.sampled_sizes[k]):
            self.sampled_sizes[k] = choices(self.pop_sz_keys[k], weights=self.pop_sz_prs[k], k=10000)
            self.samples_index[k] = 0
            curr_index = 0

        self.samples_index[k] += 1        
        return int(self.sampled_sizes[k][curr_index])

    def sample_keys(self, n):
        return choices(self.p_keys, weights=self.p_vals,k=n)

    

class SampleFootPrint:
    def __init__(self, fd, hr_type, min_val, max_val):
        self.sd_keys = []
        self.sd_vals = []
        self.sd_index = defaultdict(lambda : 0)
        self.SD = defaultdict(lambda : 0)        

        f = open(i_file, "r")
        l = f.readline()
        l = l.strip().split(" ")
        if hr_type == "bhr":
            bytes_miss = float(l[-1])
            bytes_req = float(l[1])
            self.SD[-1] = float(bytes_miss)/bytes_req
        else:
            reqs_miss = float(l[-2])
            reqs = float(l[0])
            self.SD[-1] = float(reqs_miss)/reqs                    
            self.sd_index[-1] = 0

        total_pr = 0
        for l in f:
            l = l.strip().split(" ")
            sd = int(l[1])
            self.SD[sd] += float(l[2])        
            total_pr += float(l[2])
            
        self.sd_keys = list(self.SD.keys())
        self.sd_keys.sort()

        i = 1
        curr_pr = 0
        self.sd_pr = defaultdict()

        for sd in self.sd_keys:
            self.sd_vals.append(self.SD[sd])
            curr_pr += self.SD[sd]

            if sd >= 0:
                self.sd_pr[sd] = float(curr_pr - self.SD[-1])/(1 - self.SD[-1])

            self.sd_index[sd] = i
            i += 1            
                            
    def sample_keys(self, obj_sizes, sampled_sds, n):
        return choices(self.sd_keys, weights = self.sd_vals, k = n)


    def findPr(self, sd):
        return self.sd_pr[sd]

class CorrelationDistribution:
    """
    Implementation of a n dimensional correlation probability distribution
    Key functionality is to allow sampling
    """
    def __init__(self, pathPc, pathMeta):
        # Read metadata first
        with open(pathMeta, 'r') as f:
            self.__meta = json.load(f)
            self.__traceNum = len(self.__meta)
        self.__dist = {}
        self.__distDict = {}
        # self.__sizeMarginalDist = [defaultdict(lambda :defaultdict(float)) for _ in range(self.__traceNum)] 
        # self.allPossiblePopularity = [[] for _ in range(self.__traceNum)]
        # self.allPossibleSize = [[] for _ in range(self.__traceNum)]
        self.__kdPopSz: list[NearestQueueMap] = [NearestQueueMap() for _ in range(self.__traceNum)]
        with open(pathPc, 'r') as f:
            for line in f:
                tokens = line.split(',') # first n tokens are popularity for traces, the last two are size, probability
                objSize = int(tokens[-2])
                prob = float(tokens[-1])
                cur_dict: dict = self.__dist
                self.__distDict[tuple([int(x) for x in tokens[:self.__traceNum]] + [objSize])] = prob
                for i in range(self.__traceNum):
                    # Interate over each token to compute joint dist and marignal dist
                    # Joint dist
                    cur_dict.setdefault(int(tokens[i]), {})
                    cur_dict = cur_dict[int(tokens[i])]
                    self.__kdPopSz[i].add_point(int(tokens[i]), objSize, None, auto_update=False)
                    # Marginal dist
                    # self.__sizeMarginalDist[i][int(tokens[i])][objSize] += prob 
                # Nested dictionary in order of p1, p2, ..., pn, size
                cur_dict[objSize] = prob
        self.__conditionalDist = [defaultdict(lambda :defaultdict(lambda: defaultdict(lambda: defaultdict(float)))) for _ in range(self.__traceNum)] 
        for tree in self.__kdPopSz:
            tree._update_kdtree()
        self.__distKeyList = list(self.__distDict.keys())
        self.__distValList = list(self.__distDict.values())
    
    def sampleForAll(self, k: int):
        
        return random.choices(self.__distKeyList, self.__distValList, k=k)
    
    def __dfsExploreConditionalProb(self, curIdx, targetIdx, targetPop, targetSize, curDict, curList):
        """
        DFS explore for conditional prob with targetIdx, targetPop, targetSize
        """
        if curIdx == self.__traceNum:
            if targetSize not in curDict:
                # Has zero discrete prob
                return
            self.__conditionalDist[targetIdx][targetPop][targetSize][tuple(curList)] = curDict[targetSize]
            return
        if curIdx == targetIdx:
            if targetPop not in curDict.keys():
                # Has zero discrete probability
                return
            self.__dfsExploreConditionalProb(curIdx + 1, targetIdx, targetPop, targetSize, curDict[targetPop], curList)
        else:
            for k in curDict.keys():
                # Explore all path
                self.__dfsExploreConditionalProb(curIdx + 1, targetIdx, targetPop, targetSize, curDict[k], curList + [k])

    def sample(self, idx, pop, size, k):
        """
        Method for sampling conditional correlation distribution of index idx with given pop and size
        Use lazy evaluation to compute
        @return
            tuple of length (n - 1)
        """
        nearestPop, nearestSz = self.__kdPopSz[idx].get_nearest_non_empty(pop, size)
        pop = int(nearestPop)
        size = int(nearestSz)
        if pop not in self.__conditionalDist[idx] or size not in self.__conditionalDist[idx][pop]:
            # Compute the conditional distribution
            self.__dfsExploreConditionalProb(0, idx, pop, size, self.__dist, []) 
        # Sample a distribution
        curDist = self.__conditionalDist[idx][pop][size] # Distribution that maps tuple of popularities to prob
        # Hack that might be false
        if len(curDist) == 0:
            return tuple([[0 for _ in range(self.__traceNum - 1)]])

        # print(f"{idx}, {pop}, {size}")
        # print("Len: ", len(curDist))
        # print(curDist.keys())
        # print(curDist.values())
        # print(self.__sizeMarginalDist[idx][pop][size])

        return choices(list(curDist.keys()), list(curDist.values()), k=k)

    @property 
    def conditionalDist(self):
        return self.__conditionalDist

    @property 
    def dist(self):
        return self.__dist