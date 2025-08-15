import os
from pathlib import Path
import concurrent.futures
from collections import defaultdict
import json

def discretize_size(sz):
    if sz <= 500:
        sz = int(round(sz/10))*10
        if sz <= 0:
            sz = 1
    elif sz <= 1000:
        sz = int(round(sz/100))*100
    elif sz <= 10000:
        sz = int(round(sz/1000))*1000
    elif  sz <= 100000:
        sz = int(round(sz/10000))*10000
    else:
        sz = int(round(sz/100000))*100000
    return sz

def discretize_pop(pop):
    if pop > 50 and pop <= 500:
        pop = int(pop/10)*10
    elif pop > 500 and pop <= 5000:
        pop = int(pop/50)*50
    elif pop > 5000:
        pop = int(pop/100)*100
    return pop

def process_file(file_name):
    print(f"Process {file_name}")
    metadata = {}
    with open(file_name, 'r') as f:
        for line in f:
            _, obj_id, obj_size = line.split(':')
            obj_size = int(obj_size)
            if obj_id not in metadata:
                metadata[obj_id] = [obj_size, 0]
            metadata[obj_id][-1] += 1

    for k in metadata.keys():
        obj_size, pop = metadata[k]
        metadata[k] = (discretize_size(obj_size / 1000), discretize_pop(pop))
    return metadata 

def worker(file_names):
    result_dict = {}
    for file_name in file_names:
        result = process_file(file_name)
        result_dict[file_name] = result
    return result_dict

def process_files_in_parallel(file_list):
        
    worker_num = 8 
    file_chunks = [[] for _ in range(worker_num)]
    for i, file_name in enumerate(file_list):
        file_chunks[i % worker_num].append(file_name)
    
    # Using ProcessPoolExecutor to create 8 subprocesses
    with concurrent.futures.ProcessPoolExecutor(max_workers=worker_num) as executor:
        results = executor.map(worker, file_chunks)
    
    # Merge all dictionaries from each subprocess
    final_result = defaultdict()
    for result_dict in results:
        final_result.update(result_dict)
    
    return dict(final_result)
    
if __name__ == '__main__':
    os.chdir(os.path.dirname(Path(__file__)))
    trace_metadata = os.path.join(os.path.dirname(Path(__file__).parent), 'user.json')
    with open(trace_metadata, 'r') as f:
        trace_metadata = json.load(f)

    trace_files = [t[-1] for t in trace_metadata]
    # Process all traces
    result_global = process_files_in_parallel(trace_files) 
    trace_keys = trace_files
    seen = set()
    distribution = {} 
    for i, trace in enumerate(trace_keys):
        current = result_global[trace]
        for k in current.keys():
            if k in seen:
                continue
            seen.add(k)
            # initialize pop array
            pop = [0 for _ in range(len(trace_keys))]
            obj_size, obj_cnt = current[k]
            pop[i] = obj_cnt 
            for j in range(len(trace_keys)):
                if i != j:
                    if k in result_global[trace_keys[j]]:
                        pop[j] = result_global[trace_keys[j]][k][-1]
            pop += [obj_size] 
            distribution.setdefault(tuple(pop), 0)
            distribution[tuple(pop)] += 1
    total_freq = sum(distribution.values())

    prob_dict = {k: v / total_freq for k, v in distribution.items()}

    sorted_prob = sorted(prob_dict.items(), key=lambda x: x[-1], reverse=True)
    with open("index_keys.txt", "w") as f:
        json.dump(trace_files, f)
    with open("correlation.txt", "w") as f:
        for key, probability in sorted_prob:
            line = ",".join(map(str, key)) + f", {probability:.12f}\n"
            f.write(line)