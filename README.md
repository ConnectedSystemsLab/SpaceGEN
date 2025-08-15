# SpaceGEN
SpaceGEN is a tool used to generate content popularity correlated synthetic CDN traces. It yields high fidelity traces that satisfy cache-level and object-level properties comparing to the original traces. SpaceGEN originated from SIGCOMM 2025 paper: StarCDN: Moving Content Delivery Networks to Space.
```
@inproceedings{karger1997consistent,
  title={StarCDN: Moving Content Delivery Networks to Space},
  author={William X. Zheng, Aryan Taneja, Maleeha Masood, Anirudh Sabnis, Ramesh K. Sitaraman, and Deepak Vasisht},
  series={SIGCOMM 2025},
  year={2025}
}
```

## 1. Installation
This repository uses python3.9+. To install dependendies run:
`pip install -r requirements.txt`
## 2. Run SpaceGEN 

### 2.1 Prepare Statistics
SpaceGEN requires two statistics computed from the production traces: popularity footprint descriptor (pFD) and global popularity distribution (GPD). We have provided these two statistics for web, video, and donwload traffics from Akamai's production traces in `./data`. Alternatively, users can also generate their own pFD and GPD. The generation tool for pFD can be found in JEDI's repository [link](). The script used to compute GPD can be found in `./scripts`.

### 2.2 Generating Synthetic Traces
Import the trace generator module and run the trace generation using the codes below (example codes and input file available in ./examples). The parameter passed into the run function is the minimum objects that will be generated for the server with least traffic pressure.
```
gen = CorrelatedTraceGenerator("PATH_TO_CORRELATION", "input.json", "./OUTPUT/")
gen.run(60000000)
```
### 2.3 Output
The generator is going to output several synthetic traces in the order of `input.json`. In each trace, every request is in the format of `time,id,size(KB)`. We cannot release the detailed traffic rate from the original traces due to sonsitivity. 

## 3. References and Previous Works
Sundarrajan, Aditya, Mingdong Feng, Mangesh Kasbekar, and Ramesh K. Sitaraman. "[Footprint descriptors: Theory and practice of cache provisioning in a global cdn](https://groups.cs.umass.edu/ramesh/wp-content/uploads/sites/3/2019/12/Footprint-Descriptors-Theory-and-Practice-of-Cache-Provisioning-in-a-Global-CDN.pdf)." In Proceedings of the 13th International Conference on emerging Networking EXperiments and Technologies 2017.

Anirudh Sabnis, Ramesh K. Sitaraman. "[TRAGEN: A Synthetic Trace Generator for Realistic Cache Simulations](https://groups.cs.umass.edu/ramesh/wp-content/uploads/sites/3/2019/12/Footprint-Descriptors-Theory-and-Practice-of-Cache-Provisioning-in-a-Global-CDN.pdf)." In Proceedings of the 21st ACM Internet Measurement Conference 2021.

Anirudh Sabnis, Ramesh K. Sitaraman. "[JEDI: Model-driven trace generation for cache simulations](https://groups.cs.umass.edu/ramesh/wp-content/uploads/sites/3/2022/11/JEDI.pdf)." In Proceedings of the 22st ACM Internet Measurement Conference 2022.