from popularity_trace_generator import CorrelatedTraceGenerator
gen = CorrelatedTraceGenerator("PATH_TO_CORRELATION", "input.json", "PFDs_Download_Traffic/OUTPUT/")
gen.run(60000000)