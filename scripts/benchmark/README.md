## Python scripts to benchmark the Metadata Stores

### Currently, holds the script for benchmarking read performance of Zookeeper

Requires: `Python==3.9.7+`

To get full set of features, run: `python zk_benchmark.py --help`

#### Examples:

Benchmark get_children for 5000 nodes: `python zk_benchmark.py -n 5000`

Benchmark for 5k nodes using 12 threads for dataloading: `python zk_benchmark.py -n 5000 --num_threads=12`

Use alternative data loader implementations:
- Single Threaded: `python zk_benchmark.py -n 5000 --dl_mode=st`
- Multi Threaded: `python zk_benchmark.py -n 5000 --dl_mode=mt --num_threads=4`
- Multi Process (Experimental): `python zk_benchmark.py -n 5000 --dl_mode=mp`

Specify the sampling for the measurements: `python zk_benchmark.py -n 5000 --measure_samples=100`

Output is reported as Min | Max | Mean over the samples

#### TODO:
- Use multi feature via Transaction for faster dataloading
- get_node_data measurement
- Fix issue with Multi Process dataloader where it does not respect the SIGTERM and keeps on running background tasks.
