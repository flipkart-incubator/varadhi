from typing import Generator
from kazoo.client import KazooClient
from tqdm import tqdm
from time import perf_counter
from random import randbytes
import string
import random
import concurrent.futures

# typedef our generator
NodeGenerator = Generator[tuple[str, bytes], None, None]

# Util functions
def random_string(size: int) -> str:
    letters = string.ascii_letters + string.digits + "_"
    res = "".join(random.choices(letters, k=size))
    return res
    
def randstr(low: int, high: int) -> str:
    size = random.randrange(low, high)
    return random_string(size)

# Generator which generates the child node data based on params
def node_generator(num: int, name="", min_name_len=-1, max_name_len=-1, data_size_bytes=1024) -> NodeGenerator:
    gen = 0
    while gen < num:
        name_res = ""
        if len(name) > 0:
            name_res = "{}_{}".format(name, str(gen + 1))
        else:
            name_res = randstr(min_name_len, max_name_len)
        yield (name_res, randbytes(data_size_bytes))
        gen += 1

class catchtime:
    def __enter__(self):
        self.time = perf_counter()
        return self

    def __exit__(self, type, value, traceback):
        self.time = (perf_counter() - self.time) * 1000
        self.readout = f'Time: {self.time:.3f} ms'
        # print(self.readout)

## Config classes start
class ZkClientConfig:
    def __init__(self, hosts: str = "localhost:2281", root_node: str = "/benchmark"):
        self.hosts = hosts
        self.root_node = root_node

class DataLoaderConfig:
    def __init__(self, parallelism=5, chunk_size=10000):
        self.parallelism = parallelism
        self.chunk_size = chunk_size

class ChildNodeConfig:
    def __init__(self, number: int, fixed_name: str = "", name_len: int = -1, data_bytes: int = 1024):
        self.number = number
        self.name_len = name_len
        self.fixed_name = fixed_name
        self.data_bytes = data_bytes

class BenchmarkRunConfig:
    def __init__(self, name: str, child_node_config: ChildNodeConfig):
        self.name = name
        self.child_node_config = child_node_config

class BenchmarkConfig:
    def __init__(self, zk_config: ZkClientConfig, runs: list[BenchmarkRunConfig], data_loader_config: DataLoaderConfig):
        self.zk_config = zk_config
        self.root_node = zk_config.root_node
        self.runs = runs
        self.data_loader_config = data_loader_config
## Config classes end

## ZkClientWrapper
class ZkClientWrapper:
    def __init__(self, config: ZkClientConfig, name: str = "default"):
        self.hosts = config.hosts
        self.root_node = config.root_node
        self.name = name
        self.zk = KazooClient(hosts=self.hosts)
    
    def _log(self, message: str):
        print("[{}] Zk Client: {}".format(self.name, message))

    def start(self):
        # self._log("Trying to open connection")
        self.zk.start()
        self.ensure_root_node()

    def stop(self):
        # self._log("Closing connection to zookeeper")
        self.zk.stop()
        self.zk.close()
    
    def __enter__(self):
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        return True

    def ls(self, path="/") -> list[str]:
        return self.zk.get_children(path) or []
    
    def create(self, path: str, data=b''):
        if not self.zk.exists(path):
            self.zk.create(path, value=data, makepath=True)
    
    def ensure_root_node(self):
        self.create(self.root_node)
    
    def clean(self, path: str):
        self.zk.delete(path, recursive=True)
    
    def clean_all(self):
        print("cleanup all in {}".format(self.root_node))
        self.clean(self.root_node)

# Data loader which loads data into Zk in chunked manner
class DataLoader(object):
    def __init__(self, config: DataLoaderConfig, zk_config: ZkClientConfig):
        self.root_node = zk_config.root_node
        self.config = config
        self.zk_client = ZkClientWrapper(zk_config, "data_loader_zk")
        
    def __enter__(self):
        self.zk_client.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.zk_client.stop()
        return True

    def __get_chunked_generators(self, child_node_config: ChildNodeConfig) -> list[tuple[NodeGenerator, int]]:
        rem_size = child_node_config.number
        result = []
        while rem_size > 0:
            gen_size = min(rem_size, self.config.chunk_size)
            result.append((node_generator(
                gen_size, 
                name=child_node_config.fixed_name, 
                min_name_len=child_node_config.name_len, 
                max_name_len=child_node_config.name_len+1, 
                data_size_bytes=child_node_config.data_bytes), gen_size))
            rem_size -= gen_size
        
        return result

    def load_data(self, parent: str, child_node_config: ChildNodeConfig):
        parent_node="{}/{}".format(self.root_node, parent)
        self.zk_client.create(parent_node)

        generators_with_size = self.__get_chunked_generators(child_node_config)
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.config.parallelism) as exec:
            futures_to_id = {exec.submit(self._fill, parent_node, generator, size): id for (id, (generator, size)) in enumerate(generators_with_size)}
            for future in concurrent.futures.as_completed(futures_to_id):
                id = futures_to_id[future]
                try:
                    res = future.result()
                except Exception as exc:
                    print("loader: {} generated an exception: {}".format(id, exc))

    def _fill(self, path: str, child_generator: NodeGenerator, gen_size: int):
        for node in tqdm(child_generator, total=gen_size):
            child_node = "{}/{}".format(path, node[0])
            self.zk_client.create(child_node, data=node[1])

class MultiProcessDataLoader:
    def __init__(self, config: DataLoaderConfig, zk_config: ZkClientConfig):
        self.root_node = zk_config.root_node
        self.config = config
        self.zk_config = zk_config
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        return True

    def __get_chunked_configs(self, child_node_config: ChildNodeConfig) -> list[ChildNodeConfig]:
        rem_size = child_node_config.number
        result = []
        while rem_size > 0:
            gen_size = min(rem_size, self.config.chunk_size)
            result.append(ChildNodeConfig(
                gen_size, 
                fixed_name=child_node_config.fixed_name, 
                name_len=child_node_config.name_len, 
                data_bytes=child_node_config.data_bytes))
            rem_size -= gen_size
        
        return result

    def load_data(self, parent: str, child_node_config: ChildNodeConfig):
        configs = self.__get_chunked_configs(child_node_config)
        with concurrent.futures.ProcessPoolExecutor(max_workers=self.config.parallelism) as exec:
            futures_to_id = {exec.submit(self._process, parent, config): id for (id, config) in enumerate(configs)}
            for future in concurrent.futures.as_completed(futures_to_id):
                id = futures_to_id[future]
                try:
                    res = future.result()
                except Exception as exc:
                    print("process: {} generated an exception: {}".format(id, exc))

    def _process(self, parent: str, child_node_config: ChildNodeConfig):
        with DataLoader(self.config, zk_config=self.zk_config) as loader:
            loader.load_data(parent, child_node_config)

USE_MULTI_PROCESS = True
SHOULD_MEASURE = False
class Benchmark:
    def __init__(self, config: BenchmarkConfig):
        self.config = config
        self.zk_config = config.zk_config
        self.data_loader_config = config.data_loader_config
        self.root_node = config.root_node
    
    def run(self, skip_measure: bool = False, use_multiprocess: bool = False):
        for (idx, run) in enumerate(self.config.runs):
            print("Starting benchmark run: {} [{} of {}]".format(run.name, idx+1, len(self.config.runs)))
            self._run(run, skip_measure, use_multiprocess)
    
    def cleanup(self):
        with ZkClientWrapper(config=self.zk_config, name="cleanup_zk") as client:
            client.clean_all()
    
    def _run(self, run_config: BenchmarkRunConfig, skip_measure: bool, use_multiprocess: bool):
        path = self._get_parent_path(run_config.name)
        measure_samples = 5

        ## DataLoading step
        print("Loading nodes under path:", path)
        with self._get_data_loader(self.data_loader_config, self.zk_config, use_multiprocess) as loader:
            loader.load_data(run_config.name, run_config.child_node_config)

        ## Measure step
        if not skip_measure:
            with ZkClientWrapper(config=self.zk_config, name="measure_zk") as client:
                print("Num child nodes under path {}: {}".format(path, len(client.ls(path))))
                with catchtime() as t:
                    for _ in range(measure_samples):
                        client.ls(path)
                print("Latency get_children on path {}: {} ms".format(path, t.time / measure_samples))
    
    def _get_data_loader(self, data_loader_config: DataLoaderConfig, zk_config: ZkClientConfig, use_multiprocess: bool = False):
        if use_multiprocess:
            print("Using multi processing data loader")
            return MultiProcessDataLoader(data_loader_config, zk_config)
        print("Using multi threaded data loader")
        return DataLoader(data_loader_config, zk_config)

    def _get_parent_path(self, parent_name: str) -> str:
        return "{}/{}".format(self.root_node, parent_name)

import argparse
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--hosts", help="zookeeper host:port (comma separated if multiple)", default="localhost:2281")
    parser.add_argument("-r", "--root_path", help="Root path under which benchmark will run", default="/benchmark")
    parser.add_argument("--cleanup", help="cleanup znodes in root path", action="store_true")
    parser.add_argument("--skip_measure", help="should skip measurements", action="store_true")
    parser.add_argument("--use_mp_dl", help="should use multiprocess dataloader", action="store_true")

    parser.add_argument("--num_threads", help="threads/processes to use for dataloading", type=int, default=6)
    parser.add_argument("--data_chunk", help="split dataloading to specified chunks", type=int, default=10_000)
    parser.add_argument("-n", "--num_child_nodes", help="number of child nodes to create", type=int)
    parser.add_argument("-p", "--parent", help="name of parent node under which to create child nodes", type=str, default="parent")
    parser.add_argument("-l", "--name_len", help="length of child node name", type=int, default=30)
    parser.add_argument("-d", "--data_size_bytes", help="data size in bytes for each child node", type=int, default=1024)
    
    args = parser.parse_args()

    print("Starting up with config:", args)

    data_loader_config = DataLoaderConfig(parallelism=args.num_threads, chunk_size=args.data_chunk)
    zk_config = ZkClientConfig(hosts=args.hosts, root_node=args.root_path)
    child_config = ChildNodeConfig(args.num_child_nodes, name_len=args.name_len, data_bytes=args.data_size_bytes)
    run = BenchmarkRunConfig(args.parent, child_config)
    config = BenchmarkConfig(zk_config=zk_config, runs=[run], data_loader_config=data_loader_config)

    benchmark = Benchmark(config)

    if args.cleanup:
        benchmark.cleanup()
    else:
        if not args.num_child_nodes:
            print("ERR: please specify number of child nodes to create")
            exit()
        benchmark.run(args.skip_measure, args.use_mp_dl)
