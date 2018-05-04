# Benchmark
A benchmark tool used for performance evaluation of astronomy pipeline tasks in a container environment. Includes a simulation package to create a standardised astronomy data set, and an environment profiling suite that details the hardware, operating system and virtual environment typically used to execute pipeline tasks, along with runtime and resource profiling of the tasks. Example pipeline tasks include NRAO CASA tclean and WSClean.

## Prerequisites

- Python 2.7
- Psutil

## Running benchmark tool

```
import Benchmark from benchmark

mybenchmark = Benchmark()

sn = 'image_script_tclean.py'
cp = 'path_to_container/jupyter-casa-0.2.0.simg'
desc = 'tclean on jupyter-casa'
exec_ = 'casa --nologger --log2term --nogui -c'
tid = 'tclean'

mybenchmark.execute(script_name=sn, container_path=cp, testid=tid, exec_path=exec_, description=desc)
mybenchmark.write_to_csv()
```

See example notebook for detailed instructions


## Acknowledgments
Dask Development Team (2016). Dask: Library for dynamic task scheduling
URL http://dask.pydata.org
