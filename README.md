## Building an Elastic Block Storage over EBOFs Using Shadow Views

This is the codebase for paper [Building an Elastic Block Storage over EBOFs Using Shadow Views](https://www.usenix.org/conference/nsdi25/presentation/jiang) at [NSDI '25](https://www.usenix.org/conference/nsdi25).

## Introduction
Flint is an elastic block storage over EBOFs. It is tenant-aware and remote target-aware,
delivering efficient multi-tenancy and workload adaptiveness.

## Instructions
Firstly, install prerequisites through ```install_deps.sh```

Then, create logical volumes on each FS1600 EBOF NVMe drive on the control plane web interface and connect to them via NVMe-TCP.

To build the project, run
```
mkdir build
cd build
cmake .. -DCMAKE_PREFIX_PATH=/path/to/deps
make -j8
```

The arbiter process is recommended to run on a dedicated node
```
sudo ./arbiter example_conf.yaml
```

In ```tools```, we provide these tools:

```
./tools/ssd_tool 
Usage: ssd_tool [options] [arguments]
Options:
  -h, --help          Display this help message
  -d, --device        ssd to format
  -f, --format        Format ssd
      --format-all    Format all ssd
  -a, --show-all      Display all ssd information
```

```
Usage: volume_tool [options] [arguments]
Options:
  -h, --help        Display this help message
  -c, --create      Create volume
  -r, --remove      Remove volume
  -f, --rep-factor  Replication factor
  -s, --size        Size of the volume in MiB
  -p, --policy      Extent allocation policy of the volume
  -v, --show        List single volume
      --show-all    List all volumes
```

The ```bench_tool``` should run with a spec yaml. We provide an example under ```tools/```.