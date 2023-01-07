# pcp
Parallel file copy

Usage: `pcp [-f] source destination`

The number of parallel jobs is by default the number of available CPU threads.
To change this set the environment variable PCP_THREADS with the desired number of jobs:

`PCP_THREADS=4 pcp source destination`

To enable syncing of data on disk set the environment variable PCP_SYNC to true:

`PCP_SYNC=true pcp source destination`

### Unscientific test results:

Desktop PC 24 threads, 64GB RAM, NVMe SSD
```
$ ls -hal logs.tar
-rw-r--r-- 1 zaf zaf 45G Nov  2 11:44 logs.tar

$ time ./pcp logs.tar pcp.tar

real	0m28.035s
user	0m3.553s
sys	2m57.918s

$ time cp logs.tar cp.tar

real	0m42.219s
user	0m0.004s
sys	0m25.414s

$ time rsync logs.tar rsync.tar

real	1m6.785s
user	0m8.188s
sys	0m49.989s

$ time cat logs.tar > cat.tar

real	0m42.877s
user	0m0.000s
sys	0m28.206s
```

GCP VM 32 threads, 256GB RAM, Balanced PD
```
$ ls -hal data1
-rw-r----- 1 zaf zaf 801G Nov  2 14:09 data1

$ time ./pcp data1 data1.copy

real	15m52.845s
user	7m21.129s
sys	48m51.074s

$ time cp data1 data1.copy

real	36m57.556s
user	0m6.384s
sys	17m54.382s

```
