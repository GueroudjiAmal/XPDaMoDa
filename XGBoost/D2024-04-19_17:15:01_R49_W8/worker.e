2024-04-19 17:40:32,570 - distributed.utils - INFO - Reload module MofkaWorkerPlugin from .py file
2024-04-19 17:40:32,570 - distributed.utils - INFO - Reload module MofkaWorkerPlugin from .py file
2024-04-19 17:40:32,571 - distributed.utils - INFO - Reload module MofkaWorkerPlugin from .py file
2024-04-19 17:40:32,571 - distributed.utils - INFO - Reload module MofkaWorkerPlugin from .py file
2024-04-19 17:40:32,637 - distributed.preloading - INFO - Import preload module: MofkaWorkerPlugin.py
2024-04-19 17:40:32,638 - distributed.preloading - INFO - Import preload module: MofkaWorkerPlugin.py
2024-04-19 17:40:32,638 - distributed.preloading - INFO - Import preload module: MofkaWorkerPlugin.py
2024-04-19 17:40:32,638 - distributed.preloading - INFO - Import preload module: MofkaWorkerPlugin.py
2024-04-19 17:40:32,680 - distributed.nanny - INFO -         Start Nanny at: 'tcp://10.201.1.19:42563'
2024-04-19 17:40:32,680 - distributed.nanny - INFO -         Start Nanny at: 'tcp://10.201.1.19:35353'
2024-04-19 17:40:32,680 - distributed.nanny - INFO -         Start Nanny at: 'tcp://10.201.1.19:33563'
2024-04-19 17:40:32,681 - distributed.nanny - INFO -         Start Nanny at: 'tcp://10.201.1.19:39087'
2024-04-19 17:40:33,448 - distributed.preloading - INFO - Creating preload: MofkaWorkerPlugin.py
2024-04-19 17:40:33,449 - distributed.utils - INFO - Reload module MofkaWorkerPlugin from .py file
2024-04-19 17:40:33,451 - distributed.preloading - INFO - Creating preload: MofkaWorkerPlugin.py
2024-04-19 17:40:33,451 - distributed.preloading - INFO - Creating preload: MofkaWorkerPlugin.py
2024-04-19 17:40:33,451 - distributed.preloading - INFO - Creating preload: MofkaWorkerPlugin.py
2024-04-19 17:40:33,452 - distributed.utils - INFO - Reload module MofkaWorkerPlugin from .py file
2024-04-19 17:40:33,452 - distributed.utils - INFO - Reload module MofkaWorkerPlugin from .py file
2024-04-19 17:40:33,452 - distributed.utils - INFO - Reload module MofkaWorkerPlugin from .py file
2024-04-19 17:40:33,491 - distributed.preloading - INFO - Import preload module: MofkaWorkerPlugin.py
2024-04-19 17:40:33,494 - distributed.preloading - INFO - Import preload module: MofkaWorkerPlugin.py
2024-04-19 17:40:33,494 - distributed.preloading - INFO - Import preload module: MofkaWorkerPlugin.py
2024-04-19 17:40:33,494 - distributed.preloading - INFO - Import preload module: MofkaWorkerPlugin.py
2024-04-19 17:40:33,904 - distributed.utils - INFO - Reload module MofkaWorkerPlugin from .py file
2024-04-19 17:40:33,905 - distributed.utils - INFO - Reload module MofkaWorkerPlugin from .py file
2024-04-19 17:40:33,905 - distributed.utils - INFO - Reload module MofkaWorkerPlugin from .py file
2024-04-19 17:40:33,905 - distributed.utils - INFO - Reload module MofkaWorkerPlugin from .py file
2024-04-19 17:40:34,090 - distributed.preloading - INFO - Run preload setup: MofkaWorkerPlugin.py
2024-04-19 17:40:34,090 - distributed.preloading - INFO - Run preload setup: MofkaWorkerPlugin.py
2024-04-19 17:40:34,090 - distributed.preloading - INFO - Run preload setup: MofkaWorkerPlugin.py
2024-04-19 17:40:34,090 - distributed.preloading - INFO - Run preload setup: MofkaWorkerPlugin.py
2024-04-19 17:40:34,123 - distributed.preloading - INFO - Import preload module: MofkaWorkerPlugin.py
2024-04-19 17:40:34,123 - distributed.preloading - INFO - Import preload module: MofkaWorkerPlugin.py
2024-04-19 17:40:34,123 - distributed.preloading - INFO - Import preload module: MofkaWorkerPlugin.py
2024-04-19 17:40:34,123 - distributed.preloading - INFO - Import preload module: MofkaWorkerPlugin.py
2024-04-19 17:40:34,141 - distributed.nanny - INFO -         Start Nanny at: 'tcp://10.201.4.43:39309'
2024-04-19 17:40:34,141 - distributed.nanny - INFO -         Start Nanny at: 'tcp://10.201.4.43:37573'
2024-04-19 17:40:34,141 - distributed.nanny - INFO -         Start Nanny at: 'tcp://10.201.4.43:34507'
2024-04-19 17:40:34,141 - distributed.nanny - INFO -         Start Nanny at: 'tcp://10.201.4.43:33647'
[error] Could not initialize hg_class with protocol cxi
2024-04-19 17:40:34,938 - distributed.preloading - ERROR - Failed to start preload: MofkaWorkerPlugin.py
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/preloading.py", line 234, in start
    await preload.start()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/preloading.py", line 209, in start
    await result
  File "/tmp/dask-scratch-space/worker-gw6zxh2v/MofkaWorkerPlugin.py", line 163, in dask_setup
    plugin = MofkaWorkerPlugin(worker, mofka_protocol, ssg_file)
  File "/tmp/dask-scratch-space/worker-gw6zxh2v/MofkaWorkerPlugin.py", line 35, in __init__
    self.engine = Engine(self.protocol, use_progress_thread=True)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/pymargo/core.py", line 353, in __init__
    self._mid = _pymargo.init(
RuntimeError: margo_init() returned MARGO_INSTANCE_NULL
2024-04-19 17:40:34,941 - distributed.worker - INFO -       Start worker at:    tcp://10.201.1.19:44397
2024-04-19 17:40:34,941 - distributed.worker - INFO -          Listening to:    tcp://10.201.1.19:44397
2024-04-19 17:40:34,941 - distributed.worker - INFO -          dashboard at:          10.201.1.19:40335
2024-04-19 17:40:34,941 - distributed.worker - INFO - Waiting to connect to:    tcp://10.201.1.157:8786
2024-04-19 17:40:34,941 - distributed.worker - INFO - -------------------------------------------------
2024-04-19 17:40:34,941 - distributed.worker - INFO -               Threads:                          8
2024-04-19 17:40:34,941 - distributed.worker - INFO -                Memory:                 503.22 GiB
2024-04-19 17:40:34,941 - distributed.worker - INFO -       Local Directory: /tmp/dask-scratch-space/worker-gw6zxh2v
2024-04-19 17:40:34,941 - distributed.worker - INFO - -------------------------------------------------
[error] Could not initialize hg_class with protocol cxi
2024-04-19 17:40:34,948 - distributed.preloading - ERROR - Failed to start preload: MofkaWorkerPlugin.py
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/preloading.py", line 234, in start
    await preload.start()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/preloading.py", line 209, in start
    await result
  File "/tmp/dask-scratch-space/worker-bmooj8q1/MofkaWorkerPlugin.py", line 163, in dask_setup
    plugin = MofkaWorkerPlugin(worker, mofka_protocol, ssg_file)
  File "/tmp/dask-scratch-space/worker-bmooj8q1/MofkaWorkerPlugin.py", line 35, in __init__
    self.engine = Engine(self.protocol, use_progress_thread=True)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/pymargo/core.py", line 353, in __init__
    self._mid = _pymargo.init(
RuntimeError: margo_init() returned MARGO_INSTANCE_NULL
2024-04-19 17:40:34,950 - distributed.worker - INFO -       Start worker at:    tcp://10.201.1.19:37845
2024-04-19 17:40:34,950 - distributed.worker - INFO -          Listening to:    tcp://10.201.1.19:37845
2024-04-19 17:40:34,950 - distributed.worker - INFO -          dashboard at:          10.201.1.19:35907
2024-04-19 17:40:34,950 - distributed.worker - INFO - Waiting to connect to:    tcp://10.201.1.157:8786
2024-04-19 17:40:34,950 - distributed.worker - INFO - -------------------------------------------------
2024-04-19 17:40:34,950 - distributed.worker - INFO -               Threads:                          8
2024-04-19 17:40:34,950 - distributed.worker - INFO -                Memory:                 503.22 GiB
2024-04-19 17:40:34,950 - distributed.worker - INFO -       Local Directory: /tmp/dask-scratch-space/worker-bmooj8q1
2024-04-19 17:40:34,950 - distributed.worker - INFO - -------------------------------------------------
[error] Could not initialize hg_class with protocol cxi
[error] Could not initialize hg_class with protocol cxi
2024-04-19 17:40:34,953 - distributed.preloading - ERROR - Failed to start preload: MofkaWorkerPlugin.py
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/preloading.py", line 234, in start
    await preload.start()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/preloading.py", line 209, in start
    await result
  File "/tmp/dask-scratch-space/worker-3z3fxm1t/MofkaWorkerPlugin.py", line 163, in dask_setup
    plugin = MofkaWorkerPlugin(worker, mofka_protocol, ssg_file)
  File "/tmp/dask-scratch-space/worker-3z3fxm1t/MofkaWorkerPlugin.py", line 35, in __init__
    self.engine = Engine(self.protocol, use_progress_thread=True)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/pymargo/core.py", line 353, in __init__
    self._mid = _pymargo.init(
RuntimeError: margo_init() returned MARGO_INSTANCE_NULL
2024-04-19 17:40:34,955 - distributed.worker - INFO -       Start worker at:    tcp://10.201.1.19:36633
2024-04-19 17:40:34,955 - distributed.worker - INFO -          Listening to:    tcp://10.201.1.19:36633
2024-04-19 17:40:34,955 - distributed.worker - INFO -          dashboard at:          10.201.1.19:42029
2024-04-19 17:40:34,955 - distributed.worker - INFO - Waiting to connect to:    tcp://10.201.1.157:8786
2024-04-19 17:40:34,955 - distributed.worker - INFO - -------------------------------------------------
2024-04-19 17:40:34,955 - distributed.worker - INFO -               Threads:                          8
2024-04-19 17:40:34,955 - distributed.worker - INFO -                Memory:                 503.22 GiB
2024-04-19 17:40:34,955 - distributed.worker - INFO -       Local Directory: /tmp/dask-scratch-space/worker-3z3fxm1t
2024-04-19 17:40:34,955 - distributed.worker - INFO - -------------------------------------------------
2024-04-19 17:40:34,955 - distributed.preloading - ERROR - Failed to start preload: MofkaWorkerPlugin.py
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/preloading.py", line 234, in start
    await preload.start()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/preloading.py", line 209, in start
    await result
  File "/tmp/dask-scratch-space/worker-gaf35bly/MofkaWorkerPlugin.py", line 163, in dask_setup
    plugin = MofkaWorkerPlugin(worker, mofka_protocol, ssg_file)
  File "/tmp/dask-scratch-space/worker-gaf35bly/MofkaWorkerPlugin.py", line 35, in __init__
    self.engine = Engine(self.protocol, use_progress_thread=True)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/pymargo/core.py", line 353, in __init__
    self._mid = _pymargo.init(
RuntimeError: margo_init() returned MARGO_INSTANCE_NULL
2024-04-19 17:40:34,957 - distributed.worker - INFO -       Start worker at:    tcp://10.201.1.19:37783
2024-04-19 17:40:34,957 - distributed.worker - INFO -          Listening to:    tcp://10.201.1.19:37783
2024-04-19 17:40:34,957 - distributed.worker - INFO -          dashboard at:          10.201.1.19:33397
2024-04-19 17:40:34,957 - distributed.worker - INFO - Waiting to connect to:    tcp://10.201.1.157:8786
2024-04-19 17:40:34,957 - distributed.worker - INFO - -------------------------------------------------
2024-04-19 17:40:34,957 - distributed.worker - INFO -               Threads:                          8
2024-04-19 17:40:34,957 - distributed.worker - INFO -                Memory:                 503.22 GiB
2024-04-19 17:40:34,957 - distributed.worker - INFO -       Local Directory: /tmp/dask-scratch-space/worker-gaf35bly
2024-04-19 17:40:34,957 - distributed.worker - INFO - -------------------------------------------------
2024-04-19 17:40:35,096 - distributed.preloading - INFO - Creating preload: MofkaWorkerPlugin.py
2024-04-19 17:40:35,096 - distributed.preloading - INFO - Creating preload: MofkaWorkerPlugin.py
2024-04-19 17:40:35,096 - distributed.preloading - INFO - Creating preload: MofkaWorkerPlugin.py
2024-04-19 17:40:35,096 - distributed.preloading - INFO - Creating preload: MofkaWorkerPlugin.py
2024-04-19 17:40:35,097 - distributed.utils - INFO - Reload module MofkaWorkerPlugin from .py file
2024-04-19 17:40:35,097 - distributed.utils - INFO - Reload module MofkaWorkerPlugin from .py file
2024-04-19 17:40:35,097 - distributed.utils - INFO - Reload module MofkaWorkerPlugin from .py file
2024-04-19 17:40:35,097 - distributed.utils - INFO - Reload module MofkaWorkerPlugin from .py file
2024-04-19 17:40:35,142 - distributed.preloading - INFO - Import preload module: MofkaWorkerPlugin.py
2024-04-19 17:40:35,142 - distributed.preloading - INFO - Import preload module: MofkaWorkerPlugin.py
2024-04-19 17:40:35,143 - distributed.preloading - INFO - Import preload module: MofkaWorkerPlugin.py
2024-04-19 17:40:35,143 - distributed.preloading - INFO - Import preload module: MofkaWorkerPlugin.py
2024-04-19 17:40:36,105 - distributed.preloading - INFO - Run preload setup: MofkaWorkerPlugin.py
2024-04-19 17:40:36,105 - distributed.preloading - INFO - Run preload setup: MofkaWorkerPlugin.py
2024-04-19 17:40:36,105 - distributed.preloading - INFO - Run preload setup: MofkaWorkerPlugin.py
2024-04-19 17:40:36,106 - distributed.preloading - INFO - Run preload setup: MofkaWorkerPlugin.py
2024-04-19 17:40:37,180 - distributed.worker - INFO - Starting Worker plugin MofkaWorkerPlugin-1d05ee92-f46b-4401-ba39-62c55b6e9f2b
2024-04-19 17:40:37,181 - distributed.worker - INFO -       Start worker at:    tcp://10.201.4.43:42069
2024-04-19 17:40:37,181 - distributed.worker - INFO - Starting Worker plugin MofkaWorkerPlugin-0be0b93d-5975-4ae1-ab59-0d14be2d5695
2024-04-19 17:40:37,181 - distributed.worker - INFO -          Listening to:    tcp://10.201.4.43:42069
2024-04-19 17:40:37,181 - distributed.worker - INFO -          dashboard at:          10.201.4.43:35849
2024-04-19 17:40:37,181 - distributed.worker - INFO - Waiting to connect to:    tcp://10.201.1.157:8786
2024-04-19 17:40:37,181 - distributed.worker - INFO - -------------------------------------------------
2024-04-19 17:40:37,181 - distributed.worker - INFO -               Threads:                          8
2024-04-19 17:40:37,181 - distributed.worker - INFO -       Start worker at:    tcp://10.201.4.43:35895
2024-04-19 17:40:37,181 - distributed.worker - INFO -          Listening to:    tcp://10.201.4.43:35895
2024-04-19 17:40:37,181 - distributed.worker - INFO -          dashboard at:          10.201.4.43:36833
2024-04-19 17:40:37,181 - distributed.worker - INFO - Waiting to connect to:    tcp://10.201.1.157:8786
2024-04-19 17:40:37,181 - distributed.worker - INFO - Starting Worker plugin MofkaWorkerPlugin-fd4cc308-b411-4706-82f4-bfe8d8ecb4a9
2024-04-19 17:40:37,181 - distributed.worker - INFO -       Start worker at:    tcp://10.201.4.43:45149
2024-04-19 17:40:37,181 - distributed.worker - INFO -          Listening to:    tcp://10.201.4.43:45149
2024-04-19 17:40:37,181 - distributed.worker - INFO -          dashboard at:          10.201.4.43:40781
2024-04-19 17:40:37,181 - distributed.worker - INFO - Starting Worker plugin MofkaWorkerPlugin-2d264d30-8564-43e5-8c75-84665609ad30
2024-04-19 17:40:37,181 - distributed.worker - INFO -       Start worker at:    tcp://10.201.4.43:33241
2024-04-19 17:40:37,181 - distributed.worker - INFO -          Listening to:    tcp://10.201.4.43:33241
2024-04-19 17:40:37,181 - distributed.worker - INFO -          dashboard at:          10.201.4.43:43699
2024-04-19 17:40:37,181 - distributed.worker - INFO - Waiting to connect to:    tcp://10.201.1.157:8786
2024-04-19 17:40:37,181 - distributed.worker - INFO - -------------------------------------------------
2024-04-19 17:40:37,181 - distributed.worker - INFO -               Threads:                          8
2024-04-19 17:40:37,181 - distributed.worker - INFO -                Memory:                 503.22 GiB
2024-04-19 17:40:37,181 - distributed.worker - INFO -                Memory:                 503.22 GiB
2024-04-19 17:40:37,181 - distributed.worker - INFO -       Local Directory: /tmp/dask-scratch-space/worker-mbruvv5k
2024-04-19 17:40:37,181 - distributed.worker - INFO - -------------------------------------------------
2024-04-19 17:40:37,181 - distributed.worker - INFO - -------------------------------------------------
2024-04-19 17:40:37,181 - distributed.worker - INFO -               Threads:                          8
2024-04-19 17:40:37,181 - distributed.worker - INFO -                Memory:                 503.22 GiB
2024-04-19 17:40:37,181 - distributed.worker - INFO -       Local Directory: /tmp/dask-scratch-space/worker-zecdt7m3
2024-04-19 17:40:37,181 - distributed.worker - INFO - -------------------------------------------------
2024-04-19 17:40:37,181 - distributed.worker - INFO - Waiting to connect to:    tcp://10.201.1.157:8786
2024-04-19 17:40:37,181 - distributed.worker - INFO - -------------------------------------------------
2024-04-19 17:40:37,181 - distributed.worker - INFO -               Threads:                          8
2024-04-19 17:40:37,181 - distributed.worker - INFO -                Memory:                 503.22 GiB
2024-04-19 17:40:37,181 - distributed.worker - INFO -       Local Directory: /tmp/dask-scratch-space/worker-w3ko5xwx
2024-04-19 17:40:37,181 - distributed.worker - INFO - -------------------------------------------------
2024-04-19 17:40:37,181 - distributed.worker - INFO -       Local Directory: /tmp/dask-scratch-space/worker-k1dq_x4l
2024-04-19 17:40:37,181 - distributed.worker - INFO - -------------------------------------------------
2024-04-19 17:40:37,425 - distributed.worker - INFO - Starting Worker plugin shuffle
2024-04-19 17:40:37,425 - distributed.worker - INFO -         Registered to:    tcp://10.201.1.157:8786
2024-04-19 17:40:37,425 - distributed.worker - INFO - -------------------------------------------------
2024-04-19 17:40:37,426 - distributed.core - INFO - Starting established connection to tcp://10.201.1.157:8786
2024-04-19 17:40:37,427 - distributed.worker - INFO - Starting Worker plugin shuffle
2024-04-19 17:40:37,428 - distributed.worker - INFO -         Registered to:    tcp://10.201.1.157:8786
2024-04-19 17:40:37,428 - distributed.worker - INFO - -------------------------------------------------
2024-04-19 17:40:37,428 - distributed.core - INFO - Starting established connection to tcp://10.201.1.157:8786
2024-04-19 17:40:37,429 - distributed.worker - INFO - Starting Worker plugin shuffle
2024-04-19 17:40:37,430 - distributed.worker - INFO -         Registered to:    tcp://10.201.1.157:8786
2024-04-19 17:40:37,430 - distributed.worker - INFO - -------------------------------------------------
2024-04-19 17:40:37,430 - distributed.worker - INFO - Starting Worker plugin shuffle
2024-04-19 17:40:37,430 - distributed.core - INFO - Starting established connection to tcp://10.201.1.157:8786
2024-04-19 17:40:37,431 - distributed.worker - INFO -         Registered to:    tcp://10.201.1.157:8786
2024-04-19 17:40:37,431 - distributed.worker - INFO - -------------------------------------------------
2024-04-19 17:40:37,431 - distributed.core - INFO - Starting established connection to tcp://10.201.1.157:8786
2024-04-19 17:40:39,172 - distributed.worker - INFO - Starting Worker plugin shuffle
2024-04-19 17:40:39,172 - distributed.worker - INFO -         Registered to:    tcp://10.201.1.157:8786
2024-04-19 17:40:39,173 - distributed.worker - INFO - -------------------------------------------------
2024-04-19 17:40:39,173 - distributed.core - INFO - Starting established connection to tcp://10.201.1.157:8786
2024-04-19 17:40:39,174 - distributed.worker - INFO - Starting Worker plugin shuffle
2024-04-19 17:40:39,174 - distributed.worker - INFO -         Registered to:    tcp://10.201.1.157:8786
2024-04-19 17:40:39,174 - distributed.worker - INFO - -------------------------------------------------
2024-04-19 17:40:39,175 - distributed.core - INFO - Starting established connection to tcp://10.201.1.157:8786
2024-04-19 17:40:39,175 - distributed.worker - INFO - Starting Worker plugin shuffle
2024-04-19 17:40:39,175 - distributed.worker - INFO -         Registered to:    tcp://10.201.1.157:8786
2024-04-19 17:40:39,175 - distributed.worker - INFO - -------------------------------------------------
2024-04-19 17:40:39,176 - distributed.core - INFO - Starting established connection to tcp://10.201.1.157:8786
2024-04-19 17:40:39,176 - distributed.worker - INFO - Starting Worker plugin shuffle
2024-04-19 17:40:39,176 - distributed.worker - INFO -         Registered to:    tcp://10.201.1.157:8786
2024-04-19 17:40:39,177 - distributed.worker - INFO - -------------------------------------------------
2024-04-19 17:40:39,177 - distributed.core - INFO - Starting established connection to tcp://10.201.1.157:8786
2024-04-19 17:40:49,787 - distributed.utils_perf - INFO - full garbage collection released 116.03 MiB from 0 reference cycles (threshold: 9.54 MiB)
2024-04-19 17:40:49,834 - distributed.utils_perf - INFO - full garbage collection released 16.71 MiB from 0 reference cycles (threshold: 9.54 MiB)
2024-04-19 17:40:49,848 - distributed.utils_perf - INFO - full garbage collection released 26.34 MiB from 0 reference cycles (threshold: 9.54 MiB)
2024-04-19 17:40:50,852 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.94s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:40:50,852 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.96s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:40:50,869 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.93s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:40:50,870 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.96s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:40:54,450 - distributed.utils_perf - INFO - full garbage collection released 157.84 MiB from 0 reference cycles (threshold: 9.54 MiB)
2024-04-19 17:40:54,459 - distributed.utils_perf - INFO - full garbage collection released 749.76 MiB from 0 reference cycles (threshold: 9.54 MiB)
2024-04-19 17:40:55,028 - distributed.utils_perf - INFO - full garbage collection released 1.55 GiB from 0 reference cycles (threshold: 9.54 MiB)
2024-04-19 17:40:55,254 - distributed.utils_perf - INFO - full garbage collection released 64.41 MiB from 0 reference cycles (threshold: 9.54 MiB)
2024-04-19 17:40:55,527 - distributed.utils_perf - INFO - full garbage collection released 530.26 MiB from 0 reference cycles (threshold: 9.54 MiB)
2024-04-19 17:40:55,929 - distributed.utils_perf - INFO - full garbage collection released 1.29 GiB from 0 reference cycles (threshold: 9.54 MiB)
2024-04-19 17:40:56,131 - distributed.utils_perf - INFO - full garbage collection released 2.30 GiB from 0 reference cycles (threshold: 9.54 MiB)
2024-04-19 17:40:56,335 - distributed.utils_perf - INFO - full garbage collection released 1.93 GiB from 0 reference cycles (threshold: 9.54 MiB)
2024-04-19 17:40:56,452 - distributed.utils_perf - INFO - full garbage collection released 165.38 MiB from 0 reference cycles (threshold: 9.54 MiB)
2024-04-19 17:40:56,825 - distributed.utils_perf - INFO - full garbage collection released 313.32 MiB from 0 reference cycles (threshold: 9.54 MiB)
2024-04-19 17:40:56,923 - distributed.utils_perf - INFO - full garbage collection released 66.16 MiB from 0 reference cycles (threshold: 9.54 MiB)
2024-04-19 17:40:56,940 - distributed.utils_perf - INFO - full garbage collection released 174.18 MiB from 0 reference cycles (threshold: 9.54 MiB)
2024-04-19 17:41:00,457 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.37s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:41:00,509 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.89s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:41:00,960 - distributed.core - INFO - Event loop was unresponsive in Worker for 4.55s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:41:01,343 - distributed.core - INFO - Event loop was unresponsive in Worker for 4.19s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:41:01,407 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.89s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:41:02,644 - distributed.utils_perf - INFO - full garbage collection released 115.16 MiB from 0 reference cycles (threshold: 9.54 MiB)
2024-04-19 17:41:03,252 - distributed.core - INFO - Event loop was unresponsive in Worker for 4.25s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:41:04,390 - distributed.utils_perf - INFO - full garbage collection released 1.48 GiB from 0 reference cycles (threshold: 9.54 MiB)
2024-04-19 17:41:04,479 - distributed.utils_perf - INFO - full garbage collection released 2.32 GiB from 0 reference cycles (threshold: 9.54 MiB)
2024-04-19 17:41:04,690 - distributed.core - INFO - Event loop was unresponsive in Worker for 4.63s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:41:07,133 - distributed.core - INFO - Event loop was unresponsive in Worker for 6.68s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:41:07,251 - distributed.core - INFO - Event loop was unresponsive in Worker for 9.57s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:41:07,405 - distributed.core - INFO - Event loop was unresponsive in Worker for 6.90s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:41:11,578 - distributed.core - INFO - Event loop was unresponsive in Worker for 4.24s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:41:11,991 - distributed.utils_perf - INFO - full garbage collection released 425.89 MiB from 0 reference cycles (threshold: 9.54 MiB)
2024-04-19 17:41:12,742 - distributed.core - INFO - Event loop was unresponsive in Worker for 4.58s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:41:14,641 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.18s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:41:16,921 - distributed.core - INFO - Event loop was unresponsive in Worker for 7.20s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:41:17,020 - distributed.core - INFO - Event loop was unresponsive in Worker for 9.46s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:41:19,680 - distributed.core - INFO - Event loop was unresponsive in Worker for 13.72s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:41:20,178 - distributed.core - INFO - Event loop was unresponsive in Worker for 6.74s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:41:20,926 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.47s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:41:22,155 - distributed.core - INFO - Event loop was unresponsive in Worker for 4.87s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:41:22,211 - distributed.core - INFO - Event loop was unresponsive in Worker for 8.02s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:41:22,599 - distributed.core - INFO - Event loop was unresponsive in Worker for 5.72s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:41:29,725 - distributed.utils_perf - INFO - full garbage collection released 495.69 MiB from 0 reference cycles (threshold: 9.54 MiB)
2024-04-19 17:41:29,934 - distributed.core - INFO - Event loop was unresponsive in Worker for 15.29s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:41:31,182 - distributed.core - INFO - Event loop was unresponsive in Worker for 8.37s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:41:33,227 - distributed.utils_perf - INFO - full garbage collection released 56.27 MiB from 0 reference cycles (threshold: 9.54 MiB)
2024-04-19 17:41:33,434 - distributed.core - INFO - Event loop was unresponsive in Worker for 22.50s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:41:33,793 - distributed.utils_perf - INFO - full garbage collection released 179.07 MiB from 0 reference cycles (threshold: 9.54 MiB)
2024-04-19 17:41:34,051 - distributed.core - INFO - Event loop was unresponsive in Worker for 13.13s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:41:34,221 - distributed.core - INFO - Event loop was unresponsive in Worker for 10.02s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:41:34,947 - distributed.core - INFO - Event loop was unresponsive in Worker for 12.99s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:41:36,220 - distributed.utils_perf - INFO - full garbage collection released 288.65 MiB from 0 reference cycles (threshold: 9.54 MiB)
2024-04-19 17:41:36,254 - distributed.core - INFO - Event loop was unresponsive in Worker for 16.08s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:41:36,742 - distributed.core - INFO - Event loop was unresponsive in Worker for 13.55s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:41:37,197 - distributed.utils_perf - INFO - full garbage collection released 40.75 MiB from 0 reference cycles (threshold: 9.54 MiB)
2024-04-19 17:41:39,520 - distributed.core - INFO - Event loop was unresponsive in Worker for 7.44s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:41:41,272 - distributed.core - INFO - Event loop was unresponsive in Worker for 7.55s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:41:42,269 - distributed.core - INFO - Event loop was unresponsive in Worker for 8.05s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:41:44,032 - distributed.core - INFO - Event loop was unresponsive in Worker for 9.98s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:41:45,103 - distributed.core - INFO - Event loop was unresponsive in Worker for 7.72s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:41:52,792 - distributed.core - INFO - Event loop was unresponsive in Worker for 10.52s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:41:53,599 - distributed.utils_perf - INFO - full garbage collection released 20.79 MiB from 0 reference cycles (threshold: 9.54 MiB)
2024-04-19 17:41:53,773 - distributed.core - INFO - Event loop was unresponsive in Worker for 17.03s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:41:55,283 - distributed.utils_perf - INFO - full garbage collection released 81.57 MiB from 0 reference cycles (threshold: 9.54 MiB)
2024-04-19 17:41:56,722 - distributed.core - INFO - Event loop was unresponsive in Worker for 17.20s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:41:57,479 - distributed.core - INFO - Event loop was unresponsive in Worker for 12.38s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:41:59,580 - distributed.utils_perf - INFO - full garbage collection released 136.20 MiB from 0 reference cycles (threshold: 9.54 MiB)
2024-04-19 17:41:59,667 - distributed.core - INFO - Event loop was unresponsive in Worker for 24.72s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:42:02,185 - distributed.core - INFO - Event loop was unresponsive in Worker for 18.15s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:42:03,317 - distributed.core - INFO - Event loop was unresponsive in Worker for 33.38s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:42:11,857 - distributed.core - INFO - Event loop was unresponsive in Worker for 14.64s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:42:12,057 - distributed.core - INFO - Event loop was unresponsive in Worker for 14.58s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:42:12,137 - distributed.utils_perf - INFO - full garbage collection released 56.84 MiB from 0 reference cycles (threshold: 9.54 MiB)
2024-04-19 17:42:18,011 - distributed.core - INFO - Event loop was unresponsive in Worker for 18.04s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:42:18,858 - distributed.core - INFO - Event loop was unresponsive in Worker for 25.60s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:42:19,219 - distributed.core - INFO - Event loop was unresponsive in Worker for 37.95s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:42:21,334 - distributed.core - INFO - Event loop was unresponsive in Worker for 17.83s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:42:22,396 - distributed.core - INFO - Event loop was unresponsive in Worker for 10.34s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:42:23,948 - distributed.core - INFO - Event loop was unresponsive in Worker for 5.94s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:42:24,448 - distributed.core - INFO - Event loop was unresponsive in Worker for 5.29s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:42:25,200 - distributed.core - INFO - Event loop was unresponsive in Worker for 22.63s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:42:25,675 - distributed.core - INFO - Event loop was unresponsive in Worker for 13.36s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:42:25,803 - distributed.core - INFO - Event loop was unresponsive in Worker for 31.58s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:42:29,370 - distributed.core - INFO - Event loop was unresponsive in Worker for 6.97s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:42:31,192 - distributed.core - INFO - Event loop was unresponsive in Worker for 5.91s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:42:31,973 - distributed.core - INFO - Event loop was unresponsive in Worker for 7.53s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:42:34,289 - distributed.core - INFO - Event loop was unresponsive in Worker for 9.09s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:42:34,838 - distributed.core - INFO - Event loop was unresponsive in Worker for 7.68s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:42:36,326 - distributed.core - INFO - Event loop was unresponsive in Worker for 9.06s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:42:37,307 - distributed.core - INFO - Event loop was unresponsive in Worker for 5.96s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:42:39,533 - distributed.core - INFO - Event loop was unresponsive in Worker for 13.47s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:42:39,713 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.27s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:42:40,200 - distributed.core - INFO - Event loop was unresponsive in Worker for 5.91s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:42:44,497 - distributed.core - INFO - Event loop was unresponsive in Worker for 5.30s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:42:45,414 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.73s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:42:45,972 - distributed.core - INFO - Event loop was unresponsive in Worker for 14.18s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:42:47,873 - distributed.core - INFO - Event loop was unresponsive in Worker for 10.09s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:42:52,296 - distributed.core - INFO - Event loop was unresponsive in Worker for 6.36s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:42:52,402 - distributed.core - INFO - Event loop was unresponsive in Worker for 17.84s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:42:55,167 - distributed.core - INFO - Event loop was unresponsive in Worker for 9.42s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:42:56,554 - distributed.core - INFO - Event loop was unresponsive in Worker for 12.91s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:42:57,025 - distributed.core - INFO - Event loop was unresponsive in Worker for 15.60s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:42:57,034 - distributed.core - INFO - Event loop was unresponsive in Worker for 5.82s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:42:58,258 - distributed.core - INFO - Event loop was unresponsive in Worker for 9.72s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:42:58,410 - distributed.core - INFO - Event loop was unresponsive in Worker for 6.11s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:43:00,094 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.06s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:43:02,627 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.57s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:43:04,572 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.29s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:43:06,969 - distributed.core - INFO - Event loop was unresponsive in Worker for 12.43s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:43:10,298 - distributed.core - INFO - Event loop was unresponsive in Worker for 11.71s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:43:10,554 - distributed.core - INFO - Event loop was unresponsive in Worker for 22.37s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:43:11,365 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.92s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:43:12,926 - distributed.core - INFO - Event loop was unresponsive in Worker for 12.83s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:43:16,473 - distributed.core - INFO - Event loop was unresponsive in Worker for 6.17s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:43:17,836 - distributed.core - INFO - Event loop was unresponsive in Worker for 4.57s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:43:19,138 - distributed.core - INFO - Event loop was unresponsive in Worker for 4.44s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:43:19,244 - distributed.utils_perf - INFO - full garbage collection released 249.35 MiB from 0 reference cycles (threshold: 9.54 MiB)
2024-04-19 17:43:26,189 - distributed.core - INFO - Event loop was unresponsive in Worker for 22.62s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:43:26,502 - distributed.core - INFO - Event loop was unresponsive in Worker for 8.67s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:43:28,032 - distributed.core - INFO - Event loop was unresponsive in Worker for 21.22s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:43:28,793 - distributed.core - INFO - Event loop was unresponsive in Worker for 19.94s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:43:32,788 - distributed.core - INFO - Event loop was unresponsive in Worker for 32.91s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:43:33,920 - distributed.core - INFO - Event loop was unresponsive in Worker for 22.55s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:43:35,061 - distributed.core - INFO - Event loop was unresponsive in Worker for 15.92s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:43:38,133 - distributed.core - INFO - Event loop was unresponsive in Worker for 8.77s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:43:39,400 - distributed.core - INFO - Event loop was unresponsive in Worker for 5.48s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:43:44,607 - distributed.core - INFO - Event loop was unresponsive in Worker for 28.13s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:43:46,561 - distributed.core - INFO - Event loop was unresponsive in Worker for 12.11s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:43:47,054 - distributed.core - INFO - Event loop was unresponsive in Worker for 8.92s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:43:48,790 - distributed.core - INFO - Event loop was unresponsive in Worker for 9.39s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:43:52,054 - distributed.core - INFO - Event loop was unresponsive in Worker for 20.69s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:43:52,834 - distributed.core - INFO - Event loop was unresponsive in Worker for 26.31s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:43:54,369 - distributed.core - INFO - Event loop was unresponsive in Worker for 4.62s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:43:56,601 - distributed.utils_perf - INFO - full garbage collection released 211.30 MiB from 0 reference cycles (threshold: 9.54 MiB)
2024-04-19 17:43:56,873 - distributed.core - INFO - Event loop was unresponsive in Worker for 5.93s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:43:57,016 - distributed.core - INFO - Event loop was unresponsive in Worker for 9.96s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:43:58,461 - distributed.core - INFO - Event loop was unresponsive in Worker for 26.15s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:44:00,652 - distributed.core - INFO - Event loop was unresponsive in Worker for 7.82s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:44:04,344 - distributed.core - INFO - Event loop was unresponsive in Worker for 5.89s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:44:05,146 - distributed.core - INFO - Event loop was unresponsive in Worker for 12.91s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:44:05,842 - distributed.core - INFO - Event loop was unresponsive in Worker for 21.23s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:44:06,461 - distributed.core - INFO - Event loop was unresponsive in Worker for 9.45s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:44:10,786 - distributed.core - INFO - Event loop was unresponsive in Worker for 16.42s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:44:15,000 - distributed.core - INFO - Event loop was unresponsive in Worker for 39.94s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:44:15,059 - distributed.core - INFO - Event loop was unresponsive in Worker for 10.72s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:44:18,600 - distributed.core - INFO - Event loop was unresponsive in Worker for 19.91s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:44:18,765 - distributed.core - INFO - Event loop was unresponsive in Worker for 14.35s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:44:22,049 - distributed.core - INFO - Event loop was unresponsive in Worker for 11.06s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:44:22,540 - distributed.core - INFO - Event loop was unresponsive in Worker for 7.48s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:44:24,119 - distributed.core - INFO - Event loop was unresponsive in Worker for 5.02s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:44:25,781 - distributed.core - INFO - Event loop was unresponsive in Worker for 6.40s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:44:28,513 - distributed.core - INFO - Event loop was unresponsive in Worker for 22.05s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:44:30,018 - distributed.core - INFO - Event loop was unresponsive in Worker for 7.97s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:44:30,438 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.19s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:44:33,361 - distributed.core - INFO - Event loop was unresponsive in Worker for 16.17s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:44:34,284 - distributed.core - INFO - Event loop was unresponsive in Worker for 26.69s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:44:35,884 - distributed.core - INFO - Event loop was unresponsive in Worker for 5.87s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:44:35,945 - distributed.core - INFO - Event loop was unresponsive in Worker for 30.80s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:44:38,989 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.24s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:44:39,367 - distributed.core - INFO - Event loop was unresponsive in Worker for 15.25s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:44:42,261 - distributed.core - INFO - Event loop was unresponsive in Worker for 4.53s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:44:42,394 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.33s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:44:43,410 - distributed.core - INFO - Event loop was unresponsive in Worker for 11.59s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:44:44,390 - distributed.core - INFO - Event loop was unresponsive in Worker for 16.30s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:44:44,805 - distributed.core - INFO - Event loop was unresponsive in Worker for 14.37s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:44:48,126 - distributed.core - INFO - Event loop was unresponsive in Worker for 8.76s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:44:49,638 - distributed.core - INFO - Event loop was unresponsive in Worker for 11.89s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:44:49,980 - distributed.core - INFO - Event loop was unresponsive in Worker for 7.59s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:44:52,873 - distributed.core - INFO - Event loop was unresponsive in Worker for 13.08s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:45:00,435 - distributed.utils_perf - INFO - full garbage collection released 264.18 MiB from 56 reference cycles (threshold: 9.54 MiB)
2024-04-19 17:45:00,950 - distributed.core - INFO - Event loop was unresponsive in Worker for 15.60s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:45:01,536 - distributed.core - INFO - Event loop was unresponsive in Worker for 8.95s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:45:02,673 - distributed.core - INFO - Event loop was unresponsive in Worker for 15.96s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:45:04,226 - distributed.core - INFO - Event loop was unresponsive in Worker for 16.28s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:45:04,344 - distributed.core - INFO - Event loop was unresponsive in Worker for 14.71s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:45:07,433 - distributed.core - INFO - Event loop was unresponsive in Worker for 20.21s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:45:09,289 - distributed.core - INFO - Event loop was unresponsive in Worker for 13.09s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:45:10,009 - distributed.core - INFO - Event loop was unresponsive in Worker for 5.78s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:45:11,512 - distributed.core - INFO - Event loop was unresponsive in Worker for 9.94s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:45:12,122 - distributed.core - INFO - Event loop was unresponsive in Worker for 13.45s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:45:17,719 - distributed.core - INFO - Event loop was unresponsive in Worker for 9.94s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:45:20,689 - distributed.core - INFO - Event loop was unresponsive in Worker for 18.02s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:45:22,478 - distributed.core - INFO - Event loop was unresponsive in Worker for 13.43s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:45:23,155 - distributed.core - INFO - Event loop was unresponsive in Worker for 17.90s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:45:24,295 - distributed.core - INFO - Event loop was unresponsive in Worker for 12.25s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:45:24,857 - distributed.core - INFO - Event loop was unresponsive in Worker for 11.59s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:45:26,331 - distributed.utils_perf - INFO - full garbage collection released 29.97 MiB from 75 reference cycles (threshold: 9.54 MiB)
2024-04-19 17:45:26,751 - distributed.core - INFO - Event loop was unresponsive in Worker for 5.54s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:45:27,572 - distributed.core - INFO - Event loop was unresponsive in Worker for 12.81s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:45:34,772 - distributed.core - INFO - Event loop was unresponsive in Worker for 13.81s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:45:37,051 - distributed.core - INFO - Event loop was unresponsive in Worker for 6.97s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:45:39,490 - distributed.core - INFO - Event loop was unresponsive in Worker for 13.37s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:45:39,762 - distributed.core - INFO - Event loop was unresponsive in Worker for 13.34s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:45:39,881 - distributed.core - INFO - Event loop was unresponsive in Worker for 5.05s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:45:41,734 - distributed.core - INFO - Event loop was unresponsive in Worker for 17.29s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:45:42,902 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.14s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:45:43,157 - distributed.core - INFO - Event loop was unresponsive in Worker for 18.30s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:45:48,440 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.01s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:45:48,560 - distributed.core - INFO - Event loop was unresponsive in Worker for 5.46s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:45:50,928 - distributed.core - INFO - Event loop was unresponsive in Worker for 10.92s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:45:51,719 - distributed.core - INFO - Event loop was unresponsive in Worker for 13.06s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:45:54,859 - distributed.core - INFO - Event loop was unresponsive in Worker for 11.96s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:45:57,736 - distributed.core - INFO - Event loop was unresponsive in Worker for 4.86s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:46:01,571 - distributed.utils_perf - INFO - full garbage collection released 202.47 MiB from 0 reference cycles (threshold: 9.54 MiB)
2024-04-19 17:46:02,579 - distributed.core - INFO - Event loop was unresponsive in Worker for 14.14s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:46:02,714 - distributed.core - INFO - Event loop was unresponsive in Worker for 13.90s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:46:04,584 - distributed.core - INFO - Event loop was unresponsive in Worker for 49.92s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:46:05,673 - distributed.core - INFO - Event loop was unresponsive in Worker for 11.20s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:46:06,316 - distributed.core - INFO - Event loop was unresponsive in Worker for 12.55s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:46:06,631 - distributed.core - INFO - Event loop was unresponsive in Worker for 38.42s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:46:05,903 - distributed.worker - ERROR - Worker stream died during communication: tcp://10.201.4.43:42069
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 546, in connect
    stream = await self.client.connect(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/tornado/tcpclient.py", line 279, in connect
    af, addr, stream = await connector.start(connect_timeout=timeout)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/tasks.py", line 456, in wait_for
    return fut.result()
asyncio.exceptions.CancelledError

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/core.py", line 342, in connect
    comm = await wait_for(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/utils.py", line 1961, in wait_for
    return await asyncio.wait_for(fut, timeout)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/tasks.py", line 458, in wait_for
    raise exceptions.TimeoutError() from exc
asyncio.exceptions.TimeoutError

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/worker.py", line 2059, in gather_dep
    response = await get_data_from_worker(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/worker.py", line 2860, in get_data_from_worker
    comm = await rpc.connect(worker)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1674, in connect
    return connect_attempt.result()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1564, in _connect
    comm = await connect(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/core.py", line 368, in connect
    raise OSError(
OSError: Timed out trying to connect to tcp://10.201.4.43:42069 after 30 s
2024-04-19 17:46:07,086 - distributed.core - INFO - Event loop was unresponsive in Worker for 4.04s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:46:11,114 - distributed.comm.tcp - INFO - Connection from tcp://10.201.1.19:49716 closed before handshake completed
2024-04-19 17:46:11,202 - distributed.core - INFO - Event loop was unresponsive in Worker for 4.89s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:46:12,284 - distributed.core - INFO - Event loop was unresponsive in Worker for 6.61s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:46:12,552 - distributed.utils_perf - INFO - full garbage collection released 1.38 GiB from 0 reference cycles (threshold: 9.54 MiB)
2024-04-19 17:46:13,612 - distributed.core - INFO - Event loop was unresponsive in Worker for 14.12s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:46:15,143 - distributed.core - INFO - Event loop was unresponsive in Worker for 20.28s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:46:16,573 - distributed.core - INFO - Event loop was unresponsive in Worker for 9.49s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:46:21,165 - distributed.core - INFO - Event loop was unresponsive in Worker for 16.48s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:46:22,066 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.88s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:46:24,558 - distributed.core - INFO - Event loop was unresponsive in Worker for 13.36s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:46:25,584 - distributed.core - INFO - Event loop was unresponsive in Worker for 9.34s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:46:27,516 - distributed.core - INFO - Event loop was unresponsive in Worker for 17.05s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:46:27,931 - distributed.core - INFO - Event loop was unresponsive in Worker for 21.30s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:46:31,530 - distributed.core - INFO - Event loop was unresponsive in Worker for 17.51s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:46:33,393 - distributed.core - INFO - Event loop was unresponsive in Worker for 4.08s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:46:33,600 - distributed.core - INFO - Event loop was unresponsive in Worker for 6.08s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:46:34,283 - distributed.core - INFO - Event loop was unresponsive in Worker for 7.35s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:46:35,342 - distributed.core - INFO - Event loop was unresponsive in Worker for 13.13s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:46:36,479 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.22s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:46:37,857 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.98s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:46:38,920 - distributed.core - INFO - Event loop was unresponsive in Worker for 4.64s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:46:42,316 - distributed.core - INFO - Event loop was unresponsive in Worker for 6.97s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:46:42,361 - distributed.core - INFO - Event loop was unresponsive in Worker for 17.92s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:46:43,119 - distributed.utils_perf - INFO - full garbage collection released 3.92 GiB from 37 reference cycles (threshold: 9.54 MiB)
2024-04-19 17:46:43,518 - distributed.core - INFO - Event loop was unresponsive in Worker for 25.43s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:46:43,787 - distributed.core - INFO - Event loop was unresponsive in Worker for 15.61s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:46:43,862 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.32s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:46:48,991 - distributed.core - INFO - Event loop was unresponsive in Worker for 4.51s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:46:49,626 - distributed.core - INFO - Event loop was unresponsive in Worker for 4.50s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:46:51,304 - distributed.core - INFO - Event loop was unresponsive in Worker for 14.82s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:46:52,219 - distributed.core - INFO - Event loop was unresponsive in Worker for 18.62s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:46:53,894 - distributed.core - INFO - Event loop was unresponsive in Worker for 8.92s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:46:56,579 - distributed.core - INFO - Event loop was unresponsive in Worker for 4.57s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:46:57,069 - distributed.core - INFO - Event loop was unresponsive in Worker for 17.64s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:47:02,898 - distributed.core - INFO - Event loop was unresponsive in Worker for 13.27s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:47:03,883 - distributed.core - INFO - Event loop was unresponsive in Worker for 10.22s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:47:04,658 - distributed.core - INFO - Event loop was unresponsive in Worker for 9.64s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:47:05,832 - distributed.core - INFO - Event loop was unresponsive in Worker for 14.64s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:47:06,190 - distributed.core - INFO - Event loop was unresponsive in Worker for 13.43s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:47:09,089 - distributed.core - INFO - Event loop was unresponsive in Worker for 5.05s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:47:09,547 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.72s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:47:11,209 - distributed.core - INFO - Event loop was unresponsive in Worker for 17.28s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:47:12,649 - distributed.core - INFO - Event loop was unresponsive in Worker for 5.18s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:47:15,530 - distributed.core - INFO - Event loop was unresponsive in Worker for 18.95s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:47:21,237 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.77s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:47:21,980 - distributed.core - INFO - Event loop was unresponsive in Worker for 12.89s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:47:22,137 - distributed.core - INFO - Event loop was unresponsive in Worker for 18.22s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:47:22,922 - distributed.core - INFO - Event loop was unresponsive in Worker for 4.93s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:47:25,045 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.81s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:47:28,518 - distributed.core - INFO - Event loop was unresponsive in Worker for 22.55s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:47:28,654 - distributed.core - INFO - Event loop was unresponsive in Worker for 4.97s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:47:31,504 - distributed.core - INFO - Event loop was unresponsive in Worker for 20.83s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:47:35,261 - distributed.core - INFO - Event loop was unresponsive in Worker for 29.07s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:47:36,141 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.48s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:47:36,565 - distributed.core - INFO - Event loop was unresponsive in Worker for 9.81s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:47:37,018 - distributed.core - INFO - Event loop was unresponsive in Worker for 14.10s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:47:37,318 - distributed.core - INFO - Event loop was unresponsive in Worker for 12.52s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:47:39,256 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.59s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:47:42,840 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.95s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:47:44,112 - distributed.core - INFO - Event loop was unresponsive in Worker for 4.62s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:47:47,013 - distributed.core - INFO - Event loop was unresponsive in Worker for 5.51s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:47:48,270 - distributed.core - INFO - Event loop was unresponsive in Worker for 19.62s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:47:48,818 - distributed.utils_perf - INFO - full garbage collection released 315.85 MiB from 18 reference cycles (threshold: 9.54 MiB)
2024-04-19 17:47:49,044 - distributed.core - INFO - Event loop was unresponsive in Worker for 34.09s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:47:49,246 - distributed.core - INFO - Event loop was unresponsive in Worker for 5.76s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:47:50,039 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.03s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:47:50,195 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.26s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:47:51,180 - distributed.core - INFO - Event loop was unresponsive in Worker for 11.92s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:47:51,406 - distributed.core - INFO - Event loop was unresponsive in Worker for 15.27s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:47:55,073 - distributed.core - INFO - Event loop was unresponsive in Worker for 4.31s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:47:57,260 - distributed.core - INFO - Event loop was unresponsive in Worker for 7.22s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:47:58,006 - distributed.core - INFO - Event loop was unresponsive in Worker for 8.76s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:47:58,015 - distributed.core - INFO - Event loop was unresponsive in Worker for 5.06s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:47:58,757 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.45s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:47:59,722 - distributed.core - INFO - Event loop was unresponsive in Worker for 22.40s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:47:59,960 - distributed.utils_perf - INFO - full garbage collection released 13.69 MiB from 189 reference cycles (threshold: 9.54 MiB)
2024-04-19 17:48:02,705 - distributed.core - INFO - Event loop was unresponsive in Worker for 7.63s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:48:02,903 - distributed.core - INFO - Event loop was unresponsive in Worker for 4.37s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:48:02,996 - distributed.core - INFO - Event loop was unresponsive in Worker for 4.77s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:48:03,352 - distributed.utils_perf - WARNING - full garbage collections took 49% CPU time recently (threshold: 10%)
2024-04-19 17:48:03,388 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.35s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:48:04,444 - distributed.utils_perf - WARNING - full garbage collections took 49% CPU time recently (threshold: 10%)
2024-04-19 17:48:05,823 - distributed.utils_perf - WARNING - full garbage collections took 50% CPU time recently (threshold: 10%)
2024-04-19 17:48:07,513 - distributed.utils_perf - WARNING - full garbage collections took 49% CPU time recently (threshold: 10%)
2024-04-19 17:48:07,906 - distributed.core - INFO - Event loop was unresponsive in Worker for 4.73s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:48:09,617 - distributed.utils_perf - WARNING - full garbage collections took 49% CPU time recently (threshold: 10%)
2024-04-19 17:48:10,150 - distributed.core - INFO - Event loop was unresponsive in Worker for 5.88s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:48:12,199 - distributed.utils_perf - WARNING - full garbage collections took 49% CPU time recently (threshold: 10%)
2024-04-19 17:48:12,385 - distributed.core - INFO - Event loop was unresponsive in Worker for 21.21s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:48:12,907 - distributed.core - INFO - Event loop was unresponsive in Worker for 13.24s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:48:13,565 - distributed.core - INFO - Event loop was unresponsive in Worker for 10.18s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:48:14,390 - distributed.core - INFO - Event loop was unresponsive in Worker for 4.70s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:48:15,382 - distributed.utils_perf - WARNING - full garbage collections took 48% CPU time recently (threshold: 10%)
2024-04-19 17:48:20,218 - distributed.core - INFO - Event loop was unresponsive in Worker for 16.32s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:48:22,366 - distributed.core - INFO - Event loop was unresponsive in Worker for 17.33s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:48:22,424 - distributed.core - INFO - Event loop was unresponsive in Worker for 4.48s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:48:22,699 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.99s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:48:22,899 - distributed.utils_perf - INFO - full garbage collection released 1.06 GiB from 37 reference cycles (threshold: 9.54 MiB)
2024-04-19 17:48:23,279 - distributed.core - INFO - Event loop was unresponsive in Worker for 10.37s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:48:26,311 - distributed.core - INFO - Event loop was unresponsive in Worker for 6.09s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:48:26,986 - distributed.core - INFO - Event loop was unresponsive in Worker for 4.92s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:48:26,988 - distributed.core - INFO - Event loop was unresponsive in Worker for 4.03s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:48:28,069 - distributed.core - INFO - Event loop was unresponsive in Worker for 5.70s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:48:28,764 - distributed.utils_perf - INFO - full garbage collection released 17.69 MiB from 152 reference cycles (threshold: 9.54 MiB)
2024-04-19 17:48:29,838 - distributed.core - INFO - Event loop was unresponsive in Worker for 13.63s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:48:30,725 - distributed.core - INFO - Event loop was unresponsive in Worker for 4.29s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:48:31,344 - distributed.core - INFO - Event loop was unresponsive in Worker for 4.66s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:48:32,560 - distributed.core - INFO - Event loop was unresponsive in Worker for 4.12s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:48:34,499 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.16s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:48:35,040 - distributed.utils_perf - WARNING - full garbage collections took 44% CPU time recently (threshold: 10%)
2024-04-19 17:48:36,973 - distributed.utils_perf - WARNING - full garbage collections took 44% CPU time recently (threshold: 10%)
2024-04-19 17:48:37,566 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.64s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:48:39,155 - distributed.core - INFO - Event loop was unresponsive in Worker for 12.17s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:48:39,168 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.97s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:48:39,374 - distributed.utils_perf - WARNING - full garbage collections took 44% CPU time recently (threshold: 10%)
2024-04-19 17:48:42,316 - distributed.utils_perf - WARNING - full garbage collections took 44% CPU time recently (threshold: 10%)
2024-04-19 17:48:45,796 - distributed.core - INFO - Event loop was unresponsive in Worker for 17.73s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:48:45,992 - distributed.utils_perf - WARNING - full garbage collections took 44% CPU time recently (threshold: 10%)
2024-04-19 17:48:47,949 - distributed.core - INFO - Event loop was unresponsive in Worker for 16.26s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:48:49,097 - distributed.core - INFO - Event loop was unresponsive in Worker for 11.53s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:48:49,215 - distributed.core - INFO - Event loop was unresponsive in Worker for 4.50s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:48:53,536 - distributed.core - INFO - Event loop was unresponsive in Worker for 6.58s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:48:53,900 - distributed.core - INFO - Event loop was unresponsive in Worker for 14.17s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:48:55,015 - distributed.core - INFO - Event loop was unresponsive in Worker for 4.83s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:48:56,907 - distributed.core - INFO - Event loop was unresponsive in Worker for 5.18s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:48:57,233 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.70s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:48:57,546 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.65s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:48:58,462 - distributed.utils_perf - WARNING - full garbage collections took 40% CPU time recently (threshold: 10%)
2024-04-19 17:48:59,135 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.60s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:48:59,536 - distributed.utils_perf - WARNING - full garbage collections took 40% CPU time recently (threshold: 10%)
2024-04-19 17:49:00,866 - distributed.utils_perf - WARNING - full garbage collections took 40% CPU time recently (threshold: 10%)
2024-04-19 17:49:01,209 - distributed.core - INFO - Event loop was unresponsive in Worker for 11.99s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:49:02,552 - distributed.utils_perf - WARNING - full garbage collections took 40% CPU time recently (threshold: 10%)
2024-04-19 17:49:03,782 - distributed.core - INFO - Event loop was unresponsive in Worker for 4.98s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:49:04,571 - distributed.core - INFO - Event loop was unresponsive in Worker for 5.38s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:49:04,627 - distributed.utils_perf - WARNING - full garbage collections took 40% CPU time recently (threshold: 10%)
2024-04-19 17:49:06,354 - distributed.core - INFO - Event loop was unresponsive in Worker for 9.18s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:49:07,198 - distributed.utils_perf - WARNING - full garbage collections took 40% CPU time recently (threshold: 10%)
2024-04-19 17:49:09,018 - distributed.core - INFO - Event loop was unresponsive in Worker for 4.67s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:49:10,733 - distributed.core - INFO - Event loop was unresponsive in Worker for 11.60s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:49:11,297 - distributed.core - INFO - Event loop was unresponsive in Worker for 4.82s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:49:13,278 - distributed.core - INFO - Event loop was unresponsive in Worker for 4.56s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:49:16,072 - distributed.core - INFO - Event loop was unresponsive in Worker for 4.13s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:49:17,521 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.37s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:49:17,746 - distributed.core - INFO - Event loop was unresponsive in Worker for 5.45s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:49:17,897 - distributed.utils_perf - WARNING - full garbage collections took 37% CPU time recently (threshold: 10%)
2024-04-19 17:49:18,224 - distributed.core - INFO - Event loop was unresponsive in Worker for 11.87s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:49:18,407 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.60s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:49:18,439 - distributed.utils_perf - WARNING - full garbage collections took 36% CPU time recently (threshold: 10%)
2024-04-19 17:49:19,104 - distributed.utils_perf - WARNING - full garbage collections took 35% CPU time recently (threshold: 10%)
2024-04-19 17:49:19,636 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.60s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:49:19,918 - distributed.utils_perf - WARNING - full garbage collections took 33% CPU time recently (threshold: 10%)
2024-04-19 17:49:20,924 - distributed.utils_perf - WARNING - full garbage collections took 33% CPU time recently (threshold: 10%)
2024-04-19 17:49:21,988 - distributed.utils_perf - INFO - full garbage collection released 15.56 MiB from 169 reference cycles (threshold: 9.54 MiB)
2024-04-19 17:49:22,171 - distributed.utils_perf - WARNING - full garbage collections took 31% CPU time recently (threshold: 10%)
2024-04-19 17:49:22,888 - distributed.core - INFO - Event loop was unresponsive in Worker for 5.14s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:49:23,721 - distributed.utils_perf - WARNING - full garbage collections took 29% CPU time recently (threshold: 10%)
2024-04-19 17:49:25,668 - distributed.utils_perf - WARNING - full garbage collections took 28% CPU time recently (threshold: 10%)
2024-04-19 17:49:26,447 - distributed.core - INFO - Event loop was unresponsive in Worker for 8.22s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:49:28,041 - distributed.utils_perf - WARNING - full garbage collections took 26% CPU time recently (threshold: 10%)
2024-04-19 17:49:31,003 - distributed.utils_perf - WARNING - full garbage collections took 25% CPU time recently (threshold: 10%)
2024-04-19 17:49:33,070 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.54s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:49:34,450 - distributed.core - INFO - Event loop was unresponsive in Worker for 4.11s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:49:34,667 - distributed.utils_perf - WARNING - full garbage collections took 22% CPU time recently (threshold: 10%)
2024-04-19 17:49:35,558 - distributed.core - INFO - Event loop was unresponsive in Worker for 6.59s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:49:40,991 - distributed.core - INFO - Event loop was unresponsive in Worker for 4.35s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:49:43,596 - distributed.core - INFO - Event loop was unresponsive in Worker for 4.75s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:49:54,602 - distributed.core - INFO - Event loop was unresponsive in Worker for 17.24s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:50:02,163 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.46s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:50:06,874 - distributed.core - INFO - Event loop was unresponsive in Worker for 4.50s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
[17:50:37] task [xgboost.dask-tcp://10.201.1.19:36633]:tcp://10.201.1.19:36633 got new rank 0
[17:50:37] task [xgboost.dask-tcp://10.201.1.19:37783]:tcp://10.201.1.19:37783 got new rank 1
[17:50:37] task [xgboost.dask-tcp://10.201.1.19:37845]:tcp://10.201.1.19:37845 got new rank 2
[17:50:37] task [xgboost.dask-tcp://10.201.1.19:44397]:tcp://10.201.1.19:44397 got new rank 3
[17:50:37] task [xgboost.dask-tcp://10.201.4.43:33241]:tcp://10.201.4.43:33241 got new rank 4
[17:50:37] task [xgboost.dask-tcp://10.201.4.43:35895]:tcp://10.201.4.43:35895 got new rank 5
[17:50:37] task [xgboost.dask-tcp://10.201.4.43:42069]:tcp://10.201.4.43:42069 got new rank 6
[17:50:37] task [xgboost.dask-tcp://10.201.4.43:45149]:tcp://10.201.4.43:45149 got new rank 7
2024-04-19 17:53:00,796 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.38s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
[17:53:43] task [xgboost.dask-tcp://10.201.1.19:36633]:tcp://10.201.1.19:36633 got new rank 0
[17:53:43] task [xgboost.dask-tcp://10.201.1.19:37783]:tcp://10.201.1.19:37783 got new rank 1
[17:53:43] task [xgboost.dask-tcp://10.201.1.19:37845]:tcp://10.201.1.19:37845 got new rank 2
[17:53:43] task [xgboost.dask-tcp://10.201.1.19:44397]:tcp://10.201.1.19:44397 got new rank 3
[17:53:43] task [xgboost.dask-tcp://10.201.4.43:33241]:tcp://10.201.4.43:33241 got new rank 4
[17:53:43] task [xgboost.dask-tcp://10.201.4.43:35895]:tcp://10.201.4.43:35895 got new rank 5
[17:53:43] task [xgboost.dask-tcp://10.201.4.43:42069]:tcp://10.201.4.43:42069 got new rank 6
[17:53:43] task [xgboost.dask-tcp://10.201.4.43:45149]:tcp://10.201.4.43:45149 got new rank 7
[17:56:53] task [xgboost.dask-tcp://10.201.1.19:36633]:tcp://10.201.1.19:36633 got new rank 0
[17:56:53] task [xgboost.dask-tcp://10.201.1.19:37783]:tcp://10.201.1.19:37783 got new rank 1
[17:56:53] task [xgboost.dask-tcp://10.201.1.19:37845]:tcp://10.201.1.19:37845 got new rank 2
[17:56:53] task [xgboost.dask-tcp://10.201.1.19:44397]:tcp://10.201.1.19:44397 got new rank 3
[17:56:53] task [xgboost.dask-tcp://10.201.4.43:33241]:tcp://10.201.4.43:33241 got new rank 4
[17:56:53] task [xgboost.dask-tcp://10.201.4.43:35895]:tcp://10.201.4.43:35895 got new rank 5
[17:56:53] task [xgboost.dask-tcp://10.201.4.43:42069]:tcp://10.201.4.43:42069 got new rank 6
[17:56:53] task [xgboost.dask-tcp://10.201.4.43:45149]:tcp://10.201.4.43:45149 got new rank 7
2024-04-19 17:59:22,841 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.25s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:59:23,108 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.59s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:59:23,168 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.64s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:59:23,369 - distributed.core - INFO - Event loop was unresponsive in Worker for 3.79s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
[18:00:00] task [xgboost.dask-tcp://10.201.1.19:36633]:tcp://10.201.1.19:36633 got new rank 0
[18:00:00] task [xgboost.dask-tcp://10.201.1.19:37783]:tcp://10.201.1.19:37783 got new rank 1
[18:00:00] task [xgboost.dask-tcp://10.201.1.19:37845]:tcp://10.201.1.19:37845 got new rank 2
[18:00:00] task [xgboost.dask-tcp://10.201.1.19:44397]:tcp://10.201.1.19:44397 got new rank 3
[18:00:00] task [xgboost.dask-tcp://10.201.4.43:33241]:tcp://10.201.4.43:33241 got new rank 4
[18:00:00] task [xgboost.dask-tcp://10.201.4.43:35895]:tcp://10.201.4.43:35895 got new rank 5
[18:00:00] task [xgboost.dask-tcp://10.201.4.43:42069]:tcp://10.201.4.43:42069 got new rank 6
[18:00:00] task [xgboost.dask-tcp://10.201.4.43:45149]:tcp://10.201.4.43:45149 got new rank 7
[18:02:53] task [xgboost.dask-tcp://10.201.1.19:36633]:tcp://10.201.1.19:36633 got new rank 0
[18:02:53] task [xgboost.dask-tcp://10.201.1.19:37783]:tcp://10.201.1.19:37783 got new rank 1
[18:02:53] task [xgboost.dask-tcp://10.201.1.19:37845]:tcp://10.201.1.19:37845 got new rank 2
[18:02:53] task [xgboost.dask-tcp://10.201.1.19:44397]:tcp://10.201.1.19:44397 got new rank 3
[18:02:53] task [xgboost.dask-tcp://10.201.4.43:33241]:tcp://10.201.4.43:33241 got new rank 4
[18:02:53] task [xgboost.dask-tcp://10.201.4.43:35895]:tcp://10.201.4.43:35895 got new rank 5
[18:02:53] task [xgboost.dask-tcp://10.201.4.43:42069]:tcp://10.201.4.43:42069 got new rank 6
[18:02:53] task [xgboost.dask-tcp://10.201.4.43:45149]:tcp://10.201.4.43:45149 got new rank 7
2024-04-19 18:08:14,311 - distributed.worker - INFO - Stopping worker at tcp://10.201.4.43:35895. Reason: scheduler-close
2024-04-19 18:08:14,311 - distributed.worker - INFO - Stopping worker at tcp://10.201.4.43:45149. Reason: scheduler-close
2024-04-19 18:08:14,311 - distributed.worker - INFO - Stopping worker at tcp://10.201.4.43:33241. Reason: scheduler-close
2024-04-19 18:08:14,311 - distributed.worker - INFO - Stopping worker at tcp://10.201.4.43:42069. Reason: scheduler-close
2024-04-19 18:08:14,312 - distributed.nanny - INFO - Closing Nanny gracefully at 'tcp://10.201.4.43:33647'. Reason: scheduler-close
2024-04-19 18:08:14,312 - distributed.nanny - INFO - Closing Nanny gracefully at 'tcp://10.201.4.43:37573'. Reason: scheduler-close
2024-04-19 18:08:14,313 - distributed.nanny - INFO - Closing Nanny gracefully at 'tcp://10.201.4.43:39309'. Reason: scheduler-close
2024-04-19 18:08:14,313 - distributed.nanny - INFO - Closing Nanny gracefully at 'tcp://10.201.4.43:34507'. Reason: scheduler-close
2024-04-19 18:08:14,311 - distributed.worker - INFO - Stopping worker at tcp://10.201.1.19:37845. Reason: scheduler-close
2024-04-19 18:08:14,311 - distributed.worker - INFO - Stopping worker at tcp://10.201.1.19:37783. Reason: scheduler-close
2024-04-19 18:08:14,311 - distributed.worker - INFO - Stopping worker at tcp://10.201.1.19:44397. Reason: scheduler-close
2024-04-19 18:08:14,311 - distributed.worker - INFO - Stopping worker at tcp://10.201.1.19:36633. Reason: scheduler-close
2024-04-19 18:08:14,314 - distributed.batched - INFO - Batched Comm Closed <TCP (closed) Worker->Scheduler local=tcp://10.201.1.19:54560 remote=tcp://10.201.1.157:8786>
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 297, in write
    raise StreamClosedError()
tornado.iostream.StreamClosedError: Stream is closed

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/batched.py", line 115, in _background_send
    nbytes = yield coro
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/tornado/gen.py", line 767, in run
    value = future.result()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 307, in write
    convert_stream_closed_error(self, e)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 142, in convert_stream_closed_error
    raise CommClosedError(f"in {obj}: {exc}") from exc
distributed.comm.core.CommClosedError: in <TCP (closed) Worker->Scheduler local=tcp://10.201.1.19:54560 remote=tcp://10.201.1.157:8786>: Stream is closed
2024-04-19 18:08:14,319 - distributed.nanny - INFO - Closing Nanny gracefully at 'tcp://10.201.1.19:35353'. Reason: scheduler-close
2024-04-19 18:08:14,314 - distributed.batched - INFO - Batched Comm Closed <TCP (closed) Worker->Scheduler local=tcp://10.201.1.19:54552 remote=tcp://10.201.1.157:8786>
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 297, in write
    raise StreamClosedError()
tornado.iostream.StreamClosedError: Stream is closed

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/batched.py", line 115, in _background_send
    nbytes = yield coro
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/tornado/gen.py", line 767, in run
    value = future.result()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 307, in write
    convert_stream_closed_error(self, e)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 142, in convert_stream_closed_error
    raise CommClosedError(f"in {obj}: {exc}") from exc
distributed.comm.core.CommClosedError: in <TCP (closed) Worker->Scheduler local=tcp://10.201.1.19:54552 remote=tcp://10.201.1.157:8786>: Stream is closed
2024-04-19 18:08:14,314 - distributed.batched - INFO - Batched Comm Closed <TCP (closed) Worker->Scheduler local=tcp://10.201.1.19:54548 remote=tcp://10.201.1.157:8786>
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 297, in write
    raise StreamClosedError()
tornado.iostream.StreamClosedError: Stream is closed

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/batched.py", line 115, in _background_send
    nbytes = yield coro
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/tornado/gen.py", line 767, in run
    value = future.result()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 307, in write
    convert_stream_closed_error(self, e)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 142, in convert_stream_closed_error
    raise CommClosedError(f"in {obj}: {exc}") from exc
distributed.comm.core.CommClosedError: in <TCP (closed) Worker->Scheduler local=tcp://10.201.1.19:54548 remote=tcp://10.201.1.157:8786>: Stream is closed
2024-04-19 18:08:14,314 - distributed.batched - INFO - Batched Comm Closed <TCP (closed) Worker->Scheduler local=tcp://10.201.1.19:54536 remote=tcp://10.201.1.157:8786>
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 297, in write
    raise StreamClosedError()
tornado.iostream.StreamClosedError: Stream is closed

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/batched.py", line 115, in _background_send
    nbytes = yield coro
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/tornado/gen.py", line 767, in run
    value = future.result()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 307, in write
    convert_stream_closed_error(self, e)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 142, in convert_stream_closed_error
    raise CommClosedError(f"in {obj}: {exc}") from exc
distributed.comm.core.CommClosedError: in <TCP (closed) Worker->Scheduler local=tcp://10.201.1.19:54536 remote=tcp://10.201.1.157:8786>: Stream is closed
2024-04-19 18:08:14,322 - distributed.nanny - INFO - Closing Nanny gracefully at 'tcp://10.201.1.19:42563'. Reason: scheduler-close
2024-04-19 18:08:14,323 - distributed.nanny - INFO - Closing Nanny gracefully at 'tcp://10.201.1.19:33563'. Reason: scheduler-close
2024-04-19 18:08:14,323 - distributed.nanny - INFO - Closing Nanny gracefully at 'tcp://10.201.1.19:39087'. Reason: scheduler-close
2024-04-19 18:08:14,430 - distributed.core - INFO - Received 'close-stream' from tcp://10.201.1.157:8786; closing.
2024-04-19 18:08:14,430 - distributed.nanny - INFO - Worker closed
2024-04-19 18:08:14,478 - distributed.core - INFO - Received 'close-stream' from tcp://10.201.1.157:8786; closing.
2024-04-19 18:08:14,479 - distributed.nanny - INFO - Worker closed
2024-04-19 18:08:14,483 - distributed.core - INFO - Received 'close-stream' from tcp://10.201.1.157:8786; closing.
2024-04-19 18:08:14,483 - distributed.nanny - INFO - Worker closed
2024-04-19 18:08:14,494 - distributed.core - INFO - Received 'close-stream' from tcp://10.201.1.157:8786; closing.
2024-04-19 18:08:14,494 - distributed.nanny - INFO - Worker closed
2024-04-19 18:08:14,977 - distributed.worker - ERROR - Failed to communicate with scheduler during heartbeat.
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
tornado.iostream.StreamClosedError: Stream is closed

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/worker.py", line 1252, in heartbeat
    response = await retry_operation(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/utils_comm.py", line 455, in retry_operation
    return await retry(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/utils_comm.py", line 434, in retry
    return await coro()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1395, in send_recv_from_rpc
    return await send_recv(comm=comm, op=key, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1154, in send_recv
    response = await comm.read(deserializers=deserializers)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 236, in read
    convert_stream_closed_error(self, e)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 142, in convert_stream_closed_error
    raise CommClosedError(f"in {obj}: {exc}") from exc
distributed.comm.core.CommClosedError: in <TCP (closed) ConnectionPool.heartbeat_worker local=tcp://10.201.4.43:33594 remote=tcp://10.201.1.157:8786>: Stream is closed
2024-04-19 18:08:14,977 - distributed.worker - ERROR - Failed to communicate with scheduler during heartbeat.
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
tornado.iostream.StreamClosedError: Stream is closed

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/worker.py", line 1252, in heartbeat
    response = await retry_operation(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/utils_comm.py", line 455, in retry_operation
    return await retry(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/utils_comm.py", line 434, in retry
    return await coro()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1395, in send_recv_from_rpc
    return await send_recv(comm=comm, op=key, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1154, in send_recv
    response = await comm.read(deserializers=deserializers)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 236, in read
    convert_stream_closed_error(self, e)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 142, in convert_stream_closed_error
    raise CommClosedError(f"in {obj}: {exc}") from exc
distributed.comm.core.CommClosedError: in <TCP (closed) ConnectionPool.heartbeat_worker local=tcp://10.201.4.43:45248 remote=tcp://10.201.1.157:8786>: Stream is closed
2024-04-19 18:08:14,978 - distributed.worker - ERROR - Failed to communicate with scheduler during heartbeat.
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
tornado.iostream.StreamClosedError: Stream is closed

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/worker.py", line 1252, in heartbeat
    response = await retry_operation(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/utils_comm.py", line 455, in retry_operation
    return await retry(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/utils_comm.py", line 434, in retry
    return await coro()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1395, in send_recv_from_rpc
    return await send_recv(comm=comm, op=key, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1154, in send_recv
    response = await comm.read(deserializers=deserializers)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 236, in read
    convert_stream_closed_error(self, e)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 142, in convert_stream_closed_error
    raise CommClosedError(f"in {obj}: {exc}") from exc
distributed.comm.core.CommClosedError: in <TCP (closed) ConnectionPool.heartbeat_worker local=tcp://10.201.4.43:33582 remote=tcp://10.201.1.157:8786>: Stream is closed
2024-04-19 18:08:14,978 - distributed.worker - ERROR - Failed to communicate with scheduler during heartbeat.
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
tornado.iostream.StreamClosedError: Stream is closed

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/worker.py", line 1252, in heartbeat
    response = await retry_operation(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/utils_comm.py", line 455, in retry_operation
    return await retry(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/utils_comm.py", line 434, in retry
    return await coro()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1395, in send_recv_from_rpc
    return await send_recv(comm=comm, op=key, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1154, in send_recv
    response = await comm.read(deserializers=deserializers)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 236, in read
    convert_stream_closed_error(self, e)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 142, in convert_stream_closed_error
    raise CommClosedError(f"in {obj}: {exc}") from exc
distributed.comm.core.CommClosedError: in <TCP (closed) ConnectionPool.heartbeat_worker local=tcp://10.201.4.43:33604 remote=tcp://10.201.1.157:8786>: Stream is closed
2024-04-19 18:08:14,984 - distributed.core - INFO - Received 'close-stream' from tcp://10.201.1.157:8786; closing.
2024-04-19 18:08:14,984 - distributed.nanny - INFO - Worker closed
2024-04-19 18:08:15,000 - distributed.core - INFO - Received 'close-stream' from tcp://10.201.1.157:8786; closing.
2024-04-19 18:08:15,000 - distributed.nanny - INFO - Worker closed
2024-04-19 18:08:15,043 - distributed.core - INFO - Received 'close-stream' from tcp://10.201.1.157:8786; closing.
2024-04-19 18:08:15,044 - distributed.nanny - INFO - Worker closed
2024-04-19 18:08:15,049 - distributed.core - INFO - Received 'close-stream' from tcp://10.201.1.157:8786; closing.
2024-04-19 18:08:15,050 - distributed.nanny - INFO - Worker closed
2024-04-19 18:08:16,499 - distributed.nanny - ERROR - Worker process died unexpectedly
2024-04-19 18:08:16,499 - distributed.nanny - ERROR - Worker process died unexpectedly
2024-04-19 18:08:16,540 - distributed.nanny - ERROR - Worker process died unexpectedly
2024-04-19 18:08:16,991 - distributed.nanny - ERROR - Worker process died unexpectedly
2024-04-19 18:08:17,060 - distributed.nanny - ERROR - Worker process died unexpectedly
2024-04-19 18:08:17,078 - distributed.nanny - ERROR - Worker process died unexpectedly
2024-04-19 18:08:17,128 - distributed.nanny - ERROR - Worker process died unexpectedly
2024-04-19 18:08:17,748 - distributed.nanny - INFO - Closing Nanny at 'tcp://10.201.4.43:39309'. Reason: nanny-close-gracefully
2024-04-19 18:08:17,749 - distributed.dask_worker - INFO - End worker
2024-04-19 18:08:17,778 - distributed.nanny - INFO - Closing Nanny at 'tcp://10.201.4.43:34507'. Reason: nanny-close-gracefully
2024-04-19 18:08:17,779 - distributed.dask_worker - INFO - End worker
2024-04-19 18:08:17,798 - distributed.nanny - INFO - Closing Nanny at 'tcp://10.201.4.43:37573'. Reason: nanny-close-gracefully
2024-04-19 18:08:17,799 - distributed.dask_worker - INFO - End worker
2024-04-19 18:08:17,824 - distributed.nanny - INFO - Closing Nanny at 'tcp://10.201.1.19:33563'. Reason: nanny-close-gracefully
2024-04-19 18:08:17,825 - distributed.dask_worker - INFO - End worker
2024-04-19 18:08:17,832 - distributed.nanny - INFO - Closing Nanny at 'tcp://10.201.1.19:35353'. Reason: nanny-close-gracefully
2024-04-19 18:08:17,833 - distributed.dask_worker - INFO - End worker
2024-04-19 18:08:17,936 - distributed.nanny - INFO - Closing Nanny at 'tcp://10.201.1.19:42563'. Reason: nanny-close-gracefully
2024-04-19 18:08:17,937 - distributed.dask_worker - INFO - End worker
2024-04-19 18:08:18,258 - distributed.nanny - INFO - Closing Nanny at 'tcp://10.201.4.43:33647'. Reason: nanny-close-gracefully
2024-04-19 18:08:18,259 - distributed.dask_worker - INFO - End worker
2024-04-19 18:08:18,307 - distributed.nanny - INFO - Closing Nanny at 'tcp://10.201.1.19:39087'. Reason: nanny-close-gracefully
2024-04-19 18:08:18,307 - distributed.dask_worker - INFO - End worker
