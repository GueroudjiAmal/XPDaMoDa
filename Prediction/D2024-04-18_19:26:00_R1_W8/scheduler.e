2024-04-18 19:26:17,807 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-18 19:26:17,880 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-18 19:26:17,881 - distributed.scheduler - INFO - -----------------------------------------------
2024-04-18 19:26:17,885 - distributed.preloading - INFO - Creating preload: MofkaSchedulerPlugin.py
2024-04-18 19:26:17,885 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-18 19:26:17,891 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-18 19:26:18,446 - distributed.http.proxy - INFO - To route to workers diagnostics web server please install jupyter-server-proxy: python -m pip install jupyter-server-proxy
2024-04-18 19:26:18,483 - distributed.scheduler - INFO - State start
2024-04-18 19:26:18,486 - distributed.scheduler - INFO - -----------------------------------------------
/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/utils.py:181: RuntimeWarning: Couldn't detect a suitable IP address for reaching '8.8.8.8', defaulting to hostname: [Errno 101] Network is unreachable
  warnings.warn(
2024-04-18 19:26:18,490 - distributed.scheduler - INFO -   Scheduler at:   tcp://10.201.3.231:8786
2024-04-18 19:26:18,490 - distributed.scheduler - INFO -   dashboard at:  http://10.201.3.231:8787/status
2024-04-18 19:26:18,493 - distributed.preloading - INFO - Run preload setup: MofkaSchedulerPlugin.py
2024-04-18 19:26:19,370 - distributed.scheduler - INFO - Registering Worker plugin shuffle
2024-04-18 19:26:22,841 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.228:37899', status: init, memory: 0, processing: 0>
2024-04-18 19:26:23,382 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.228:37899
2024-04-18 19:26:23,383 - distributed.core - INFO - Starting established connection to tcp://10.201.3.228:42248
2024-04-18 19:26:23,384 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.228:35987', status: init, memory: 0, processing: 0>
2024-04-18 19:26:23,385 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.228:35987
2024-04-18 19:26:23,385 - distributed.core - INFO - Starting established connection to tcp://10.201.3.228:42218
2024-04-18 19:26:23,386 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.228:42711', status: init, memory: 0, processing: 0>
2024-04-18 19:26:23,386 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.228:42711
2024-04-18 19:26:23,386 - distributed.core - INFO - Starting established connection to tcp://10.201.3.228:42234
2024-04-18 19:26:23,387 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.228:35275', status: init, memory: 0, processing: 0>
2024-04-18 19:26:23,387 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.228:35275
2024-04-18 19:26:23,387 - distributed.core - INFO - Starting established connection to tcp://10.201.3.228:42208
2024-04-18 19:26:24,209 - distributed.scheduler - INFO - Receive client connection: Client-8ae72771-fdb9-11ee-8368-6805cad940cc
2024-04-18 19:26:24,210 - distributed.core - INFO - Starting established connection to tcp://10.201.3.236:50622
2024-04-18 19:26:29,478 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.244:38483', status: init, memory: 0, processing: 0>
2024-04-18 19:26:29,478 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.244:38483
2024-04-18 19:26:29,478 - distributed.core - INFO - Starting established connection to tcp://10.201.3.244:42792
2024-04-18 19:26:29,479 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.244:43033', status: init, memory: 0, processing: 0>
2024-04-18 19:26:29,485 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.244:43033
2024-04-18 19:26:29,485 - distributed.core - INFO - Starting established connection to tcp://10.201.3.244:42770
2024-04-18 19:26:29,486 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.244:33309', status: init, memory: 0, processing: 0>
2024-04-18 19:26:29,491 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.244:33309
2024-04-18 19:26:29,491 - distributed.core - INFO - Starting established connection to tcp://10.201.3.244:42808
2024-04-18 19:26:29,493 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.244:37533', status: init, memory: 0, processing: 0>
2024-04-18 19:26:29,498 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.244:37533
2024-04-18 19:26:29,498 - distributed.core - INFO - Starting established connection to tcp://10.201.3.244:42782
2024-04-18 19:26:29,499 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 4.43s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 19:26:45,924 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 16.36s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 19:29:34,950 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 25.74s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 19:29:35,073 - distributed.scheduler - INFO - Scheduler closing due to unknown reason...
2024-04-18 19:29:35,073 - distributed.scheduler - INFO - Scheduler closing all comms
2024-04-18 19:29:35,074 - distributed.core - INFO - Connection to tcp://10.201.3.228:42248 has been closed.
2024-04-18 19:29:35,075 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.228:37899', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468575.0750477')
2024-04-18 19:29:35,577 - distributed.core - INFO - Connection to tcp://10.201.3.228:42218 has been closed.
2024-04-18 19:29:35,577 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.228:35987', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468575.5778608')
2024-04-18 19:29:35,578 - distributed.core - INFO - Connection to tcp://10.201.3.228:42234 has been closed.
2024-04-18 19:29:35,578 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.228:42711', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468575.5784907')
2024-04-18 19:29:35,579 - distributed.core - INFO - Connection to tcp://10.201.3.228:42208 has been closed.
2024-04-18 19:29:35,579 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.228:35275', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468575.5790985')
2024-04-18 19:29:35,579 - distributed.core - INFO - Connection to tcp://10.201.3.244:42792 has been closed.
2024-04-18 19:29:35,579 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.244:38483', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468575.5796697')
2024-04-18 19:29:35,580 - distributed.core - INFO - Connection to tcp://10.201.3.244:42770 has been closed.
2024-04-18 19:29:35,580 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.244:43033', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468575.5801823')
2024-04-18 19:29:35,580 - distributed.core - INFO - Connection to tcp://10.201.3.244:42808 has been closed.
2024-04-18 19:29:35,580 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.244:33309', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468575.5807402')
2024-04-18 19:29:35,581 - distributed.core - INFO - Connection to tcp://10.201.3.244:42782 has been closed.
2024-04-18 19:29:35,581 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.244:37533', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468575.5812535')
2024-04-18 19:29:35,581 - distributed.scheduler - INFO - Lost all workers
2024-04-18 19:29:35,583 - distributed.scheduler - INFO - Stopped scheduler at 'tcp://10.201.3.231:8786'
2024-04-18 19:29:35,584 - distributed.scheduler - INFO - End scheduler
