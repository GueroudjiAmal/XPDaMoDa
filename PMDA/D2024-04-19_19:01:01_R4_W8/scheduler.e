2024-04-19 19:01:30,917 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-19 19:01:31,158 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-19 19:01:31,159 - distributed.scheduler - INFO - -----------------------------------------------
2024-04-19 19:01:31,167 - distributed.preloading - INFO - Creating preload: MofkaSchedulerPlugin.py
2024-04-19 19:01:31,167 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-19 19:01:31,173 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-19 19:01:32,164 - distributed.http.proxy - INFO - To route to workers diagnostics web server please install jupyter-server-proxy: python -m pip install jupyter-server-proxy
2024-04-19 19:01:32,204 - distributed.scheduler - INFO - State start
2024-04-19 19:01:32,207 - distributed.scheduler - INFO - -----------------------------------------------
/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/utils.py:181: RuntimeWarning: Couldn't detect a suitable IP address for reaching '8.8.8.8', defaulting to hostname: [Errno 101] Network is unreachable
  warnings.warn(
2024-04-19 19:01:32,225 - distributed.scheduler - INFO -   Scheduler at:   tcp://10.201.3.238:8786
2024-04-19 19:01:32,225 - distributed.scheduler - INFO -   dashboard at:  http://10.201.3.238:8787/status
2024-04-19 19:01:32,227 - distributed.preloading - INFO - Run preload setup: MofkaSchedulerPlugin.py
2024-04-19 19:01:33,138 - distributed.scheduler - INFO - Registering Worker plugin shuffle
2024-04-19 19:01:39,653 - distributed.scheduler - INFO - Receive client connection: Client-3fa72df9-fe7f-11ee-8d48-6805cad940cc
2024-04-19 19:01:41,985 - distributed.core - INFO - Starting established connection to tcp://10.201.3.236:52258
2024-04-19 19:01:41,988 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.228:41779', status: init, memory: 0, processing: 0>
2024-04-19 19:01:41,988 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.228:41779
2024-04-19 19:01:41,988 - distributed.core - INFO - Starting established connection to tcp://10.201.3.228:60812
2024-04-19 19:01:41,989 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.228:36429', status: init, memory: 0, processing: 0>
2024-04-19 19:01:41,989 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.228:36429
2024-04-19 19:01:41,989 - distributed.core - INFO - Starting established connection to tcp://10.201.3.228:60810
2024-04-19 19:01:41,995 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.228:45419', status: init, memory: 0, processing: 0>
2024-04-19 19:01:41,995 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.228:45419
2024-04-19 19:01:41,995 - distributed.core - INFO - Starting established connection to tcp://10.201.3.228:60848
2024-04-19 19:01:41,996 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.228:35537', status: init, memory: 0, processing: 0>
2024-04-19 19:01:41,996 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.228:35537
2024-04-19 19:01:41,996 - distributed.core - INFO - Starting established connection to tcp://10.201.3.228:60806
2024-04-19 19:01:41,997 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.228:33249', status: init, memory: 0, processing: 0>
2024-04-19 19:01:41,997 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.228:33249
2024-04-19 19:01:41,997 - distributed.core - INFO - Starting established connection to tcp://10.201.3.228:60820
2024-04-19 19:01:41,998 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.228:38435', status: init, memory: 0, processing: 0>
2024-04-19 19:01:41,999 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.228:38435
2024-04-19 19:01:41,999 - distributed.core - INFO - Starting established connection to tcp://10.201.3.228:60818
2024-04-19 19:01:42,000 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.228:43113', status: init, memory: 0, processing: 0>
2024-04-19 19:01:42,000 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.228:43113
2024-04-19 19:01:42,000 - distributed.core - INFO - Starting established connection to tcp://10.201.3.228:60832
2024-04-19 19:01:42,001 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.228:44877', status: init, memory: 0, processing: 0>
2024-04-19 19:01:42,001 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.228:44877
2024-04-19 19:01:42,001 - distributed.core - INFO - Starting established connection to tcp://10.201.3.228:60862
2024-04-19 19:01:46,843 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 4.44s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 19:02:20,257 - distributed.scheduler - INFO - Scheduler closing due to unknown reason...
2024-04-19 19:02:20,257 - distributed.scheduler - INFO - Scheduler closing all comms
2024-04-19 19:02:20,259 - distributed.core - INFO - Connection to tcp://10.201.3.228:60812 has been closed.
2024-04-19 19:02:20,259 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.228:41779', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.2591672')
2024-04-19 19:02:20,281 - distributed.core - INFO - Connection to tcp://10.201.3.228:60810 has been closed.
2024-04-19 19:02:20,281 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.228:36429', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.2815049')
2024-04-19 19:02:20,282 - distributed.core - INFO - Connection to tcp://10.201.3.228:60848 has been closed.
2024-04-19 19:02:20,282 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.228:45419', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.282214')
2024-04-19 19:02:20,282 - distributed.core - INFO - Connection to tcp://10.201.3.228:60806 has been closed.
2024-04-19 19:02:20,282 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.228:35537', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.282739')
2024-04-19 19:02:20,283 - distributed.core - INFO - Connection to tcp://10.201.3.228:60820 has been closed.
2024-04-19 19:02:20,283 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.228:33249', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.283212')
2024-04-19 19:02:20,283 - distributed.core - INFO - Connection to tcp://10.201.3.228:60818 has been closed.
2024-04-19 19:02:20,283 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.228:38435', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.2836897')
2024-04-19 19:02:20,284 - distributed.core - INFO - Connection to tcp://10.201.3.228:60832 has been closed.
2024-04-19 19:02:20,284 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.228:43113', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.2841918')
2024-04-19 19:02:20,284 - distributed.core - INFO - Connection to tcp://10.201.3.228:60862 has been closed.
2024-04-19 19:02:20,284 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.228:44877', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.2846994')
2024-04-19 19:02:20,285 - distributed.scheduler - INFO - Lost all workers
2024-04-19 19:02:20,286 - distributed.scheduler - INFO - Stopped scheduler at 'tcp://10.201.3.238:8786'
2024-04-19 19:02:20,287 - distributed.scheduler - INFO - End scheduler
