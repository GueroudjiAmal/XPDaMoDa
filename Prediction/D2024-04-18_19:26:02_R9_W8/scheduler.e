2024-04-18 19:26:30,630 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-18 19:26:30,835 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-18 19:26:30,836 - distributed.scheduler - INFO - -----------------------------------------------
2024-04-18 19:26:30,844 - distributed.preloading - INFO - Creating preload: MofkaSchedulerPlugin.py
2024-04-18 19:26:30,844 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-18 19:26:30,850 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-18 19:26:31,833 - distributed.http.proxy - INFO - To route to workers diagnostics web server please install jupyter-server-proxy: python -m pip install jupyter-server-proxy
2024-04-18 19:26:31,873 - distributed.scheduler - INFO - State start
2024-04-18 19:26:31,877 - distributed.scheduler - INFO - -----------------------------------------------
/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/utils.py:181: RuntimeWarning: Couldn't detect a suitable IP address for reaching '8.8.8.8', defaulting to hostname: [Errno 101] Network is unreachable
  warnings.warn(
2024-04-18 19:26:31,896 - distributed.scheduler - INFO -   Scheduler at:    tcp://10.201.1.22:8786
2024-04-18 19:26:31,896 - distributed.scheduler - INFO -   dashboard at:  http://10.201.1.22:8787/status
2024-04-18 19:26:31,898 - distributed.preloading - INFO - Run preload setup: MofkaSchedulerPlugin.py
2024-04-18 19:26:32,739 - distributed.scheduler - INFO - Registering Worker plugin shuffle
2024-04-18 19:26:40,341 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.1.29:39669', status: init, memory: 0, processing: 0>
2024-04-18 19:26:42,228 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.1.29:39669
2024-04-18 19:26:42,228 - distributed.core - INFO - Starting established connection to tcp://10.201.1.29:34548
2024-04-18 19:26:42,229 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.1.29:39065', status: init, memory: 0, processing: 0>
2024-04-18 19:26:42,230 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.1.29:39065
2024-04-18 19:26:42,230 - distributed.core - INFO - Starting established connection to tcp://10.201.1.29:34540
2024-04-18 19:26:42,233 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.1.29:37083', status: init, memory: 0, processing: 0>
2024-04-18 19:26:42,233 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.1.29:37083
2024-04-18 19:26:42,233 - distributed.core - INFO - Starting established connection to tcp://10.201.1.29:34528
2024-04-18 19:26:42,234 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.1.29:41069', status: init, memory: 0, processing: 0>
2024-04-18 19:26:42,234 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.1.29:41069
2024-04-18 19:26:42,234 - distributed.core - INFO - Starting established connection to tcp://10.201.1.29:34526
2024-04-18 19:26:42,235 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.1.26:40385', status: init, memory: 0, processing: 0>
2024-04-18 19:26:42,236 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.1.26:40385
2024-04-18 19:26:42,236 - distributed.core - INFO - Starting established connection to tcp://10.201.1.26:59180
2024-04-18 19:26:42,237 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.1.26:44361', status: init, memory: 0, processing: 0>
2024-04-18 19:26:42,237 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.1.26:44361
2024-04-18 19:26:42,237 - distributed.core - INFO - Starting established connection to tcp://10.201.1.26:59184
2024-04-18 19:26:42,238 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.1.26:37515', status: init, memory: 0, processing: 0>
2024-04-18 19:26:42,238 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.1.26:37515
2024-04-18 19:26:42,238 - distributed.core - INFO - Starting established connection to tcp://10.201.1.26:59154
2024-04-18 19:26:42,239 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.1.26:43739', status: init, memory: 0, processing: 0>
2024-04-18 19:26:42,239 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.1.26:43739
2024-04-18 19:26:42,239 - distributed.core - INFO - Starting established connection to tcp://10.201.1.26:59164
2024-04-18 19:26:47,638 - distributed.scheduler - INFO - Receive client connection: Client-981692a6-fdb9-11ee-8a8b-6805cae12f6c
2024-04-18 19:26:47,639 - distributed.core - INFO - Starting established connection to tcp://10.201.1.24:54362
2024-04-18 19:27:00,546 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 11.77s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 19:27:17,366 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 16.76s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 19:30:13,453 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 25.63s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 19:30:13,598 - distributed.scheduler - INFO - Scheduler closing due to unknown reason...
2024-04-18 19:30:13,600 - distributed.scheduler - INFO - Scheduler closing all comms
2024-04-18 19:30:13,601 - distributed.core - INFO - Connection to tcp://10.201.1.29:34548 has been closed.
2024-04-18 19:30:13,601 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.1.29:39669', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468613.6015294')
2024-04-18 19:30:14,098 - distributed.core - INFO - Connection to tcp://10.201.1.29:34540 has been closed.
2024-04-18 19:30:14,098 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.1.29:39065', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468614.098877')
2024-04-18 19:30:14,099 - distributed.core - INFO - Connection to tcp://10.201.1.29:34528 has been closed.
2024-04-18 19:30:14,099 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.1.29:37083', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468614.0995572')
2024-04-18 19:30:14,100 - distributed.core - INFO - Connection to tcp://10.201.1.29:34526 has been closed.
2024-04-18 19:30:14,100 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.1.29:41069', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468614.1001127')
2024-04-18 19:30:14,100 - distributed.core - INFO - Connection to tcp://10.201.1.26:59180 has been closed.
2024-04-18 19:30:14,100 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.1.26:40385', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468614.1006534')
2024-04-18 19:30:14,101 - distributed.core - INFO - Connection to tcp://10.201.1.26:59184 has been closed.
2024-04-18 19:30:14,101 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.1.26:44361', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468614.1011775')
2024-04-18 19:30:14,101 - distributed.core - INFO - Connection to tcp://10.201.1.26:59154 has been closed.
2024-04-18 19:30:14,101 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.1.26:37515', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468614.1016932')
2024-04-18 19:30:14,102 - distributed.core - INFO - Connection to tcp://10.201.1.26:59164 has been closed.
2024-04-18 19:30:14,102 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.1.26:43739', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468614.1022086')
2024-04-18 19:30:14,102 - distributed.scheduler - INFO - Lost all workers
2024-04-18 19:30:14,104 - distributed.scheduler - INFO - Stopped scheduler at 'tcp://10.201.1.22:8786'
2024-04-18 19:30:14,105 - distributed.scheduler - INFO - End scheduler
