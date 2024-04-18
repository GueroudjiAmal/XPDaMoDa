2024-04-18 19:26:30,242 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-18 19:26:30,467 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-18 19:26:30,468 - distributed.scheduler - INFO - -----------------------------------------------
2024-04-18 19:26:30,476 - distributed.preloading - INFO - Creating preload: MofkaSchedulerPlugin.py
2024-04-18 19:26:30,476 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-18 19:26:30,482 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-18 19:26:31,455 - distributed.http.proxy - INFO - To route to workers diagnostics web server please install jupyter-server-proxy: python -m pip install jupyter-server-proxy
2024-04-18 19:26:31,495 - distributed.scheduler - INFO - State start
2024-04-18 19:26:31,498 - distributed.scheduler - INFO - -----------------------------------------------
/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/utils.py:181: RuntimeWarning: Couldn't detect a suitable IP address for reaching '8.8.8.8', defaulting to hostname: [Errno 101] Network is unreachable
  warnings.warn(
2024-04-18 19:26:31,517 - distributed.scheduler - INFO -   Scheduler at:    tcp://10.201.3.97:8786
2024-04-18 19:26:31,517 - distributed.scheduler - INFO -   dashboard at:  http://10.201.3.97:8787/status
2024-04-18 19:26:31,520 - distributed.preloading - INFO - Run preload setup: MofkaSchedulerPlugin.py
2024-04-18 19:26:32,343 - distributed.scheduler - INFO - Registering Worker plugin shuffle
2024-04-18 19:26:38,714 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.105:45205', status: init, memory: 0, processing: 0>
2024-04-18 19:26:40,531 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.105:45205
2024-04-18 19:26:40,531 - distributed.core - INFO - Starting established connection to tcp://10.201.3.105:43040
2024-04-18 19:26:40,532 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.105:45285', status: init, memory: 0, processing: 0>
2024-04-18 19:26:40,533 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.105:45285
2024-04-18 19:26:40,533 - distributed.core - INFO - Starting established connection to tcp://10.201.3.105:43068
2024-04-18 19:26:40,535 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.105:40421', status: init, memory: 0, processing: 0>
2024-04-18 19:26:40,536 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.105:40421
2024-04-18 19:26:40,536 - distributed.core - INFO - Starting established connection to tcp://10.201.3.105:43034
2024-04-18 19:26:40,537 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.105:36179', status: init, memory: 0, processing: 0>
2024-04-18 19:26:40,537 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.105:36179
2024-04-18 19:26:40,537 - distributed.core - INFO - Starting established connection to tcp://10.201.3.105:43056
2024-04-18 19:26:40,538 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.107:38293', status: init, memory: 0, processing: 0>
2024-04-18 19:26:40,539 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.107:38293
2024-04-18 19:26:40,539 - distributed.core - INFO - Starting established connection to tcp://10.201.3.107:34474
2024-04-18 19:26:40,539 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.107:37823', status: init, memory: 0, processing: 0>
2024-04-18 19:26:40,540 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.107:37823
2024-04-18 19:26:40,540 - distributed.core - INFO - Starting established connection to tcp://10.201.3.107:34454
2024-04-18 19:26:40,540 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.107:45307', status: init, memory: 0, processing: 0>
2024-04-18 19:26:40,541 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.107:45307
2024-04-18 19:26:40,541 - distributed.core - INFO - Starting established connection to tcp://10.201.3.107:34468
2024-04-18 19:26:40,542 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.107:43809', status: init, memory: 0, processing: 0>
2024-04-18 19:26:40,542 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.107:43809
2024-04-18 19:26:40,542 - distributed.core - INFO - Starting established connection to tcp://10.201.3.107:34466
2024-04-18 19:26:47,127 - distributed.scheduler - INFO - Receive client connection: Client-97c3a1ca-fdb9-11ee-ba64-6805cacca872
2024-04-18 19:26:47,128 - distributed.core - INFO - Starting established connection to tcp://10.201.3.99:39066
2024-04-18 19:26:59,686 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 11.40s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 19:27:16,035 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 16.31s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 19:30:10,589 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 26.54s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 19:30:10,600 - distributed.scheduler - INFO - Scheduler closing due to unknown reason...
2024-04-18 19:30:10,602 - distributed.scheduler - INFO - Scheduler closing all comms
2024-04-18 19:30:10,603 - distributed.core - INFO - Connection to tcp://10.201.3.105:43040 has been closed.
2024-04-18 19:30:10,604 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.105:45205', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468610.6040227')
2024-04-18 19:30:11,116 - distributed.core - INFO - Connection to tcp://10.201.3.105:43068 has been closed.
2024-04-18 19:30:11,116 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.105:45285', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468611.1166556')
2024-04-18 19:30:11,117 - distributed.core - INFO - Connection to tcp://10.201.3.105:43034 has been closed.
2024-04-18 19:30:11,117 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.105:40421', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468611.117339')
2024-04-18 19:30:11,117 - distributed.core - INFO - Connection to tcp://10.201.3.105:43056 has been closed.
2024-04-18 19:30:11,117 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.105:36179', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468611.117873')
2024-04-18 19:30:11,118 - distributed.core - INFO - Connection to tcp://10.201.3.107:34474 has been closed.
2024-04-18 19:30:11,118 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.107:38293', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468611.118433')
2024-04-18 19:30:11,118 - distributed.core - INFO - Connection to tcp://10.201.3.107:34454 has been closed.
2024-04-18 19:30:11,118 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.107:37823', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468611.1189485')
2024-04-18 19:30:11,119 - distributed.core - INFO - Connection to tcp://10.201.3.107:34468 has been closed.
2024-04-18 19:30:11,119 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.107:45307', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468611.1194587')
2024-04-18 19:30:11,119 - distributed.core - INFO - Connection to tcp://10.201.3.107:34466 has been closed.
2024-04-18 19:30:11,119 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.107:43809', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468611.1199436')
2024-04-18 19:30:11,120 - distributed.scheduler - INFO - Lost all workers
2024-04-18 19:30:11,122 - distributed.scheduler - INFO - Stopped scheduler at 'tcp://10.201.3.97:8786'
2024-04-18 19:30:11,122 - distributed.scheduler - INFO - End scheduler
