2024-04-18 19:26:30,242 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-18 19:26:30,445 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-18 19:26:30,446 - distributed.scheduler - INFO - -----------------------------------------------
2024-04-18 19:26:30,453 - distributed.preloading - INFO - Creating preload: MofkaSchedulerPlugin.py
2024-04-18 19:26:30,453 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-18 19:26:30,460 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-18 19:26:31,446 - distributed.http.proxy - INFO - To route to workers diagnostics web server please install jupyter-server-proxy: python -m pip install jupyter-server-proxy
2024-04-18 19:26:31,486 - distributed.scheduler - INFO - State start
2024-04-18 19:26:31,489 - distributed.scheduler - INFO - -----------------------------------------------
/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/utils.py:181: RuntimeWarning: Couldn't detect a suitable IP address for reaching '8.8.8.8', defaulting to hostname: [Errno 101] Network is unreachable
  warnings.warn(
2024-04-18 19:26:31,508 - distributed.scheduler - INFO -   Scheduler at:   tcp://10.201.0.172:8786
2024-04-18 19:26:31,508 - distributed.scheduler - INFO -   dashboard at:  http://10.201.0.172:8787/status
2024-04-18 19:26:31,511 - distributed.preloading - INFO - Run preload setup: MofkaSchedulerPlugin.py
2024-04-18 19:26:32,325 - distributed.scheduler - INFO - Registering Worker plugin shuffle
2024-04-18 19:26:39,909 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.0.158:33417', status: init, memory: 0, processing: 0>
2024-04-18 19:26:41,750 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.0.158:33417
2024-04-18 19:26:41,750 - distributed.core - INFO - Starting established connection to tcp://10.201.0.158:34158
2024-04-18 19:26:41,752 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.0.158:43427', status: init, memory: 0, processing: 0>
2024-04-18 19:26:41,752 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.0.158:43427
2024-04-18 19:26:41,752 - distributed.core - INFO - Starting established connection to tcp://10.201.0.158:34150
2024-04-18 19:26:41,755 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.0.158:39617', status: init, memory: 0, processing: 0>
2024-04-18 19:26:41,756 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.0.158:39617
2024-04-18 19:26:41,756 - distributed.core - INFO - Starting established connection to tcp://10.201.0.158:34152
2024-04-18 19:26:41,757 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.0.158:46717', status: init, memory: 0, processing: 0>
2024-04-18 19:26:41,758 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.0.158:46717
2024-04-18 19:26:41,758 - distributed.core - INFO - Starting established connection to tcp://10.201.0.158:34164
2024-04-18 19:26:41,758 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.0.156:38095', status: init, memory: 0, processing: 0>
2024-04-18 19:26:41,759 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.0.156:38095
2024-04-18 19:26:41,759 - distributed.core - INFO - Starting established connection to tcp://10.201.0.156:42510
2024-04-18 19:26:41,760 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.0.156:37403', status: init, memory: 0, processing: 0>
2024-04-18 19:26:41,760 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.0.156:37403
2024-04-18 19:26:41,760 - distributed.core - INFO - Starting established connection to tcp://10.201.0.156:42500
2024-04-18 19:26:41,761 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.0.156:36633', status: init, memory: 0, processing: 0>
2024-04-18 19:26:41,761 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.0.156:36633
2024-04-18 19:26:41,761 - distributed.core - INFO - Starting established connection to tcp://10.201.0.156:42516
2024-04-18 19:26:41,762 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.0.156:34625', status: init, memory: 0, processing: 0>
2024-04-18 19:26:41,762 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.0.156:34625
2024-04-18 19:26:41,762 - distributed.core - INFO - Starting established connection to tcp://10.201.0.156:42488
2024-04-18 19:26:46,790 - distributed.scheduler - INFO - Receive client connection: Client-9799727f-fdb9-11ee-ae51-6805cae13198
2024-04-18 19:26:46,791 - distributed.core - INFO - Starting established connection to tcp://10.201.0.175:54824
2024-04-18 19:26:59,353 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 11.43s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 19:27:16,009 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 16.60s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 19:30:13,525 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 26.14s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 19:30:13,613 - distributed.scheduler - INFO - Scheduler closing due to unknown reason...
2024-04-18 19:30:13,615 - distributed.scheduler - INFO - Scheduler closing all comms
2024-04-18 19:30:13,616 - distributed.core - INFO - Connection to tcp://10.201.0.158:34150 has been closed.
2024-04-18 19:30:13,616 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.0.158:43427', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468613.6165175')
2024-04-18 19:30:14,122 - distributed.core - INFO - Connection to tcp://10.201.0.158:34158 has been closed.
2024-04-18 19:30:14,122 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.0.158:33417', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468614.1222813')
2024-04-18 19:30:14,123 - distributed.core - INFO - Connection to tcp://10.201.0.158:34152 has been closed.
2024-04-18 19:30:14,123 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.0.158:39617', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468614.1230643')
2024-04-18 19:30:14,123 - distributed.core - INFO - Connection to tcp://10.201.0.158:34164 has been closed.
2024-04-18 19:30:14,123 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.0.158:46717', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468614.1235588')
2024-04-18 19:30:14,124 - distributed.core - INFO - Connection to tcp://10.201.0.156:42510 has been closed.
2024-04-18 19:30:14,124 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.0.156:38095', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468614.1241186')
2024-04-18 19:30:14,124 - distributed.core - INFO - Connection to tcp://10.201.0.156:42500 has been closed.
2024-04-18 19:30:14,124 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.0.156:37403', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468614.1246538')
2024-04-18 19:30:14,125 - distributed.core - INFO - Connection to tcp://10.201.0.156:42516 has been closed.
2024-04-18 19:30:14,125 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.0.156:36633', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468614.1251504')
2024-04-18 19:30:14,125 - distributed.core - INFO - Connection to tcp://10.201.0.156:42488 has been closed.
2024-04-18 19:30:14,125 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.0.156:34625', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468614.1256788')
2024-04-18 19:30:14,126 - distributed.scheduler - INFO - Lost all workers
2024-04-18 19:30:14,128 - distributed.scheduler - INFO - Stopped scheduler at 'tcp://10.201.0.172:8786'
2024-04-18 19:30:14,128 - distributed.scheduler - INFO - End scheduler
