2024-04-19 19:01:31,233 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-19 19:01:31,453 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-19 19:01:31,454 - distributed.scheduler - INFO - -----------------------------------------------
2024-04-19 19:01:31,462 - distributed.preloading - INFO - Creating preload: MofkaSchedulerPlugin.py
2024-04-19 19:01:31,462 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-19 19:01:31,468 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-19 19:01:32,505 - distributed.http.proxy - INFO - To route to workers diagnostics web server please install jupyter-server-proxy: python -m pip install jupyter-server-proxy
2024-04-19 19:01:32,546 - distributed.scheduler - INFO - State start
2024-04-19 19:01:32,549 - distributed.scheduler - INFO - -----------------------------------------------
/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/utils.py:181: RuntimeWarning: Couldn't detect a suitable IP address for reaching '8.8.8.8', defaulting to hostname: [Errno 101] Network is unreachable
  warnings.warn(
2024-04-19 19:01:32,568 - distributed.scheduler - INFO -   Scheduler at:   tcp://10.201.3.229:8786
2024-04-19 19:01:32,568 - distributed.scheduler - INFO -   dashboard at:  http://10.201.3.229:8787/status
2024-04-19 19:01:32,570 - distributed.preloading - INFO - Run preload setup: MofkaSchedulerPlugin.py
2024-04-19 19:01:33,469 - distributed.scheduler - INFO - Registering Worker plugin shuffle
2024-04-19 19:01:39,772 - distributed.scheduler - INFO - Receive client connection: Client-3fb89f8f-fe7f-11ee-b683-6805cadd5daa
2024-04-19 19:01:42,163 - distributed.core - INFO - Starting established connection to tcp://10.201.3.243:56992
2024-04-19 19:01:42,165 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.251:34939', status: init, memory: 0, processing: 0>
2024-04-19 19:01:42,166 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.251:34939
2024-04-19 19:01:42,166 - distributed.core - INFO - Starting established connection to tcp://10.201.3.251:42738
2024-04-19 19:01:42,167 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.251:37899', status: init, memory: 0, processing: 0>
2024-04-19 19:01:42,167 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.251:37899
2024-04-19 19:01:42,167 - distributed.core - INFO - Starting established connection to tcp://10.201.3.251:42772
2024-04-19 19:01:42,172 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.251:45171', status: init, memory: 0, processing: 0>
2024-04-19 19:01:42,173 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.251:45171
2024-04-19 19:01:42,173 - distributed.core - INFO - Starting established connection to tcp://10.201.3.251:42776
2024-04-19 19:01:42,174 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.251:37849', status: init, memory: 0, processing: 0>
2024-04-19 19:01:42,174 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.251:37849
2024-04-19 19:01:42,174 - distributed.core - INFO - Starting established connection to tcp://10.201.3.251:42722
2024-04-19 19:01:42,175 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.251:41213', status: init, memory: 0, processing: 0>
2024-04-19 19:01:42,175 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.251:41213
2024-04-19 19:01:42,175 - distributed.core - INFO - Starting established connection to tcp://10.201.3.251:42716
2024-04-19 19:01:42,176 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.251:44465', status: init, memory: 0, processing: 0>
2024-04-19 19:01:42,176 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.251:44465
2024-04-19 19:01:42,176 - distributed.core - INFO - Starting established connection to tcp://10.201.3.251:42710
2024-04-19 19:01:42,177 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.251:38881', status: init, memory: 0, processing: 0>
2024-04-19 19:01:42,178 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.251:38881
2024-04-19 19:01:42,178 - distributed.core - INFO - Starting established connection to tcp://10.201.3.251:42758
2024-04-19 19:01:42,178 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.251:43083', status: init, memory: 0, processing: 0>
2024-04-19 19:01:42,179 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.251:43083
2024-04-19 19:01:42,179 - distributed.core - INFO - Starting established connection to tcp://10.201.3.251:42754
2024-04-19 19:01:47,105 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 4.53s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 19:02:20,433 - distributed.scheduler - INFO - Scheduler closing due to unknown reason...
2024-04-19 19:02:20,433 - distributed.scheduler - INFO - Scheduler closing all comms
2024-04-19 19:02:20,435 - distributed.core - INFO - Connection to tcp://10.201.3.251:42738 has been closed.
2024-04-19 19:02:20,435 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.251:34939', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.4353728')
2024-04-19 19:02:20,456 - distributed.core - INFO - Connection to tcp://10.201.3.251:42772 has been closed.
2024-04-19 19:02:20,456 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.251:37899', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.4567301')
2024-04-19 19:02:20,457 - distributed.core - INFO - Connection to tcp://10.201.3.251:42776 has been closed.
2024-04-19 19:02:20,457 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.251:45171', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.457356')
2024-04-19 19:02:20,457 - distributed.core - INFO - Connection to tcp://10.201.3.251:42722 has been closed.
2024-04-19 19:02:20,457 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.251:37849', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.4579499')
2024-04-19 19:02:20,458 - distributed.core - INFO - Connection to tcp://10.201.3.251:42716 has been closed.
2024-04-19 19:02:20,458 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.251:41213', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.4585042')
2024-04-19 19:02:20,458 - distributed.core - INFO - Connection to tcp://10.201.3.251:42710 has been closed.
2024-04-19 19:02:20,459 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.251:44465', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.459046')
2024-04-19 19:02:20,459 - distributed.core - INFO - Connection to tcp://10.201.3.251:42758 has been closed.
2024-04-19 19:02:20,459 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.251:38881', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.45957')
2024-04-19 19:02:20,460 - distributed.core - INFO - Connection to tcp://10.201.3.251:42754 has been closed.
2024-04-19 19:02:20,460 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.251:43083', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.4601321')
2024-04-19 19:02:20,460 - distributed.scheduler - INFO - Lost all workers
2024-04-19 19:02:20,462 - distributed.scheduler - INFO - Stopped scheduler at 'tcp://10.201.3.229:8786'
2024-04-19 19:02:20,462 - distributed.scheduler - INFO - End scheduler
