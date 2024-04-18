2024-04-18 15:58:27,198 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-18 15:58:27,250 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-18 15:58:27,251 - distributed.scheduler - INFO - -----------------------------------------------
2024-04-18 15:58:27,255 - distributed.preloading - INFO - Creating preload: MofkaSchedulerPlugin.py
2024-04-18 15:58:27,256 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-18 15:58:27,261 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-18 15:58:27,828 - distributed.http.proxy - INFO - To route to workers diagnostics web server please install jupyter-server-proxy: python -m pip install jupyter-server-proxy
2024-04-18 15:58:27,865 - distributed.scheduler - INFO - State start
2024-04-18 15:58:27,869 - distributed.scheduler - INFO - -----------------------------------------------
/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/utils.py:181: RuntimeWarning: Couldn't detect a suitable IP address for reaching '8.8.8.8', defaulting to hostname: [Errno 101] Network is unreachable
  warnings.warn(
2024-04-18 15:58:27,873 - distributed.scheduler - INFO -   Scheduler at:   tcp://10.201.0.221:8786
2024-04-18 15:58:27,873 - distributed.scheduler - INFO -   dashboard at:  http://10.201.0.221:8787/status
2024-04-18 15:58:27,875 - distributed.preloading - INFO - Run preload setup: MofkaSchedulerPlugin.py
2024-04-18 15:58:28,659 - distributed.scheduler - INFO - Registering Worker plugin shuffle
2024-04-18 15:58:31,287 - distributed.scheduler - INFO - Receive client connection: Client-80cd22dc-fd9c-11ee-aa60-6805cae12036
2024-04-18 15:58:31,881 - distributed.core - INFO - Starting established connection to tcp://10.201.0.223:37390
2024-04-18 15:58:33,490 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.0.212:42235', status: init, memory: 0, processing: 0>
2024-04-18 15:58:33,495 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.0.212:42235
2024-04-18 15:58:33,496 - distributed.core - INFO - Starting established connection to tcp://10.201.0.212:48238
2024-04-18 15:58:33,497 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.0.212:41943', status: init, memory: 0, processing: 0>
2024-04-18 15:58:33,503 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.0.212:41943
2024-04-18 15:58:33,503 - distributed.core - INFO - Starting established connection to tcp://10.201.0.212:48210
2024-04-18 15:58:33,504 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.0.212:43403', status: init, memory: 0, processing: 0>
2024-04-18 15:58:33,504 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.0.212:43403
2024-04-18 15:58:33,504 - distributed.core - INFO - Starting established connection to tcp://10.201.0.212:48244
2024-04-18 15:58:33,505 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.0.212:42529', status: init, memory: 0, processing: 0>
2024-04-18 15:58:33,511 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.0.212:42529
2024-04-18 15:58:33,511 - distributed.core - INFO - Starting established connection to tcp://10.201.0.212:48222
2024-04-18 15:58:33,512 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.0.229:37575', status: init, memory: 0, processing: 0>
2024-04-18 15:58:33,518 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.0.229:37575
2024-04-18 15:58:33,518 - distributed.core - INFO - Starting established connection to tcp://10.201.0.229:38802
2024-04-18 15:58:33,524 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.0.229:43473', status: init, memory: 0, processing: 0>
2024-04-18 15:58:33,529 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.0.229:43473
2024-04-18 15:58:33,529 - distributed.core - INFO - Starting established connection to tcp://10.201.0.229:38788
2024-04-18 15:58:33,530 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.0.229:40493', status: init, memory: 0, processing: 0>
2024-04-18 15:58:33,531 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.0.229:40493
2024-04-18 15:58:33,531 - distributed.core - INFO - Starting established connection to tcp://10.201.0.229:38796
2024-04-18 15:58:33,531 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.0.229:41909', status: init, memory: 0, processing: 0>
2024-04-18 15:58:33,532 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.0.229:41909
2024-04-18 15:58:33,532 - distributed.core - INFO - Starting established connection to tcp://10.201.0.229:38792
2024-04-18 15:59:23,153 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 8.39s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 16:00:24,981 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 9.32s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 16:00:37,088 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 12.03s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 16:01:46,472 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 9.11s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 16:01:46,484 - distributed.scheduler - INFO - Scheduler closing due to unknown reason...
2024-04-18 16:01:46,485 - distributed.scheduler - INFO - Scheduler closing all comms
2024-04-18 16:01:46,487 - distributed.core - INFO - Connection to tcp://10.201.0.212:48238 has been closed.
2024-04-18 16:01:46,487 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.0.212:42235', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713456106.487078')
2024-04-18 16:01:47,020 - distributed.core - INFO - Connection to tcp://10.201.0.212:48210 has been closed.
2024-04-18 16:01:47,020 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.0.212:41943', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713456107.0202296')
2024-04-18 16:01:47,020 - distributed.core - INFO - Connection to tcp://10.201.0.212:48244 has been closed.
2024-04-18 16:01:47,020 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.0.212:43403', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713456107.0208666')
2024-04-18 16:01:47,021 - distributed.core - INFO - Connection to tcp://10.201.0.212:48222 has been closed.
2024-04-18 16:01:47,021 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.0.212:42529', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713456107.0214188')
2024-04-18 16:01:47,021 - distributed.core - INFO - Connection to tcp://10.201.0.229:38802 has been closed.
2024-04-18 16:01:47,021 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.0.229:37575', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713456107.0219162')
2024-04-18 16:01:47,022 - distributed.core - INFO - Connection to tcp://10.201.0.229:38788 has been closed.
2024-04-18 16:01:47,022 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.0.229:43473', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713456107.0224056')
2024-04-18 16:01:47,022 - distributed.core - INFO - Connection to tcp://10.201.0.229:38796 has been closed.
2024-04-18 16:01:47,023 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.0.229:40493', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713456107.02305')
2024-04-18 16:01:47,023 - distributed.core - INFO - Connection to tcp://10.201.0.229:38792 has been closed.
2024-04-18 16:01:47,023 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.0.229:41909', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713456107.0235953')
2024-04-18 16:01:47,024 - distributed.scheduler - INFO - Lost all workers
2024-04-18 16:01:47,026 - distributed.scheduler - INFO - Stopped scheduler at 'tcp://10.201.0.221:8786'
2024-04-18 16:01:47,026 - distributed.scheduler - INFO - End scheduler
