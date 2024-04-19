2024-04-19 19:01:25,414 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-19 19:01:25,543 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-19 19:01:25,544 - distributed.scheduler - INFO - -----------------------------------------------
2024-04-19 19:01:25,552 - distributed.preloading - INFO - Creating preload: MofkaSchedulerPlugin.py
2024-04-19 19:01:25,552 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-19 19:01:25,558 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-19 19:01:26,541 - distributed.http.proxy - INFO - To route to workers diagnostics web server please install jupyter-server-proxy: python -m pip install jupyter-server-proxy
2024-04-19 19:01:26,581 - distributed.scheduler - INFO - State start
2024-04-19 19:01:26,585 - distributed.scheduler - INFO - -----------------------------------------------
/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/utils.py:181: RuntimeWarning: Couldn't detect a suitable IP address for reaching '8.8.8.8', defaulting to hostname: [Errno 101] Network is unreachable
  warnings.warn(
2024-04-19 19:01:26,591 - distributed.scheduler - INFO -   Scheduler at:   tcp://10.201.1.129:8786
2024-04-19 19:01:26,591 - distributed.scheduler - INFO -   dashboard at:  http://10.201.1.129:8787/status
2024-04-19 19:01:26,594 - distributed.preloading - INFO - Run preload setup: MofkaSchedulerPlugin.py
2024-04-19 19:01:27,399 - distributed.scheduler - INFO - Registering Worker plugin shuffle
2024-04-19 19:01:34,269 - distributed.scheduler - INFO - Receive client connection: Client-3c72562a-fe7f-11ee-829c-6805cae1dd68
2024-04-19 19:01:36,144 - distributed.core - INFO - Starting established connection to tcp://10.201.1.128:51444
2024-04-19 19:01:36,147 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.1.175:36351', status: init, memory: 0, processing: 0>
2024-04-19 19:01:36,147 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.1.175:36351
2024-04-19 19:01:36,147 - distributed.core - INFO - Starting established connection to tcp://10.201.1.175:54342
2024-04-19 19:01:36,148 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.1.175:44001', status: init, memory: 0, processing: 0>
2024-04-19 19:01:36,148 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.1.175:44001
2024-04-19 19:01:36,149 - distributed.core - INFO - Starting established connection to tcp://10.201.1.175:54368
2024-04-19 19:01:36,151 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.1.175:33783', status: init, memory: 0, processing: 0>
2024-04-19 19:01:36,152 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.1.175:33783
2024-04-19 19:01:36,152 - distributed.core - INFO - Starting established connection to tcp://10.201.1.175:54314
2024-04-19 19:01:36,153 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.1.175:34267', status: init, memory: 0, processing: 0>
2024-04-19 19:01:36,153 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.1.175:34267
2024-04-19 19:01:36,153 - distributed.core - INFO - Starting established connection to tcp://10.201.1.175:54324
2024-04-19 19:01:36,154 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.1.175:40951', status: init, memory: 0, processing: 0>
2024-04-19 19:01:36,154 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.1.175:40951
2024-04-19 19:01:36,154 - distributed.core - INFO - Starting established connection to tcp://10.201.1.175:54336
2024-04-19 19:01:36,155 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.1.175:39657', status: init, memory: 0, processing: 0>
2024-04-19 19:01:36,155 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.1.175:39657
2024-04-19 19:01:36,155 - distributed.core - INFO - Starting established connection to tcp://10.201.1.175:54356
2024-04-19 19:01:36,156 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.1.175:40499', status: init, memory: 0, processing: 0>
2024-04-19 19:01:36,156 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.1.175:40499
2024-04-19 19:01:36,156 - distributed.core - INFO - Starting established connection to tcp://10.201.1.175:54346
2024-04-19 19:01:36,157 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.1.175:42845', status: init, memory: 0, processing: 0>
2024-04-19 19:01:36,157 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.1.175:42845
2024-04-19 19:01:36,157 - distributed.core - INFO - Starting established connection to tcp://10.201.1.175:54306
2024-04-19 19:02:11,727 - distributed.scheduler - INFO - Scheduler closing due to unknown reason...
2024-04-19 19:02:11,727 - distributed.scheduler - INFO - Scheduler closing all comms
2024-04-19 19:02:11,729 - distributed.core - INFO - Connection to tcp://10.201.1.175:54342 has been closed.
2024-04-19 19:02:11,729 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.1.175:36351', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553331.729248')
2024-04-19 19:02:11,750 - distributed.core - INFO - Connection to tcp://10.201.1.175:54368 has been closed.
2024-04-19 19:02:11,750 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.1.175:44001', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553331.7506146')
2024-04-19 19:02:11,751 - distributed.core - INFO - Connection to tcp://10.201.1.175:54314 has been closed.
2024-04-19 19:02:11,751 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.1.175:33783', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553331.751578')
2024-04-19 19:02:11,752 - distributed.core - INFO - Connection to tcp://10.201.1.175:54324 has been closed.
2024-04-19 19:02:11,752 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.1.175:34267', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553331.752115')
2024-04-19 19:02:11,752 - distributed.core - INFO - Connection to tcp://10.201.1.175:54336 has been closed.
2024-04-19 19:02:11,752 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.1.175:40951', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553331.7525997')
2024-04-19 19:02:11,752 - distributed.core - INFO - Connection to tcp://10.201.1.175:54356 has been closed.
2024-04-19 19:02:11,753 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.1.175:39657', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553331.7530475')
2024-04-19 19:02:11,753 - distributed.core - INFO - Connection to tcp://10.201.1.175:54346 has been closed.
2024-04-19 19:02:11,753 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.1.175:40499', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553331.753509')
2024-04-19 19:02:11,753 - distributed.core - INFO - Connection to tcp://10.201.1.175:54306 has been closed.
2024-04-19 19:02:11,753 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.1.175:42845', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553331.7539454')
2024-04-19 19:02:11,754 - distributed.scheduler - INFO - Lost all workers
2024-04-19 19:02:11,756 - distributed.scheduler - INFO - Stopped scheduler at 'tcp://10.201.1.129:8786'
2024-04-19 19:02:11,756 - distributed.scheduler - INFO - End scheduler
