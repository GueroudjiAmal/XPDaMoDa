2024-04-18 19:26:30,112 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-18 19:26:30,309 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-18 19:26:30,310 - distributed.scheduler - INFO - -----------------------------------------------
2024-04-18 19:26:30,317 - distributed.preloading - INFO - Creating preload: MofkaSchedulerPlugin.py
2024-04-18 19:26:30,317 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-18 19:26:30,324 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-18 19:26:31,298 - distributed.http.proxy - INFO - To route to workers diagnostics web server please install jupyter-server-proxy: python -m pip install jupyter-server-proxy
2024-04-18 19:26:31,338 - distributed.scheduler - INFO - State start
2024-04-18 19:26:31,341 - distributed.scheduler - INFO - -----------------------------------------------
/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/utils.py:181: RuntimeWarning: Couldn't detect a suitable IP address for reaching '8.8.8.8', defaulting to hostname: [Errno 101] Network is unreachable
  warnings.warn(
2024-04-18 19:26:31,359 - distributed.scheduler - INFO -   Scheduler at:   tcp://10.201.0.190:8786
2024-04-18 19:26:31,360 - distributed.scheduler - INFO -   dashboard at:  http://10.201.0.190:8787/status
2024-04-18 19:26:31,362 - distributed.preloading - INFO - Run preload setup: MofkaSchedulerPlugin.py
2024-04-18 19:26:32,187 - distributed.scheduler - INFO - Registering Worker plugin shuffle
2024-04-18 19:26:39,876 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.0.197:43969', status: init, memory: 0, processing: 0>
2024-04-18 19:26:41,738 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.0.197:43969
2024-04-18 19:26:41,738 - distributed.core - INFO - Starting established connection to tcp://10.201.0.197:50910
2024-04-18 19:26:41,739 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.0.197:44551', status: init, memory: 0, processing: 0>
2024-04-18 19:26:41,740 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.0.197:44551
2024-04-18 19:26:41,740 - distributed.core - INFO - Starting established connection to tcp://10.201.0.197:50930
2024-04-18 19:26:41,742 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.0.197:45429', status: init, memory: 0, processing: 0>
2024-04-18 19:26:41,743 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.0.197:45429
2024-04-18 19:26:41,743 - distributed.core - INFO - Starting established connection to tcp://10.201.0.197:50940
2024-04-18 19:26:41,744 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.0.197:45257', status: init, memory: 0, processing: 0>
2024-04-18 19:26:41,744 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.0.197:45257
2024-04-18 19:26:41,744 - distributed.core - INFO - Starting established connection to tcp://10.201.0.197:50918
2024-04-18 19:26:41,745 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.0.183:44003', status: init, memory: 0, processing: 0>
2024-04-18 19:26:41,746 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.0.183:44003
2024-04-18 19:26:41,746 - distributed.core - INFO - Starting established connection to tcp://10.201.0.183:34542
2024-04-18 19:26:41,747 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.0.183:42499', status: init, memory: 0, processing: 0>
2024-04-18 19:26:41,747 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.0.183:42499
2024-04-18 19:26:41,747 - distributed.core - INFO - Starting established connection to tcp://10.201.0.183:34558
2024-04-18 19:26:41,748 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.0.183:41331', status: init, memory: 0, processing: 0>
2024-04-18 19:26:41,748 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.0.183:41331
2024-04-18 19:26:41,748 - distributed.core - INFO - Starting established connection to tcp://10.201.0.183:34568
2024-04-18 19:26:41,749 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.0.183:44243', status: init, memory: 0, processing: 0>
2024-04-18 19:26:41,749 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.0.183:44243
2024-04-18 19:26:41,749 - distributed.core - INFO - Starting established connection to tcp://10.201.0.183:34552
2024-04-18 19:26:47,191 - distributed.scheduler - INFO - Receive client connection: Client-97cd3d4e-fdb9-11ee-b5df-6805cae127b4
2024-04-18 19:26:47,192 - distributed.core - INFO - Starting established connection to tcp://10.201.0.193:41320
2024-04-18 19:26:59,831 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 11.50s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 19:27:16,560 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 16.61s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 19:30:14,330 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 26.52s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 19:30:14,447 - distributed.scheduler - INFO - Scheduler closing due to unknown reason...
2024-04-18 19:30:14,449 - distributed.scheduler - INFO - Scheduler closing all comms
2024-04-18 19:30:14,450 - distributed.core - INFO - Connection to tcp://10.201.0.197:50910 has been closed.
2024-04-18 19:30:14,450 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.0.197:43969', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468614.4506178')
2024-04-18 19:30:14,960 - distributed.core - INFO - Connection to tcp://10.201.0.197:50930 has been closed.
2024-04-18 19:30:14,960 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.0.197:44551', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468614.9602923')
2024-04-18 19:30:14,960 - distributed.core - INFO - Connection to tcp://10.201.0.197:50940 has been closed.
2024-04-18 19:30:14,960 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.0.197:45429', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468614.9609487')
2024-04-18 19:30:14,961 - distributed.core - INFO - Connection to tcp://10.201.0.197:50918 has been closed.
2024-04-18 19:30:14,961 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.0.197:45257', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468614.9614944')
2024-04-18 19:30:14,961 - distributed.core - INFO - Connection to tcp://10.201.0.183:34542 has been closed.
2024-04-18 19:30:14,962 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.0.183:44003', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468614.9620519')
2024-04-18 19:30:14,962 - distributed.core - INFO - Connection to tcp://10.201.0.183:34558 has been closed.
2024-04-18 19:30:14,962 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.0.183:42499', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468614.9626153')
2024-04-18 19:30:14,963 - distributed.core - INFO - Connection to tcp://10.201.0.183:34568 has been closed.
2024-04-18 19:30:14,963 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.0.183:41331', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468614.9631526')
2024-04-18 19:30:14,963 - distributed.core - INFO - Connection to tcp://10.201.0.183:34552 has been closed.
2024-04-18 19:30:14,963 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.0.183:44243', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468614.9636574')
2024-04-18 19:30:14,964 - distributed.scheduler - INFO - Lost all workers
2024-04-18 19:30:14,966 - distributed.scheduler - INFO - Stopped scheduler at 'tcp://10.201.0.190:8786'
2024-04-18 19:30:14,966 - distributed.scheduler - INFO - End scheduler
