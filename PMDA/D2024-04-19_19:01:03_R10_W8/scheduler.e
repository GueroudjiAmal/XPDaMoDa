2024-04-19 19:01:31,406 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-19 19:01:31,603 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-19 19:01:31,604 - distributed.scheduler - INFO - -----------------------------------------------
2024-04-19 19:01:31,611 - distributed.preloading - INFO - Creating preload: MofkaSchedulerPlugin.py
2024-04-19 19:01:31,611 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-19 19:01:31,617 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-19 19:01:32,582 - distributed.http.proxy - INFO - To route to workers diagnostics web server please install jupyter-server-proxy: python -m pip install jupyter-server-proxy
2024-04-19 19:01:32,622 - distributed.scheduler - INFO - State start
2024-04-19 19:01:32,625 - distributed.scheduler - INFO - -----------------------------------------------
/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/utils.py:181: RuntimeWarning: Couldn't detect a suitable IP address for reaching '8.8.8.8', defaulting to hostname: [Errno 101] Network is unreachable
  warnings.warn(
2024-04-19 19:01:32,642 - distributed.scheduler - INFO -   Scheduler at:   tcp://10.201.3.252:8786
2024-04-19 19:01:32,643 - distributed.scheduler - INFO -   dashboard at:  http://10.201.3.252:8787/status
2024-04-19 19:01:32,645 - distributed.preloading - INFO - Run preload setup: MofkaSchedulerPlugin.py
2024-04-19 19:01:33,540 - distributed.scheduler - INFO - Registering Worker plugin shuffle
2024-04-19 19:01:39,916 - distributed.scheduler - INFO - Receive client connection: Client-3fc5aee7-fe7f-11ee-bc20-6805cac0089c
2024-04-19 19:01:42,333 - distributed.core - INFO - Starting established connection to tcp://10.201.4.15:52158
2024-04-19 19:01:42,336 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.4.19:46431', status: init, memory: 0, processing: 0>
2024-04-19 19:01:42,336 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.4.19:46431
2024-04-19 19:01:42,336 - distributed.core - INFO - Starting established connection to tcp://10.201.4.19:40786
2024-04-19 19:01:42,337 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.4.19:41961', status: init, memory: 0, processing: 0>
2024-04-19 19:01:42,338 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.4.19:41961
2024-04-19 19:01:42,338 - distributed.core - INFO - Starting established connection to tcp://10.201.4.19:40766
2024-04-19 19:01:42,346 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.4.19:36321', status: init, memory: 0, processing: 0>
2024-04-19 19:01:42,347 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.4.19:36321
2024-04-19 19:01:42,347 - distributed.core - INFO - Starting established connection to tcp://10.201.4.19:40742
2024-04-19 19:01:42,348 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.4.19:42045', status: init, memory: 0, processing: 0>
2024-04-19 19:01:42,348 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.4.19:42045
2024-04-19 19:01:42,348 - distributed.core - INFO - Starting established connection to tcp://10.201.4.19:40770
2024-04-19 19:01:42,349 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.4.19:35707', status: init, memory: 0, processing: 0>
2024-04-19 19:01:42,349 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.4.19:35707
2024-04-19 19:01:42,349 - distributed.core - INFO - Starting established connection to tcp://10.201.4.19:40800
2024-04-19 19:01:42,350 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.4.19:46271', status: init, memory: 0, processing: 0>
2024-04-19 19:01:42,350 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.4.19:46271
2024-04-19 19:01:42,350 - distributed.core - INFO - Starting established connection to tcp://10.201.4.19:40764
2024-04-19 19:01:42,351 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.4.19:40351', status: init, memory: 0, processing: 0>
2024-04-19 19:01:42,352 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.4.19:40351
2024-04-19 19:01:42,352 - distributed.core - INFO - Starting established connection to tcp://10.201.4.19:40808
2024-04-19 19:01:42,352 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.4.19:43619', status: init, memory: 0, processing: 0>
2024-04-19 19:01:42,353 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.4.19:43619
2024-04-19 19:01:42,353 - distributed.core - INFO - Starting established connection to tcp://10.201.4.19:40754
2024-04-19 19:01:47,079 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 4.34s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 19:02:20,178 - distributed.scheduler - INFO - Scheduler closing due to unknown reason...
2024-04-19 19:02:20,178 - distributed.scheduler - INFO - Scheduler closing all comms
2024-04-19 19:02:20,180 - distributed.core - INFO - Connection to tcp://10.201.4.19:40786 has been closed.
2024-04-19 19:02:20,180 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.4.19:46431', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.180248')
2024-04-19 19:02:20,202 - distributed.core - INFO - Connection to tcp://10.201.4.19:40766 has been closed.
2024-04-19 19:02:20,202 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.4.19:41961', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.2025898')
2024-04-19 19:02:20,203 - distributed.core - INFO - Connection to tcp://10.201.4.19:40742 has been closed.
2024-04-19 19:02:20,203 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.4.19:36321', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.2037306')
2024-04-19 19:02:20,204 - distributed.core - INFO - Connection to tcp://10.201.4.19:40770 has been closed.
2024-04-19 19:02:20,204 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.4.19:42045', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.2043684')
2024-04-19 19:02:20,204 - distributed.core - INFO - Connection to tcp://10.201.4.19:40800 has been closed.
2024-04-19 19:02:20,204 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.4.19:35707', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.204871')
2024-04-19 19:02:20,205 - distributed.core - INFO - Connection to tcp://10.201.4.19:40764 has been closed.
2024-04-19 19:02:20,205 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.4.19:46271', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.2053864')
2024-04-19 19:02:20,205 - distributed.core - INFO - Connection to tcp://10.201.4.19:40808 has been closed.
2024-04-19 19:02:20,205 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.4.19:40351', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.205866')
2024-04-19 19:02:20,206 - distributed.core - INFO - Connection to tcp://10.201.4.19:40754 has been closed.
2024-04-19 19:02:20,206 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.4.19:43619', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.2063773')
2024-04-19 19:02:20,206 - distributed.scheduler - INFO - Lost all workers
2024-04-19 19:02:20,208 - distributed.scheduler - INFO - Stopped scheduler at 'tcp://10.201.3.252:8786'
2024-04-19 19:02:20,208 - distributed.scheduler - INFO - End scheduler
