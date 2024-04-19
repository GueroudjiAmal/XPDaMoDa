2024-04-19 19:01:31,366 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-19 19:01:31,574 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-19 19:01:31,575 - distributed.scheduler - INFO - -----------------------------------------------
2024-04-19 19:01:31,583 - distributed.preloading - INFO - Creating preload: MofkaSchedulerPlugin.py
2024-04-19 19:01:31,583 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-19 19:01:31,605 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-19 19:01:32,567 - distributed.http.proxy - INFO - To route to workers diagnostics web server please install jupyter-server-proxy: python -m pip install jupyter-server-proxy
2024-04-19 19:01:32,607 - distributed.scheduler - INFO - State start
2024-04-19 19:01:32,610 - distributed.scheduler - INFO - -----------------------------------------------
/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/utils.py:181: RuntimeWarning: Couldn't detect a suitable IP address for reaching '8.8.8.8', defaulting to hostname: [Errno 101] Network is unreachable
  warnings.warn(
2024-04-19 19:01:32,628 - distributed.scheduler - INFO -   Scheduler at:    tcp://10.201.3.18:8786
2024-04-19 19:01:32,628 - distributed.scheduler - INFO -   dashboard at:  http://10.201.3.18:8787/status
2024-04-19 19:01:32,630 - distributed.preloading - INFO - Run preload setup: MofkaSchedulerPlugin.py
2024-04-19 19:01:33,483 - distributed.scheduler - INFO - Registering Worker plugin shuffle
2024-04-19 19:01:40,038 - distributed.scheduler - INFO - Receive client connection: Client-3fe1305c-fe7f-11ee-8fc2-6805cae199a4
2024-04-19 19:01:42,571 - distributed.core - INFO - Starting established connection to tcp://10.201.3.116:50528
2024-04-19 19:01:42,573 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.17:45407', status: init, memory: 0, processing: 0>
2024-04-19 19:01:42,573 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.17:45407
2024-04-19 19:01:42,573 - distributed.core - INFO - Starting established connection to tcp://10.201.3.17:44200
2024-04-19 19:01:42,574 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.17:35967', status: init, memory: 0, processing: 0>
2024-04-19 19:01:42,575 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.17:35967
2024-04-19 19:01:42,575 - distributed.core - INFO - Starting established connection to tcp://10.201.3.17:44244
2024-04-19 19:01:42,580 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.17:36903', status: init, memory: 0, processing: 0>
2024-04-19 19:01:42,580 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.17:36903
2024-04-19 19:01:42,581 - distributed.core - INFO - Starting established connection to tcp://10.201.3.17:44236
2024-04-19 19:01:42,581 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.17:40865', status: init, memory: 0, processing: 0>
2024-04-19 19:01:42,582 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.17:40865
2024-04-19 19:01:42,582 - distributed.core - INFO - Starting established connection to tcp://10.201.3.17:44228
2024-04-19 19:01:42,583 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.17:36723', status: init, memory: 0, processing: 0>
2024-04-19 19:01:42,583 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.17:36723
2024-04-19 19:01:42,583 - distributed.core - INFO - Starting established connection to tcp://10.201.3.17:44208
2024-04-19 19:01:42,584 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.17:37637', status: init, memory: 0, processing: 0>
2024-04-19 19:01:42,584 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.17:37637
2024-04-19 19:01:42,584 - distributed.core - INFO - Starting established connection to tcp://10.201.3.17:44214
2024-04-19 19:01:42,585 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.17:38345', status: init, memory: 0, processing: 0>
2024-04-19 19:01:42,585 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.17:38345
2024-04-19 19:01:42,585 - distributed.core - INFO - Starting established connection to tcp://10.201.3.17:44198
2024-04-19 19:01:42,586 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.17:40773', status: init, memory: 0, processing: 0>
2024-04-19 19:01:42,586 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.17:40773
2024-04-19 19:01:42,586 - distributed.core - INFO - Starting established connection to tcp://10.201.3.17:44212
2024-04-19 19:01:47,097 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 4.13s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 19:02:20,323 - distributed.scheduler - INFO - Scheduler closing due to unknown reason...
2024-04-19 19:02:20,324 - distributed.scheduler - INFO - Scheduler closing all comms
2024-04-19 19:02:20,325 - distributed.core - INFO - Connection to tcp://10.201.3.17:44200 has been closed.
2024-04-19 19:02:20,325 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.17:45407', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.3255486')
2024-04-19 19:02:20,348 - distributed.core - INFO - Connection to tcp://10.201.3.17:44244 has been closed.
2024-04-19 19:02:20,349 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.17:35967', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.3491006')
2024-04-19 19:02:20,349 - distributed.core - INFO - Connection to tcp://10.201.3.17:44236 has been closed.
2024-04-19 19:02:20,349 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.17:36903', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.3497126')
2024-04-19 19:02:20,350 - distributed.core - INFO - Connection to tcp://10.201.3.17:44228 has been closed.
2024-04-19 19:02:20,350 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.17:40865', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.3502564')
2024-04-19 19:02:20,350 - distributed.core - INFO - Connection to tcp://10.201.3.17:44208 has been closed.
2024-04-19 19:02:20,350 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.17:36723', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.3507388')
2024-04-19 19:02:20,351 - distributed.core - INFO - Connection to tcp://10.201.3.17:44214 has been closed.
2024-04-19 19:02:20,351 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.17:37637', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.3511913')
2024-04-19 19:02:20,351 - distributed.core - INFO - Connection to tcp://10.201.3.17:44198 has been closed.
2024-04-19 19:02:20,351 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.17:38345', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.3517158')
2024-04-19 19:02:20,352 - distributed.core - INFO - Connection to tcp://10.201.3.17:44212 has been closed.
2024-04-19 19:02:20,352 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.17:40773', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.3521485')
2024-04-19 19:02:20,352 - distributed.scheduler - INFO - Lost all workers
2024-04-19 19:02:20,354 - distributed.scheduler - INFO - Stopped scheduler at 'tcp://10.201.3.18:8786'
2024-04-19 19:02:20,354 - distributed.scheduler - INFO - End scheduler
