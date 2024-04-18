2024-04-18 15:57:36,106 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-18 15:57:36,158 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-18 15:57:36,159 - distributed.scheduler - INFO - -----------------------------------------------
2024-04-18 15:57:36,163 - distributed.preloading - INFO - Creating preload: MofkaSchedulerPlugin.py
2024-04-18 15:57:36,163 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-18 15:57:36,169 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-18 15:57:36,737 - distributed.http.proxy - INFO - To route to workers diagnostics web server please install jupyter-server-proxy: python -m pip install jupyter-server-proxy
2024-04-18 15:57:36,774 - distributed.scheduler - INFO - State start
2024-04-18 15:57:36,777 - distributed.scheduler - INFO - -----------------------------------------------
/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/utils.py:181: RuntimeWarning: Couldn't detect a suitable IP address for reaching '8.8.8.8', defaulting to hostname: [Errno 101] Network is unreachable
  warnings.warn(
2024-04-18 15:57:36,782 - distributed.scheduler - INFO -   Scheduler at:   tcp://10.201.2.223:8786
2024-04-18 15:57:36,782 - distributed.scheduler - INFO -   dashboard at:  http://10.201.2.223:8787/status
2024-04-18 15:57:36,784 - distributed.preloading - INFO - Run preload setup: MofkaSchedulerPlugin.py
2024-04-18 15:57:37,581 - distributed.scheduler - INFO - Registering Worker plugin shuffle
2024-04-18 15:57:40,145 - distributed.scheduler - INFO - Receive client connection: Client-62518d57-fd9c-11ee-925f-6805cacee8cc
2024-04-18 15:57:40,706 - distributed.core - INFO - Starting established connection to tcp://10.201.2.222:52292
2024-04-18 15:57:42,306 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.17:42149', status: init, memory: 0, processing: 0>
2024-04-18 15:57:42,316 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.17:42149
2024-04-18 15:57:42,317 - distributed.core - INFO - Starting established connection to tcp://10.201.3.17:46264
2024-04-18 15:57:42,318 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.2.127:37565', status: init, memory: 0, processing: 0>
2024-04-18 15:57:42,323 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.2.127:37565
2024-04-18 15:57:42,323 - distributed.core - INFO - Starting established connection to tcp://10.201.2.127:47196
2024-04-18 15:57:42,325 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.2.127:40719', status: init, memory: 0, processing: 0>
2024-04-18 15:57:42,330 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.2.127:40719
2024-04-18 15:57:42,330 - distributed.core - INFO - Starting established connection to tcp://10.201.2.127:47204
2024-04-18 15:57:42,331 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.2.127:32971', status: init, memory: 0, processing: 0>
2024-04-18 15:57:42,336 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.2.127:32971
2024-04-18 15:57:42,336 - distributed.core - INFO - Starting established connection to tcp://10.201.2.127:47212
2024-04-18 15:57:42,337 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.2.127:43135', status: init, memory: 0, processing: 0>
2024-04-18 15:57:42,337 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.2.127:43135
2024-04-18 15:57:42,337 - distributed.core - INFO - Starting established connection to tcp://10.201.2.127:47198
2024-04-18 15:57:42,338 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.17:41721', status: init, memory: 0, processing: 0>
2024-04-18 15:57:42,338 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.17:41721
2024-04-18 15:57:42,338 - distributed.core - INFO - Starting established connection to tcp://10.201.3.17:46258
2024-04-18 15:57:42,339 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.17:41867', status: init, memory: 0, processing: 0>
2024-04-18 15:57:42,339 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.17:41867
2024-04-18 15:57:42,339 - distributed.core - INFO - Starting established connection to tcp://10.201.3.17:46272
2024-04-18 15:57:42,340 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.17:37383', status: init, memory: 0, processing: 0>
2024-04-18 15:57:42,341 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.17:37383
2024-04-18 15:57:42,341 - distributed.core - INFO - Starting established connection to tcp://10.201.3.17:46268
2024-04-18 15:58:30,230 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 8.46s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 15:59:14,442 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 9.26s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 15:59:24,508 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 9.96s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 16:00:10,366 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 9.00s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 16:00:10,371 - distributed.scheduler - INFO - Scheduler closing due to unknown reason...
2024-04-18 16:00:10,372 - distributed.scheduler - INFO - Scheduler closing all comms
2024-04-18 16:00:10,374 - distributed.core - INFO - Connection to tcp://10.201.3.17:46264 has been closed.
2024-04-18 16:00:10,374 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.17:42149', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713456010.3742943')
2024-04-18 16:00:10,787 - distributed.core - INFO - Connection to tcp://10.201.2.127:47196 has been closed.
2024-04-18 16:00:10,787 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.2.127:37565', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713456010.7870939')
2024-04-18 16:00:10,787 - distributed.core - INFO - Connection to tcp://10.201.2.127:47204 has been closed.
2024-04-18 16:00:10,787 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.2.127:40719', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713456010.787768')
2024-04-18 16:00:10,788 - distributed.core - INFO - Connection to tcp://10.201.2.127:47212 has been closed.
2024-04-18 16:00:10,788 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.2.127:32971', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713456010.7884166')
2024-04-18 16:00:10,788 - distributed.core - INFO - Connection to tcp://10.201.2.127:47198 has been closed.
2024-04-18 16:00:10,789 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.2.127:43135', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713456010.78898')
2024-04-18 16:00:10,789 - distributed.core - INFO - Connection to tcp://10.201.3.17:46258 has been closed.
2024-04-18 16:00:10,789 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.17:41721', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713456010.7895305')
2024-04-18 16:00:10,789 - distributed.core - INFO - Connection to tcp://10.201.3.17:46272 has been closed.
2024-04-18 16:00:10,790 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.17:41867', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713456010.7900288')
2024-04-18 16:00:10,790 - distributed.core - INFO - Connection to tcp://10.201.3.17:46268 has been closed.
2024-04-18 16:00:10,790 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.17:37383', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713456010.7905092')
2024-04-18 16:00:10,790 - distributed.scheduler - INFO - Lost all workers
2024-04-18 16:00:10,793 - distributed.scheduler - INFO - Stopped scheduler at 'tcp://10.201.2.223:8786'
2024-04-18 16:00:10,793 - distributed.scheduler - INFO - End scheduler
