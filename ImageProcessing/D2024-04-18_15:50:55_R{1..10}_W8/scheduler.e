2024-04-18 15:51:21,118 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-18 15:51:21,361 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-18 15:51:21,362 - distributed.scheduler - INFO - -----------------------------------------------
2024-04-18 15:51:21,370 - distributed.preloading - INFO - Creating preload: MofkaSchedulerPlugin.py
2024-04-18 15:51:21,370 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-18 15:51:21,376 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-18 15:51:22,473 - distributed.http.proxy - INFO - To route to workers diagnostics web server please install jupyter-server-proxy: python -m pip install jupyter-server-proxy
2024-04-18 15:51:22,514 - distributed.scheduler - INFO - State start
2024-04-18 15:51:22,517 - distributed.scheduler - INFO - -----------------------------------------------
/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/utils.py:181: RuntimeWarning: Couldn't detect a suitable IP address for reaching '8.8.8.8', defaulting to hostname: [Errno 101] Network is unreachable
  warnings.warn(
2024-04-18 15:51:22,537 - distributed.scheduler - INFO -   Scheduler at:    tcp://10.201.0.91:8786
2024-04-18 15:51:22,538 - distributed.scheduler - INFO -   dashboard at:  http://10.201.0.91:8787/status
2024-04-18 15:51:22,540 - distributed.preloading - INFO - Run preload setup: MofkaSchedulerPlugin.py
2024-04-18 15:51:23,362 - distributed.scheduler - INFO - Registering Worker plugin shuffle
2024-04-18 15:51:31,343 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.157:45695', status: init, memory: 0, processing: 0>
2024-04-18 15:51:33,321 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.157:45695
2024-04-18 15:51:33,321 - distributed.core - INFO - Starting established connection to tcp://10.201.3.157:60416
2024-04-18 15:51:33,323 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.157:36479', status: init, memory: 0, processing: 0>
2024-04-18 15:51:33,323 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.157:36479
2024-04-18 15:51:33,323 - distributed.core - INFO - Starting established connection to tcp://10.201.3.157:60414
2024-04-18 15:51:33,326 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.157:46651', status: init, memory: 0, processing: 0>
2024-04-18 15:51:33,326 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.157:46651
2024-04-18 15:51:33,326 - distributed.core - INFO - Starting established connection to tcp://10.201.3.157:60428
2024-04-18 15:51:33,328 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.157:34847', status: init, memory: 0, processing: 0>
2024-04-18 15:51:33,328 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.157:34847
2024-04-18 15:51:33,328 - distributed.core - INFO - Starting established connection to tcp://10.201.3.157:60442
2024-04-18 15:51:33,329 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.2.190:36597', status: init, memory: 0, processing: 0>
2024-04-18 15:51:33,329 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.2.190:36597
2024-04-18 15:51:33,329 - distributed.core - INFO - Starting established connection to tcp://10.201.2.190:55500
2024-04-18 15:51:33,330 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.2.190:42097', status: init, memory: 0, processing: 0>
2024-04-18 15:51:33,330 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.2.190:42097
2024-04-18 15:51:33,330 - distributed.core - INFO - Starting established connection to tcp://10.201.2.190:55498
2024-04-18 15:51:33,331 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.2.190:43643', status: init, memory: 0, processing: 0>
2024-04-18 15:51:33,331 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.2.190:43643
2024-04-18 15:51:33,332 - distributed.core - INFO - Starting established connection to tcp://10.201.2.190:55492
2024-04-18 15:51:33,332 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.2.190:35589', status: init, memory: 0, processing: 0>
2024-04-18 15:51:33,333 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.2.190:35589
2024-04-18 15:51:33,333 - distributed.core - INFO - Starting established connection to tcp://10.201.2.190:55494
2024-04-18 15:51:33,349 - distributed.scheduler - INFO - Receive client connection: Client-8787cc3c-fd9b-11ee-8dda-6805cae1df6c
2024-04-18 15:51:33,349 - distributed.core - INFO - Starting established connection to tcp://10.201.0.90:57108
2024-04-18 15:51:38,564 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 4.84s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 15:52:32,512 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 8.39s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 15:53:35,176 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 9.19s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 15:53:46,717 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 11.47s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 15:55:00,125 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 9.30s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 15:55:00,137 - distributed.scheduler - INFO - Scheduler closing due to unknown reason...
2024-04-18 15:55:00,138 - distributed.scheduler - INFO - Scheduler closing all comms
2024-04-18 15:55:00,140 - distributed.core - INFO - Connection to tcp://10.201.3.157:60416 has been closed.
2024-04-18 15:55:00,140 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.157:45695', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713455700.1402886')
2024-04-18 15:55:00,690 - distributed.core - INFO - Connection to tcp://10.201.3.157:60414 has been closed.
2024-04-18 15:55:00,690 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.157:36479', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713455700.6906595')
2024-04-18 15:55:00,691 - distributed.core - INFO - Connection to tcp://10.201.3.157:60428 has been closed.
2024-04-18 15:55:00,691 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.157:46651', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713455700.6915472')
2024-04-18 15:55:00,692 - distributed.core - INFO - Connection to tcp://10.201.3.157:60442 has been closed.
2024-04-18 15:55:00,692 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.157:34847', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713455700.692185')
2024-04-18 15:55:00,692 - distributed.core - INFO - Connection to tcp://10.201.2.190:55500 has been closed.
2024-04-18 15:55:00,692 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.2.190:36597', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713455700.6927545')
2024-04-18 15:55:00,693 - distributed.core - INFO - Connection to tcp://10.201.2.190:55498 has been closed.
2024-04-18 15:55:00,693 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.2.190:42097', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713455700.6932266')
2024-04-18 15:55:00,693 - distributed.core - INFO - Connection to tcp://10.201.2.190:55492 has been closed.
2024-04-18 15:55:00,693 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.2.190:43643', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713455700.6937087')
2024-04-18 15:55:00,694 - distributed.core - INFO - Connection to tcp://10.201.2.190:55494 has been closed.
2024-04-18 15:55:00,694 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.2.190:35589', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713455700.694173')
2024-04-18 15:55:00,694 - distributed.scheduler - INFO - Lost all workers
2024-04-18 15:55:00,696 - distributed.scheduler - INFO - Stopped scheduler at 'tcp://10.201.0.91:8786'
2024-04-18 15:55:00,696 - distributed.scheduler - INFO - End scheduler
