2024-04-19 19:01:31,355 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-19 19:01:31,574 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-19 19:01:31,575 - distributed.scheduler - INFO - -----------------------------------------------
2024-04-19 19:01:31,583 - distributed.preloading - INFO - Creating preload: MofkaSchedulerPlugin.py
2024-04-19 19:01:31,583 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-19 19:01:31,588 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-19 19:01:32,570 - distributed.http.proxy - INFO - To route to workers diagnostics web server please install jupyter-server-proxy: python -m pip install jupyter-server-proxy
2024-04-19 19:01:32,610 - distributed.scheduler - INFO - State start
2024-04-19 19:01:32,613 - distributed.scheduler - INFO - -----------------------------------------------
/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/utils.py:181: RuntimeWarning: Couldn't detect a suitable IP address for reaching '8.8.8.8', defaulting to hostname: [Errno 101] Network is unreachable
  warnings.warn(
2024-04-19 19:01:32,631 - distributed.scheduler - INFO -   Scheduler at:     tcp://10.201.4.7:8786
2024-04-19 19:01:32,631 - distributed.scheduler - INFO -   dashboard at:  http://10.201.4.7:8787/status
2024-04-19 19:01:32,633 - distributed.preloading - INFO - Run preload setup: MofkaSchedulerPlugin.py
2024-04-19 19:01:33,456 - distributed.scheduler - INFO - Registering Worker plugin shuffle
2024-04-19 19:01:39,918 - distributed.scheduler - INFO - Receive client connection: Client-3fd59b41-fe7f-11ee-a513-6805cabffa70
2024-04-19 19:01:42,339 - distributed.core - INFO - Starting established connection to tcp://10.201.4.11:48196
2024-04-19 19:01:42,342 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.254:39001', status: init, memory: 0, processing: 0>
2024-04-19 19:01:42,342 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.254:39001
2024-04-19 19:01:42,342 - distributed.core - INFO - Starting established connection to tcp://10.201.3.254:42246
2024-04-19 19:01:42,343 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.254:38761', status: init, memory: 0, processing: 0>
2024-04-19 19:01:42,344 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.254:38761
2024-04-19 19:01:42,344 - distributed.core - INFO - Starting established connection to tcp://10.201.3.254:42312
2024-04-19 19:01:42,350 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.254:38357', status: init, memory: 0, processing: 0>
2024-04-19 19:01:42,351 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.254:38357
2024-04-19 19:01:42,351 - distributed.core - INFO - Starting established connection to tcp://10.201.3.254:42258
2024-04-19 19:01:42,352 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.254:42649', status: init, memory: 0, processing: 0>
2024-04-19 19:01:42,352 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.254:42649
2024-04-19 19:01:42,352 - distributed.core - INFO - Starting established connection to tcp://10.201.3.254:42298
2024-04-19 19:01:42,353 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.254:37823', status: init, memory: 0, processing: 0>
2024-04-19 19:01:42,353 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.254:37823
2024-04-19 19:01:42,353 - distributed.core - INFO - Starting established connection to tcp://10.201.3.254:42284
2024-04-19 19:01:42,354 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.254:43195', status: init, memory: 0, processing: 0>
2024-04-19 19:01:42,354 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.254:43195
2024-04-19 19:01:42,354 - distributed.core - INFO - Starting established connection to tcp://10.201.3.254:42342
2024-04-19 19:01:42,355 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.254:41797', status: init, memory: 0, processing: 0>
2024-04-19 19:01:42,355 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.254:41797
2024-04-19 19:01:42,355 - distributed.core - INFO - Starting established connection to tcp://10.201.3.254:42326
2024-04-19 19:01:42,356 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.254:42141', status: init, memory: 0, processing: 0>
2024-04-19 19:01:42,356 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.254:42141
2024-04-19 19:01:42,356 - distributed.core - INFO - Starting established connection to tcp://10.201.3.254:42274
2024-04-19 19:01:46,997 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 4.24s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 19:02:20,303 - distributed.scheduler - INFO - Scheduler closing due to unknown reason...
2024-04-19 19:02:20,303 - distributed.scheduler - INFO - Scheduler closing all comms
2024-04-19 19:02:20,304 - distributed.core - INFO - Connection to tcp://10.201.3.254:42246 has been closed.
2024-04-19 19:02:20,305 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.254:39001', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.3050475')
2024-04-19 19:02:20,326 - distributed.core - INFO - Connection to tcp://10.201.3.254:42312 has been closed.
2024-04-19 19:02:20,326 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.254:38761', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.3265018')
2024-04-19 19:02:20,327 - distributed.core - INFO - Connection to tcp://10.201.3.254:42258 has been closed.
2024-04-19 19:02:20,327 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.254:38357', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.327179')
2024-04-19 19:02:20,327 - distributed.core - INFO - Connection to tcp://10.201.3.254:42298 has been closed.
2024-04-19 19:02:20,327 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.254:42649', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.3276746')
2024-04-19 19:02:20,328 - distributed.core - INFO - Connection to tcp://10.201.3.254:42284 has been closed.
2024-04-19 19:02:20,328 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.254:37823', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.3281465')
2024-04-19 19:02:20,328 - distributed.core - INFO - Connection to tcp://10.201.3.254:42342 has been closed.
2024-04-19 19:02:20,328 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.254:43195', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.328624')
2024-04-19 19:02:20,329 - distributed.core - INFO - Connection to tcp://10.201.3.254:42326 has been closed.
2024-04-19 19:02:20,329 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.254:41797', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.3290787')
2024-04-19 19:02:20,329 - distributed.core - INFO - Connection to tcp://10.201.3.254:42274 has been closed.
2024-04-19 19:02:20,329 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.254:42141', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.3294952')
2024-04-19 19:02:20,329 - distributed.scheduler - INFO - Lost all workers
2024-04-19 19:02:20,331 - distributed.scheduler - INFO - Stopped scheduler at 'tcp://10.201.4.7:8786'
2024-04-19 19:02:20,331 - distributed.scheduler - INFO - End scheduler
