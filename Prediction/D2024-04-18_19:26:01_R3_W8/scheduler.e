2024-04-18 19:26:30,087 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-18 19:26:30,292 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-18 19:26:30,294 - distributed.scheduler - INFO - -----------------------------------------------
2024-04-18 19:26:30,301 - distributed.preloading - INFO - Creating preload: MofkaSchedulerPlugin.py
2024-04-18 19:26:30,301 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-18 19:26:30,307 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-18 19:26:31,299 - distributed.http.proxy - INFO - To route to workers diagnostics web server please install jupyter-server-proxy: python -m pip install jupyter-server-proxy
2024-04-18 19:26:31,340 - distributed.scheduler - INFO - State start
2024-04-18 19:26:31,343 - distributed.scheduler - INFO - -----------------------------------------------
/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/utils.py:181: RuntimeWarning: Couldn't detect a suitable IP address for reaching '8.8.8.8', defaulting to hostname: [Errno 101] Network is unreachable
  warnings.warn(
2024-04-18 19:26:31,361 - distributed.scheduler - INFO -   Scheduler at:   tcp://10.201.0.222:8786
2024-04-18 19:26:31,362 - distributed.scheduler - INFO -   dashboard at:  http://10.201.0.222:8787/status
2024-04-18 19:26:31,364 - distributed.preloading - INFO - Run preload setup: MofkaSchedulerPlugin.py
2024-04-18 19:26:32,211 - distributed.scheduler - INFO - Registering Worker plugin shuffle
2024-04-18 19:26:39,517 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.0.212:46283', status: init, memory: 0, processing: 0>
2024-04-18 19:26:41,348 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.0.212:46283
2024-04-18 19:26:41,348 - distributed.core - INFO - Starting established connection to tcp://10.201.0.212:49770
2024-04-18 19:26:41,350 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.0.212:40295', status: init, memory: 0, processing: 0>
2024-04-18 19:26:41,351 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.0.212:40295
2024-04-18 19:26:41,351 - distributed.core - INFO - Starting established connection to tcp://10.201.0.212:49754
2024-04-18 19:26:41,353 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.0.212:39731', status: init, memory: 0, processing: 0>
2024-04-18 19:26:41,354 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.0.212:39731
2024-04-18 19:26:41,354 - distributed.core - INFO - Starting established connection to tcp://10.201.0.212:49788
2024-04-18 19:26:41,354 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.0.212:40899', status: init, memory: 0, processing: 0>
2024-04-18 19:26:41,355 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.0.212:40899
2024-04-18 19:26:41,355 - distributed.core - INFO - Starting established connection to tcp://10.201.0.212:49786
2024-04-18 19:26:41,356 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.0.210:46055', status: init, memory: 0, processing: 0>
2024-04-18 19:26:41,356 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.0.210:46055
2024-04-18 19:26:41,356 - distributed.core - INFO - Starting established connection to tcp://10.201.0.210:37446
2024-04-18 19:26:41,357 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.0.210:36053', status: init, memory: 0, processing: 0>
2024-04-18 19:26:41,357 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.0.210:36053
2024-04-18 19:26:41,357 - distributed.core - INFO - Starting established connection to tcp://10.201.0.210:37476
2024-04-18 19:26:41,358 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.0.210:45981', status: init, memory: 0, processing: 0>
2024-04-18 19:26:41,358 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.0.210:45981
2024-04-18 19:26:41,358 - distributed.core - INFO - Starting established connection to tcp://10.201.0.210:37460
2024-04-18 19:26:41,359 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.0.210:41207', status: init, memory: 0, processing: 0>
2024-04-18 19:26:41,359 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.0.210:41207
2024-04-18 19:26:41,359 - distributed.core - INFO - Starting established connection to tcp://10.201.0.210:37484
2024-04-18 19:26:46,767 - distributed.scheduler - INFO - Receive client connection: Client-97970029-fdb9-11ee-8386-6805cae12060
2024-04-18 19:26:46,768 - distributed.core - INFO - Starting established connection to tcp://10.201.0.220:44154
2024-04-18 19:26:59,533 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 11.62s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 19:27:15,955 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 16.38s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 19:30:15,012 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 25.98s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 19:30:15,194 - distributed.scheduler - INFO - Scheduler closing due to unknown reason...
2024-04-18 19:30:15,196 - distributed.scheduler - INFO - Scheduler closing all comms
2024-04-18 19:30:15,197 - distributed.core - INFO - Connection to tcp://10.201.0.212:49770 has been closed.
2024-04-18 19:30:15,197 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.0.212:46283', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468615.1974115')
2024-04-18 19:30:15,699 - distributed.core - INFO - Connection to tcp://10.201.0.212:49754 has been closed.
2024-04-18 19:30:15,699 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.0.212:40295', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468615.6993434')
2024-04-18 19:30:15,699 - distributed.core - INFO - Connection to tcp://10.201.0.212:49788 has been closed.
2024-04-18 19:30:15,699 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.0.212:39731', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468615.699964')
2024-04-18 19:30:15,700 - distributed.core - INFO - Connection to tcp://10.201.0.212:49786 has been closed.
2024-04-18 19:30:15,700 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.0.212:40899', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468615.7006078')
2024-04-18 19:30:15,701 - distributed.core - INFO - Connection to tcp://10.201.0.210:37446 has been closed.
2024-04-18 19:30:15,701 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.0.210:46055', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468615.7012212')
2024-04-18 19:30:15,701 - distributed.core - INFO - Connection to tcp://10.201.0.210:37476 has been closed.
2024-04-18 19:30:15,701 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.0.210:36053', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468615.7017927')
2024-04-18 19:30:15,702 - distributed.core - INFO - Connection to tcp://10.201.0.210:37460 has been closed.
2024-04-18 19:30:15,702 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.0.210:45981', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468615.7022896')
2024-04-18 19:30:15,702 - distributed.core - INFO - Connection to tcp://10.201.0.210:37484 has been closed.
2024-04-18 19:30:15,702 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.0.210:41207', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468615.7027879')
2024-04-18 19:30:15,703 - distributed.scheduler - INFO - Lost all workers
2024-04-18 19:30:15,705 - distributed.scheduler - INFO - Stopped scheduler at 'tcp://10.201.0.222:8786'
2024-04-18 19:30:15,705 - distributed.scheduler - INFO - End scheduler
