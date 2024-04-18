2024-04-18 19:26:25,292 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-18 19:26:25,502 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-18 19:26:25,503 - distributed.scheduler - INFO - -----------------------------------------------
2024-04-18 19:26:25,510 - distributed.preloading - INFO - Creating preload: MofkaSchedulerPlugin.py
2024-04-18 19:26:25,511 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-18 19:26:25,516 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-18 19:26:26,495 - distributed.http.proxy - INFO - To route to workers diagnostics web server please install jupyter-server-proxy: python -m pip install jupyter-server-proxy
2024-04-18 19:26:26,535 - distributed.scheduler - INFO - State start
2024-04-18 19:26:26,539 - distributed.scheduler - INFO - -----------------------------------------------
/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/utils.py:181: RuntimeWarning: Couldn't detect a suitable IP address for reaching '8.8.8.8', defaulting to hostname: [Errno 101] Network is unreachable
  warnings.warn(
2024-04-18 19:26:26,557 - distributed.scheduler - INFO -   Scheduler at:   tcp://10.201.1.216:8786
2024-04-18 19:26:26,558 - distributed.scheduler - INFO -   dashboard at:  http://10.201.1.216:8787/status
2024-04-18 19:26:26,560 - distributed.preloading - INFO - Run preload setup: MofkaSchedulerPlugin.py
2024-04-18 19:26:27,397 - distributed.scheduler - INFO - Registering Worker plugin shuffle
2024-04-18 19:26:31,396 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.4.24:45103', status: init, memory: 0, processing: 0>
2024-04-18 19:26:33,290 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.4.24:45103
2024-04-18 19:26:33,290 - distributed.core - INFO - Starting established connection to tcp://10.201.4.24:33036
2024-04-18 19:26:33,292 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.4.24:36251', status: init, memory: 0, processing: 0>
2024-04-18 19:26:33,292 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.4.24:36251
2024-04-18 19:26:33,292 - distributed.core - INFO - Starting established connection to tcp://10.201.4.24:33048
2024-04-18 19:26:33,295 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.4.24:38611', status: init, memory: 0, processing: 0>
2024-04-18 19:26:33,295 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.4.24:38611
2024-04-18 19:26:33,295 - distributed.core - INFO - Starting established connection to tcp://10.201.4.24:33024
2024-04-18 19:26:33,297 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.4.24:43255', status: init, memory: 0, processing: 0>
2024-04-18 19:26:33,297 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.4.24:43255
2024-04-18 19:26:33,297 - distributed.core - INFO - Starting established connection to tcp://10.201.4.24:33028
2024-04-18 19:26:35,240 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.4.26:44297', status: init, memory: 0, processing: 0>
2024-04-18 19:26:35,240 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.4.26:44297
2024-04-18 19:26:35,240 - distributed.core - INFO - Starting established connection to tcp://10.201.4.26:59568
2024-04-18 19:26:35,241 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.4.26:39459', status: init, memory: 0, processing: 0>
2024-04-18 19:26:35,241 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.4.26:39459
2024-04-18 19:26:35,241 - distributed.core - INFO - Starting established connection to tcp://10.201.4.26:59580
2024-04-18 19:26:35,242 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.4.26:32913', status: init, memory: 0, processing: 0>
2024-04-18 19:26:35,243 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.4.26:32913
2024-04-18 19:26:35,243 - distributed.core - INFO - Starting established connection to tcp://10.201.4.26:59560
2024-04-18 19:26:35,243 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.4.26:34093', status: init, memory: 0, processing: 0>
2024-04-18 19:26:35,244 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.4.26:34093
2024-04-18 19:26:35,244 - distributed.core - INFO - Starting established connection to tcp://10.201.4.26:59596
2024-04-18 19:26:41,916 - distributed.scheduler - INFO - Receive client connection: Client-94a2334d-fdb9-11ee-8167-6805cac004f0
2024-04-18 19:26:41,916 - distributed.core - INFO - Starting established connection to tcp://10.201.2.89:60888
2024-04-18 19:26:55,359 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 11.46s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 19:27:12,135 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 16.73s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 19:30:08,426 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 26.07s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 19:30:08,520 - distributed.scheduler - INFO - Scheduler closing due to unknown reason...
2024-04-18 19:30:08,522 - distributed.scheduler - INFO - Scheduler closing all comms
2024-04-18 19:30:08,523 - distributed.core - INFO - Connection to tcp://10.201.4.24:33036 has been closed.
2024-04-18 19:30:08,523 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.4.24:45103', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468608.5239587')
2024-04-18 19:30:09,030 - distributed.core - INFO - Connection to tcp://10.201.4.24:33048 has been closed.
2024-04-18 19:30:09,030 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.4.24:36251', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468609.0303555')
2024-04-18 19:30:09,031 - distributed.core - INFO - Connection to tcp://10.201.4.24:33024 has been closed.
2024-04-18 19:30:09,031 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.4.24:38611', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468609.0310743')
2024-04-18 19:30:09,031 - distributed.core - INFO - Connection to tcp://10.201.4.24:33028 has been closed.
2024-04-18 19:30:09,031 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.4.24:43255', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468609.0317268')
2024-04-18 19:30:09,032 - distributed.core - INFO - Connection to tcp://10.201.4.26:59568 has been closed.
2024-04-18 19:30:09,032 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.4.26:44297', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468609.0323265')
2024-04-18 19:30:09,032 - distributed.core - INFO - Connection to tcp://10.201.4.26:59580 has been closed.
2024-04-18 19:30:09,032 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.4.26:39459', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468609.0328279')
2024-04-18 19:30:09,033 - distributed.core - INFO - Connection to tcp://10.201.4.26:59560 has been closed.
2024-04-18 19:30:09,033 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.4.26:32913', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468609.033313')
2024-04-18 19:30:09,033 - distributed.core - INFO - Connection to tcp://10.201.4.26:59596 has been closed.
2024-04-18 19:30:09,033 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.4.26:34093', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468609.0339139')
2024-04-18 19:30:09,034 - distributed.scheduler - INFO - Lost all workers
2024-04-18 19:30:09,036 - distributed.scheduler - INFO - Stopped scheduler at 'tcp://10.201.1.216:8786'
2024-04-18 19:30:09,036 - distributed.scheduler - INFO - End scheduler
