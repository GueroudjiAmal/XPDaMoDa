2024-04-18 19:34:43,946 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-18 19:34:44,030 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-18 19:34:44,031 - distributed.scheduler - INFO - -----------------------------------------------
2024-04-18 19:34:44,035 - distributed.preloading - INFO - Creating preload: MofkaSchedulerPlugin.py
2024-04-18 19:34:44,035 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-18 19:34:44,040 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-18 19:34:44,614 - distributed.http.proxy - INFO - To route to workers diagnostics web server please install jupyter-server-proxy: python -m pip install jupyter-server-proxy
2024-04-18 19:34:44,652 - distributed.scheduler - INFO - State start
2024-04-18 19:34:44,655 - distributed.scheduler - INFO - -----------------------------------------------
/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/utils.py:181: RuntimeWarning: Couldn't detect a suitable IP address for reaching '8.8.8.8', defaulting to hostname: [Errno 101] Network is unreachable
  warnings.warn(
2024-04-18 19:34:44,659 - distributed.scheduler - INFO -   Scheduler at:    tcp://10.201.2.88:8786
2024-04-18 19:34:44,659 - distributed.scheduler - INFO -   dashboard at:  http://10.201.2.88:8787/status
2024-04-18 19:34:44,662 - distributed.preloading - INFO - Run preload setup: MofkaSchedulerPlugin.py
2024-04-18 19:34:45,550 - distributed.scheduler - INFO - Registering Worker plugin shuffle
2024-04-18 19:34:48,069 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.4.24:39949', status: init, memory: 0, processing: 0>
2024-04-18 19:34:48,609 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.4.24:39949
2024-04-18 19:34:48,609 - distributed.core - INFO - Starting established connection to tcp://10.201.4.24:55836
2024-04-18 19:34:48,611 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.4.24:46757', status: init, memory: 0, processing: 0>
2024-04-18 19:34:48,612 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.4.24:46757
2024-04-18 19:34:48,612 - distributed.core - INFO - Starting established connection to tcp://10.201.4.24:55820
2024-04-18 19:34:48,613 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.4.24:44729', status: init, memory: 0, processing: 0>
2024-04-18 19:34:48,613 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.4.24:44729
2024-04-18 19:34:48,613 - distributed.core - INFO - Starting established connection to tcp://10.201.4.24:55814
2024-04-18 19:34:48,614 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.4.24:41325', status: init, memory: 0, processing: 0>
2024-04-18 19:34:48,614 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.4.24:41325
2024-04-18 19:34:48,614 - distributed.core - INFO - Starting established connection to tcp://10.201.4.24:55800
2024-04-18 19:34:49,221 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.4.26:38711', status: init, memory: 0, processing: 0>
2024-04-18 19:34:49,221 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.4.26:38711
2024-04-18 19:34:49,221 - distributed.core - INFO - Starting established connection to tcp://10.201.4.26:55246
2024-04-18 19:34:49,222 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.4.26:40239', status: init, memory: 0, processing: 0>
2024-04-18 19:34:49,222 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.4.26:40239
2024-04-18 19:34:49,222 - distributed.core - INFO - Starting established connection to tcp://10.201.4.26:55230
2024-04-18 19:34:49,223 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.4.26:40611', status: init, memory: 0, processing: 0>
2024-04-18 19:34:49,223 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.4.26:40611
2024-04-18 19:34:49,223 - distributed.core - INFO - Starting established connection to tcp://10.201.4.26:55216
2024-04-18 19:34:49,224 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.4.26:38573', status: init, memory: 0, processing: 0>
2024-04-18 19:34:49,225 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.4.26:38573
2024-04-18 19:34:49,225 - distributed.core - INFO - Starting established connection to tcp://10.201.4.26:55236
2024-04-18 19:34:50,016 - distributed.scheduler - INFO - Receive client connection: Client-b8623578-fdba-11ee-85e3-6805cac004f0
2024-04-18 19:34:50,016 - distributed.core - INFO - Starting established connection to tcp://10.201.2.89:48494
2024-04-18 19:34:55,390 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 4.56s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 19:35:12,227 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 16.71s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 19:38:02,657 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 26.34s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 19:38:02,833 - distributed.scheduler - INFO - Scheduler closing due to unknown reason...
2024-04-18 19:38:02,835 - distributed.scheduler - INFO - Scheduler closing all comms
2024-04-18 19:38:02,836 - distributed.core - INFO - Connection to tcp://10.201.4.24:55836 has been closed.
2024-04-18 19:38:02,836 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.4.24:39949', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713469082.836681')
2024-04-18 19:38:03,337 - distributed.core - INFO - Connection to tcp://10.201.4.24:55820 has been closed.
2024-04-18 19:38:03,337 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.4.24:46757', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713469083.3376038')
2024-04-18 19:38:03,338 - distributed.core - INFO - Connection to tcp://10.201.4.24:55814 has been closed.
2024-04-18 19:38:03,338 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.4.24:44729', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713469083.338408')
2024-04-18 19:38:03,338 - distributed.core - INFO - Connection to tcp://10.201.4.24:55800 has been closed.
2024-04-18 19:38:03,339 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.4.24:41325', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713469083.3390033')
2024-04-18 19:38:03,339 - distributed.core - INFO - Connection to tcp://10.201.4.26:55246 has been closed.
2024-04-18 19:38:03,339 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.4.26:38711', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713469083.3396218')
2024-04-18 19:38:03,340 - distributed.core - INFO - Connection to tcp://10.201.4.26:55230 has been closed.
2024-04-18 19:38:03,340 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.4.26:40239', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713469083.3401377')
2024-04-18 19:38:03,340 - distributed.core - INFO - Connection to tcp://10.201.4.26:55216 has been closed.
2024-04-18 19:38:03,340 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.4.26:40611', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713469083.3407242')
2024-04-18 19:38:03,341 - distributed.core - INFO - Connection to tcp://10.201.4.26:55236 has been closed.
2024-04-18 19:38:03,341 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.4.26:38573', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713469083.3412318')
2024-04-18 19:38:03,341 - distributed.scheduler - INFO - Lost all workers
2024-04-18 19:38:03,343 - distributed.scheduler - INFO - Stopped scheduler at 'tcp://10.201.2.88:8786'
2024-04-18 19:38:03,344 - distributed.scheduler - INFO - End scheduler
