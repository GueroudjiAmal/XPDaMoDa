2024-04-18 19:26:29,704 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-18 19:26:29,905 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-18 19:26:29,906 - distributed.scheduler - INFO - -----------------------------------------------
2024-04-18 19:26:29,914 - distributed.preloading - INFO - Creating preload: MofkaSchedulerPlugin.py
2024-04-18 19:26:29,914 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-18 19:26:29,920 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-18 19:26:30,904 - distributed.http.proxy - INFO - To route to workers diagnostics web server please install jupyter-server-proxy: python -m pip install jupyter-server-proxy
2024-04-18 19:26:30,944 - distributed.scheduler - INFO - State start
2024-04-18 19:26:30,948 - distributed.scheduler - INFO - -----------------------------------------------
/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/utils.py:181: RuntimeWarning: Couldn't detect a suitable IP address for reaching '8.8.8.8', defaulting to hostname: [Errno 101] Network is unreachable
  warnings.warn(
2024-04-18 19:26:30,967 - distributed.scheduler - INFO -   Scheduler at:    tcp://10.201.3.80:8786
2024-04-18 19:26:30,967 - distributed.scheduler - INFO -   dashboard at:  http://10.201.3.80:8787/status
2024-04-18 19:26:30,970 - distributed.preloading - INFO - Run preload setup: MofkaSchedulerPlugin.py
2024-04-18 19:26:31,779 - distributed.scheduler - INFO - Registering Worker plugin shuffle
2024-04-18 19:26:38,197 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.79:36525', status: init, memory: 0, processing: 0>
2024-04-18 19:26:40,068 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.79:36525
2024-04-18 19:26:40,068 - distributed.core - INFO - Starting established connection to tcp://10.201.3.79:44912
2024-04-18 19:26:40,070 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.79:37017', status: init, memory: 0, processing: 0>
2024-04-18 19:26:40,070 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.79:37017
2024-04-18 19:26:40,070 - distributed.core - INFO - Starting established connection to tcp://10.201.3.79:44914
2024-04-18 19:26:40,073 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.79:38653', status: init, memory: 0, processing: 0>
2024-04-18 19:26:40,073 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.79:38653
2024-04-18 19:26:40,073 - distributed.core - INFO - Starting established connection to tcp://10.201.3.79:44904
2024-04-18 19:26:40,074 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.79:44719', status: init, memory: 0, processing: 0>
2024-04-18 19:26:40,075 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.79:44719
2024-04-18 19:26:40,075 - distributed.core - INFO - Starting established connection to tcp://10.201.3.79:44892
2024-04-18 19:26:40,076 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.85:43987', status: init, memory: 0, processing: 0>
2024-04-18 19:26:40,076 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.85:43987
2024-04-18 19:26:40,076 - distributed.core - INFO - Starting established connection to tcp://10.201.3.85:49800
2024-04-18 19:26:40,077 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.85:41317', status: init, memory: 0, processing: 0>
2024-04-18 19:26:40,077 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.85:41317
2024-04-18 19:26:40,077 - distributed.core - INFO - Starting established connection to tcp://10.201.3.85:49778
2024-04-18 19:26:40,078 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.85:43145', status: init, memory: 0, processing: 0>
2024-04-18 19:26:40,078 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.85:43145
2024-04-18 19:26:40,078 - distributed.core - INFO - Starting established connection to tcp://10.201.3.85:49766
2024-04-18 19:26:40,079 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.85:43979', status: init, memory: 0, processing: 0>
2024-04-18 19:26:40,080 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.85:43979
2024-04-18 19:26:40,080 - distributed.core - INFO - Starting established connection to tcp://10.201.3.85:49790
2024-04-18 19:26:46,614 - distributed.scheduler - INFO - Receive client connection: Client-9776b168-fdb9-11ee-abb7-6805cacc0890
2024-04-18 19:26:46,614 - distributed.core - INFO - Starting established connection to tcp://10.201.3.90:48172
2024-04-18 19:26:59,480 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 11.74s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 19:27:16,234 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 16.63s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 19:30:12,498 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 26.34s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 19:30:12,646 - distributed.scheduler - INFO - Scheduler closing due to unknown reason...
2024-04-18 19:30:12,647 - distributed.scheduler - INFO - Scheduler closing all comms
2024-04-18 19:30:12,648 - distributed.core - INFO - Connection to tcp://10.201.3.79:44912 has been closed.
2024-04-18 19:30:12,648 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.79:36525', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468612.6489465')
2024-04-18 19:30:13,151 - distributed.core - INFO - Connection to tcp://10.201.3.79:44914 has been closed.
2024-04-18 19:30:13,151 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.79:37017', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468613.1511836')
2024-04-18 19:30:13,151 - distributed.core - INFO - Connection to tcp://10.201.3.79:44904 has been closed.
2024-04-18 19:30:13,151 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.79:38653', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468613.151855')
2024-04-18 19:30:13,152 - distributed.core - INFO - Connection to tcp://10.201.3.79:44892 has been closed.
2024-04-18 19:30:13,152 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.79:44719', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468613.1524913')
2024-04-18 19:30:13,152 - distributed.core - INFO - Connection to tcp://10.201.3.85:49800 has been closed.
2024-04-18 19:30:13,153 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.85:43987', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468613.153045')
2024-04-18 19:30:13,153 - distributed.core - INFO - Connection to tcp://10.201.3.85:49778 has been closed.
2024-04-18 19:30:13,153 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.85:41317', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468613.1537275')
2024-04-18 19:30:13,154 - distributed.core - INFO - Connection to tcp://10.201.3.85:49766 has been closed.
2024-04-18 19:30:13,154 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.85:43145', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468613.154238')
2024-04-18 19:30:13,154 - distributed.core - INFO - Connection to tcp://10.201.3.85:49790 has been closed.
2024-04-18 19:30:13,154 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.85:43979', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468613.154868')
2024-04-18 19:30:13,155 - distributed.scheduler - INFO - Lost all workers
2024-04-18 19:30:13,157 - distributed.scheduler - INFO - Stopped scheduler at 'tcp://10.201.3.80:8786'
2024-04-18 19:30:13,157 - distributed.scheduler - INFO - End scheduler
