2024-04-19 19:01:22,664 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-19 19:01:22,714 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-19 19:01:22,715 - distributed.scheduler - INFO - -----------------------------------------------
2024-04-19 19:01:22,719 - distributed.preloading - INFO - Creating preload: MofkaSchedulerPlugin.py
2024-04-19 19:01:22,719 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-19 19:01:22,724 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-19 19:01:23,284 - distributed.http.proxy - INFO - To route to workers diagnostics web server please install jupyter-server-proxy: python -m pip install jupyter-server-proxy
2024-04-19 19:01:23,321 - distributed.scheduler - INFO - State start
2024-04-19 19:01:23,324 - distributed.scheduler - INFO - -----------------------------------------------
/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/utils.py:181: RuntimeWarning: Couldn't detect a suitable IP address for reaching '8.8.8.8', defaulting to hostname: [Errno 101] Network is unreachable
  warnings.warn(
2024-04-19 19:01:23,328 - distributed.scheduler - INFO -   Scheduler at:     tcp://10.201.3.1:8786
2024-04-19 19:01:23,328 - distributed.scheduler - INFO -   dashboard at:  http://10.201.3.1:8787/status
2024-04-19 19:01:23,331 - distributed.preloading - INFO - Run preload setup: MofkaSchedulerPlugin.py
2024-04-19 19:01:24,136 - distributed.scheduler - INFO - Registering Worker plugin shuffle
2024-04-19 19:01:25,912 - distributed.scheduler - INFO - Receive client connection: Client-384db906-fe7f-11ee-9316-6805cacee6a0
2024-04-19 19:01:26,453 - distributed.core - INFO - Starting established connection to tcp://10.201.3.2:46896
2024-04-19 19:01:28,061 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.1.183:43427', status: init, memory: 0, processing: 0>
2024-04-19 19:01:28,062 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.1.183:43427
2024-04-19 19:01:28,062 - distributed.core - INFO - Starting established connection to tcp://10.201.1.183:41160
2024-04-19 19:01:28,063 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.1.183:33377', status: init, memory: 0, processing: 0>
2024-04-19 19:01:28,063 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.1.183:33377
2024-04-19 19:01:28,063 - distributed.core - INFO - Starting established connection to tcp://10.201.1.183:41172
2024-04-19 19:01:28,064 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.1.183:39371', status: init, memory: 0, processing: 0>
2024-04-19 19:01:28,064 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.1.183:39371
2024-04-19 19:01:28,064 - distributed.core - INFO - Starting established connection to tcp://10.201.1.183:41226
2024-04-19 19:01:28,065 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.1.183:35477', status: init, memory: 0, processing: 0>
2024-04-19 19:01:28,066 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.1.183:35477
2024-04-19 19:01:28,066 - distributed.core - INFO - Starting established connection to tcp://10.201.1.183:41214
2024-04-19 19:01:28,066 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.1.183:39179', status: init, memory: 0, processing: 0>
2024-04-19 19:01:28,067 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.1.183:39179
2024-04-19 19:01:28,067 - distributed.core - INFO - Starting established connection to tcp://10.201.1.183:41184
2024-04-19 19:01:28,068 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.1.183:33821', status: init, memory: 0, processing: 0>
2024-04-19 19:01:28,068 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.1.183:33821
2024-04-19 19:01:28,068 - distributed.core - INFO - Starting established connection to tcp://10.201.1.183:41216
2024-04-19 19:01:28,069 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.1.183:45569', status: init, memory: 0, processing: 0>
2024-04-19 19:01:28,069 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.1.183:45569
2024-04-19 19:01:28,069 - distributed.core - INFO - Starting established connection to tcp://10.201.1.183:41150
2024-04-19 19:01:28,070 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.1.183:37247', status: init, memory: 0, processing: 0>
2024-04-19 19:01:28,070 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.1.183:37247
2024-04-19 19:01:28,070 - distributed.core - INFO - Starting established connection to tcp://10.201.1.183:41198
2024-04-19 19:02:09,188 - distributed.scheduler - INFO - Scheduler closing due to unknown reason...
2024-04-19 19:02:09,189 - distributed.scheduler - INFO - Scheduler closing all comms
2024-04-19 19:02:09,190 - distributed.core - INFO - Connection to tcp://10.201.1.183:41160 has been closed.
2024-04-19 19:02:09,190 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.1.183:43427', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553329.1907423')
2024-04-19 19:02:09,209 - distributed.core - INFO - Connection to tcp://10.201.1.183:41172 has been closed.
2024-04-19 19:02:09,209 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.1.183:33377', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553329.2095852')
2024-04-19 19:02:09,210 - distributed.core - INFO - Connection to tcp://10.201.1.183:41226 has been closed.
2024-04-19 19:02:09,210 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.1.183:39371', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553329.2106864')
2024-04-19 19:02:09,211 - distributed.core - INFO - Connection to tcp://10.201.1.183:41214 has been closed.
2024-04-19 19:02:09,211 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.1.183:35477', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553329.2112906')
2024-04-19 19:02:09,211 - distributed.core - INFO - Connection to tcp://10.201.1.183:41184 has been closed.
2024-04-19 19:02:09,211 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.1.183:39179', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553329.211828')
2024-04-19 19:02:09,212 - distributed.core - INFO - Connection to tcp://10.201.1.183:41216 has been closed.
2024-04-19 19:02:09,212 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.1.183:33821', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553329.2123113')
2024-04-19 19:02:09,212 - distributed.core - INFO - Connection to tcp://10.201.1.183:41150 has been closed.
2024-04-19 19:02:09,212 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.1.183:45569', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553329.2128294')
2024-04-19 19:02:09,213 - distributed.core - INFO - Connection to tcp://10.201.1.183:41198 has been closed.
2024-04-19 19:02:09,213 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.1.183:37247', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553329.21334')
2024-04-19 19:02:09,213 - distributed.scheduler - INFO - Lost all workers
2024-04-19 19:02:09,215 - distributed.scheduler - INFO - Stopped scheduler at 'tcp://10.201.3.1:8786'
2024-04-19 19:02:09,215 - distributed.scheduler - INFO - End scheduler
