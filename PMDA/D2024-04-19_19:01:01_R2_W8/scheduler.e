2024-04-19 19:01:25,563 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-19 19:01:25,756 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-19 19:01:25,757 - distributed.scheduler - INFO - -----------------------------------------------
2024-04-19 19:01:25,764 - distributed.preloading - INFO - Creating preload: MofkaSchedulerPlugin.py
2024-04-19 19:01:25,764 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-19 19:01:25,770 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-19 19:01:26,758 - distributed.http.proxy - INFO - To route to workers diagnostics web server please install jupyter-server-proxy: python -m pip install jupyter-server-proxy
2024-04-19 19:01:26,798 - distributed.scheduler - INFO - State start
2024-04-19 19:01:26,802 - distributed.scheduler - INFO - -----------------------------------------------
/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/utils.py:181: RuntimeWarning: Couldn't detect a suitable IP address for reaching '8.8.8.8', defaulting to hostname: [Errno 101] Network is unreachable
  warnings.warn(
2024-04-19 19:01:26,820 - distributed.scheduler - INFO -   Scheduler at:    tcp://10.201.3.55:8786
2024-04-19 19:01:26,821 - distributed.scheduler - INFO -   dashboard at:  http://10.201.3.55:8787/status
2024-04-19 19:01:26,823 - distributed.preloading - INFO - Run preload setup: MofkaSchedulerPlugin.py
2024-04-19 19:01:27,675 - distributed.scheduler - INFO - Registering Worker plugin shuffle
2024-04-19 19:01:33,655 - distributed.scheduler - INFO - Receive client connection: Client-3c36d79b-fe7f-11ee-94fb-6805cae1ded6
2024-04-19 19:01:35,576 - distributed.core - INFO - Starting established connection to tcp://10.201.3.33:49716
2024-04-19 19:01:35,664 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.137:39483', status: init, memory: 0, processing: 0>
2024-04-19 19:01:35,664 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.137:39483
2024-04-19 19:01:35,664 - distributed.core - INFO - Starting established connection to tcp://10.201.3.137:40240
2024-04-19 19:01:35,665 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.137:34103', status: init, memory: 0, processing: 0>
2024-04-19 19:01:35,666 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.137:34103
2024-04-19 19:01:35,666 - distributed.core - INFO - Starting established connection to tcp://10.201.3.137:40212
2024-04-19 19:01:35,667 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.137:37277', status: init, memory: 0, processing: 0>
2024-04-19 19:01:35,667 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.137:37277
2024-04-19 19:01:35,667 - distributed.core - INFO - Starting established connection to tcp://10.201.3.137:40216
2024-04-19 19:01:35,668 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.137:35227', status: init, memory: 0, processing: 0>
2024-04-19 19:01:35,668 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.137:35227
2024-04-19 19:01:35,668 - distributed.core - INFO - Starting established connection to tcp://10.201.3.137:40250
2024-04-19 19:01:35,669 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.137:40053', status: init, memory: 0, processing: 0>
2024-04-19 19:01:35,669 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.137:40053
2024-04-19 19:01:35,670 - distributed.core - INFO - Starting established connection to tcp://10.201.3.137:40270
2024-04-19 19:01:35,670 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.137:36485', status: init, memory: 0, processing: 0>
2024-04-19 19:01:35,671 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.137:36485
2024-04-19 19:01:35,671 - distributed.core - INFO - Starting established connection to tcp://10.201.3.137:40286
2024-04-19 19:01:35,671 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.137:44931', status: init, memory: 0, processing: 0>
2024-04-19 19:01:35,672 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.137:44931
2024-04-19 19:01:35,672 - distributed.core - INFO - Starting established connection to tcp://10.201.3.137:40228
2024-04-19 19:01:35,672 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.137:41867', status: init, memory: 0, processing: 0>
2024-04-19 19:01:35,673 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.137:41867
2024-04-19 19:01:35,673 - distributed.core - INFO - Starting established connection to tcp://10.201.3.137:40264
2024-04-19 19:02:10,982 - distributed.scheduler - INFO - Scheduler closing due to unknown reason...
2024-04-19 19:02:10,983 - distributed.scheduler - INFO - Scheduler closing all comms
2024-04-19 19:02:10,984 - distributed.core - INFO - Connection to tcp://10.201.3.137:40240 has been closed.
2024-04-19 19:02:10,984 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.137:39483', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553330.9847865')
2024-04-19 19:02:11,006 - distributed.core - INFO - Connection to tcp://10.201.3.137:40212 has been closed.
2024-04-19 19:02:11,006 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.137:34103', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553331.0062387')
2024-04-19 19:02:11,006 - distributed.core - INFO - Connection to tcp://10.201.3.137:40216 has been closed.
2024-04-19 19:02:11,006 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.137:37277', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553331.0069072')
2024-04-19 19:02:11,007 - distributed.core - INFO - Connection to tcp://10.201.3.137:40250 has been closed.
2024-04-19 19:02:11,007 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.137:35227', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553331.0074077')
2024-04-19 19:02:11,007 - distributed.core - INFO - Connection to tcp://10.201.3.137:40270 has been closed.
2024-04-19 19:02:11,007 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.137:40053', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553331.0078988')
2024-04-19 19:02:11,008 - distributed.core - INFO - Connection to tcp://10.201.3.137:40286 has been closed.
2024-04-19 19:02:11,008 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.137:36485', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553331.0083523')
2024-04-19 19:02:11,008 - distributed.core - INFO - Connection to tcp://10.201.3.137:40228 has been closed.
2024-04-19 19:02:11,008 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.137:44931', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553331.0087783')
2024-04-19 19:02:11,009 - distributed.core - INFO - Connection to tcp://10.201.3.137:40264 has been closed.
2024-04-19 19:02:11,009 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.137:41867', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553331.0092027')
2024-04-19 19:02:11,009 - distributed.scheduler - INFO - Lost all workers
2024-04-19 19:02:11,011 - distributed.scheduler - INFO - Stopped scheduler at 'tcp://10.201.3.55:8786'
2024-04-19 19:02:11,011 - distributed.scheduler - INFO - End scheduler
