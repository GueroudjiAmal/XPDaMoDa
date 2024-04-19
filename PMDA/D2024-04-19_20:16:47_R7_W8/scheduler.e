2024-04-19 20:17:12,739 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-19 20:17:12,970 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-19 20:17:12,971 - distributed.scheduler - INFO - -----------------------------------------------
2024-04-19 20:17:12,980 - distributed.preloading - INFO - Creating preload: MofkaSchedulerPlugin.py
2024-04-19 20:17:12,980 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-19 20:17:12,986 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-19 20:17:14,082 - distributed.http.proxy - INFO - To route to workers diagnostics web server please install jupyter-server-proxy: python -m pip install jupyter-server-proxy
2024-04-19 20:17:14,123 - distributed.scheduler - INFO - State start
2024-04-19 20:17:14,126 - distributed.scheduler - INFO - -----------------------------------------------
/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/utils.py:181: RuntimeWarning: Couldn't detect a suitable IP address for reaching '8.8.8.8', defaulting to hostname: [Errno 101] Network is unreachable
  warnings.warn(
2024-04-19 20:17:14,147 - distributed.scheduler - INFO -   Scheduler at:   tcp://10.201.3.204:8786
2024-04-19 20:17:14,147 - distributed.scheduler - INFO -   dashboard at:  http://10.201.3.204:8787/status
2024-04-19 20:17:14,150 - distributed.preloading - INFO - Run preload setup: MofkaSchedulerPlugin.py
2024-04-19 20:17:15,008 - distributed.scheduler - INFO - Registering Worker plugin shuffle
2024-04-19 20:17:22,222 - distributed.scheduler - INFO - Receive client connection: Client-d32053f5-fe89-11ee-92c8-6805cadd6766
2024-04-19 20:17:24,239 - distributed.core - INFO - Starting established connection to tcp://10.201.3.207:34670
2024-04-19 20:17:24,241 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.212:33643', status: init, memory: 0, processing: 0>
2024-04-19 20:17:24,242 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.212:33643
2024-04-19 20:17:24,242 - distributed.core - INFO - Starting established connection to tcp://10.201.3.212:54698
2024-04-19 20:17:24,243 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.212:33303', status: init, memory: 0, processing: 0>
2024-04-19 20:17:24,243 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.212:33303
2024-04-19 20:17:24,243 - distributed.core - INFO - Starting established connection to tcp://10.201.3.212:54714
2024-04-19 20:17:24,246 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.212:35403', status: init, memory: 0, processing: 0>
2024-04-19 20:17:24,246 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.212:35403
2024-04-19 20:17:24,246 - distributed.core - INFO - Starting established connection to tcp://10.201.3.212:54638
2024-04-19 20:17:24,247 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.212:37209', status: init, memory: 0, processing: 0>
2024-04-19 20:17:24,247 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.212:37209
2024-04-19 20:17:24,247 - distributed.core - INFO - Starting established connection to tcp://10.201.3.212:54680
2024-04-19 20:17:24,248 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.212:39385', status: init, memory: 0, processing: 0>
2024-04-19 20:17:24,248 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.212:39385
2024-04-19 20:17:24,248 - distributed.core - INFO - Starting established connection to tcp://10.201.3.212:54700
2024-04-19 20:17:24,249 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.212:45873', status: init, memory: 0, processing: 0>
2024-04-19 20:17:24,249 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.212:45873
2024-04-19 20:17:24,249 - distributed.core - INFO - Starting established connection to tcp://10.201.3.212:54654
2024-04-19 20:17:24,250 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.212:40345', status: init, memory: 0, processing: 0>
2024-04-19 20:17:24,250 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.212:40345
2024-04-19 20:17:24,250 - distributed.core - INFO - Starting established connection to tcp://10.201.3.212:54690
2024-04-19 20:17:24,251 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.212:43359', status: init, memory: 0, processing: 0>
2024-04-19 20:17:24,251 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.212:43359
2024-04-19 20:17:24,251 - distributed.core - INFO - Starting established connection to tcp://10.201.3.212:54664
2024-04-19 20:17:59,898 - distributed.scheduler - INFO - Scheduler closing due to unknown reason...
2024-04-19 20:17:59,899 - distributed.scheduler - INFO - Scheduler closing all comms
2024-04-19 20:17:59,900 - distributed.core - INFO - Connection to tcp://10.201.3.212:54698 has been closed.
2024-04-19 20:17:59,900 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.212:33643', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713557879.9007583')
2024-04-19 20:17:59,925 - distributed.core - INFO - Connection to tcp://10.201.3.212:54714 has been closed.
2024-04-19 20:17:59,925 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.212:33303', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713557879.9253585')
2024-04-19 20:17:59,925 - distributed.core - INFO - Connection to tcp://10.201.3.212:54638 has been closed.
2024-04-19 20:17:59,926 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.212:35403', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713557879.9259882')
2024-04-19 20:17:59,926 - distributed.core - INFO - Connection to tcp://10.201.3.212:54680 has been closed.
2024-04-19 20:17:59,926 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.212:37209', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713557879.9265335')
2024-04-19 20:17:59,927 - distributed.core - INFO - Connection to tcp://10.201.3.212:54700 has been closed.
2024-04-19 20:17:59,927 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.212:39385', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713557879.927111')
2024-04-19 20:17:59,927 - distributed.core - INFO - Connection to tcp://10.201.3.212:54654 has been closed.
2024-04-19 20:17:59,927 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.212:45873', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713557879.927576')
2024-04-19 20:17:59,927 - distributed.core - INFO - Connection to tcp://10.201.3.212:54690 has been closed.
2024-04-19 20:17:59,928 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.212:40345', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713557879.9280367')
2024-04-19 20:17:59,928 - distributed.core - INFO - Connection to tcp://10.201.3.212:54664 has been closed.
2024-04-19 20:17:59,928 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.212:43359', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713557879.9285295')
2024-04-19 20:17:59,928 - distributed.scheduler - INFO - Lost all workers
2024-04-19 20:17:59,930 - distributed.scheduler - INFO - Stopped scheduler at 'tcp://10.201.3.204:8786'
2024-04-19 20:17:59,930 - distributed.scheduler - INFO - End scheduler
