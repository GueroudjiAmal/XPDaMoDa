2024-04-18 15:54:30,648 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-18 15:54:30,848 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-18 15:54:30,849 - distributed.scheduler - INFO - -----------------------------------------------
2024-04-18 15:54:30,857 - distributed.preloading - INFO - Creating preload: MofkaSchedulerPlugin.py
2024-04-18 15:54:30,857 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-18 15:54:30,863 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-18 15:54:31,862 - distributed.http.proxy - INFO - To route to workers diagnostics web server please install jupyter-server-proxy: python -m pip install jupyter-server-proxy
2024-04-18 15:54:31,902 - distributed.scheduler - INFO - State start
2024-04-18 15:54:31,906 - distributed.scheduler - INFO - -----------------------------------------------
/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/utils.py:181: RuntimeWarning: Couldn't detect a suitable IP address for reaching '8.8.8.8', defaulting to hostname: [Errno 101] Network is unreachable
  warnings.warn(
2024-04-18 15:54:31,923 - distributed.scheduler - INFO -   Scheduler at:    tcp://10.201.4.34:8786
2024-04-18 15:54:31,924 - distributed.scheduler - INFO -   dashboard at:  http://10.201.4.34:8787/status
2024-04-18 15:54:31,926 - distributed.preloading - INFO - Run preload setup: MofkaSchedulerPlugin.py
2024-04-18 15:54:32,759 - distributed.scheduler - INFO - Registering Worker plugin shuffle
2024-04-18 15:54:40,546 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.4.53:44561', status: init, memory: 0, processing: 0>
2024-04-18 15:54:42,541 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.4.53:44561
2024-04-18 15:54:42,541 - distributed.core - INFO - Starting established connection to tcp://10.201.4.53:56600
2024-04-18 15:54:42,542 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.4.53:38601', status: init, memory: 0, processing: 0>
2024-04-18 15:54:42,543 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.4.53:38601
2024-04-18 15:54:42,543 - distributed.core - INFO - Starting established connection to tcp://10.201.4.53:56610
2024-04-18 15:54:42,545 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.4.53:36009', status: init, memory: 0, processing: 0>
2024-04-18 15:54:42,546 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.4.53:36009
2024-04-18 15:54:42,546 - distributed.core - INFO - Starting established connection to tcp://10.201.4.53:56590
2024-04-18 15:54:42,547 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.4.53:32895', status: init, memory: 0, processing: 0>
2024-04-18 15:54:42,547 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.4.53:32895
2024-04-18 15:54:42,547 - distributed.core - INFO - Starting established connection to tcp://10.201.4.53:56606
2024-04-18 15:54:42,548 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.4.54:34447', status: init, memory: 0, processing: 0>
2024-04-18 15:54:42,549 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.4.54:34447
2024-04-18 15:54:42,549 - distributed.core - INFO - Starting established connection to tcp://10.201.4.54:47756
2024-04-18 15:54:42,550 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.4.54:40107', status: init, memory: 0, processing: 0>
2024-04-18 15:54:42,550 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.4.54:40107
2024-04-18 15:54:42,550 - distributed.core - INFO - Starting established connection to tcp://10.201.4.54:47726
2024-04-18 15:54:42,551 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.4.54:41821', status: init, memory: 0, processing: 0>
2024-04-18 15:54:42,551 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.4.54:41821
2024-04-18 15:54:42,551 - distributed.core - INFO - Starting established connection to tcp://10.201.4.54:47740
2024-04-18 15:54:42,552 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.4.54:38559', status: init, memory: 0, processing: 0>
2024-04-18 15:54:42,552 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.4.54:38559
2024-04-18 15:54:42,552 - distributed.core - INFO - Starting established connection to tcp://10.201.4.54:47772
2024-04-18 15:54:42,567 - distributed.scheduler - INFO - Receive client connection: Client-f845f890-fd9b-11ee-88db-6805cae1205a
2024-04-18 15:54:42,568 - distributed.core - INFO - Starting established connection to tcp://10.201.4.43:35492
2024-04-18 15:54:47,655 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 4.69s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 15:55:42,720 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 8.27s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 15:56:44,573 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 9.01s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 15:56:55,758 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 11.07s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 15:58:05,609 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 8.87s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 15:58:05,619 - distributed.scheduler - INFO - Scheduler closing due to unknown reason...
2024-04-18 15:58:05,620 - distributed.scheduler - INFO - Scheduler closing all comms
2024-04-18 15:58:05,621 - distributed.core - INFO - Connection to tcp://10.201.4.53:56600 has been closed.
2024-04-18 15:58:05,621 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.4.53:44561', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713455885.6217763')
2024-04-18 15:58:06,159 - distributed.core - INFO - Connection to tcp://10.201.4.53:56590 has been closed.
2024-04-18 15:58:06,159 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.4.53:36009', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713455886.1597137')
2024-04-18 15:58:06,160 - distributed.core - INFO - Connection to tcp://10.201.4.53:56606 has been closed.
2024-04-18 15:58:06,160 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.4.53:32895', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713455886.1605039')
2024-04-18 15:58:06,161 - distributed.core - INFO - Connection to tcp://10.201.4.53:56610 has been closed.
2024-04-18 15:58:06,161 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.4.53:38601', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713455886.1614087')
2024-04-18 15:58:06,162 - distributed.core - INFO - Connection to tcp://10.201.4.54:47756 has been closed.
2024-04-18 15:58:06,162 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.4.54:34447', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713455886.16221')
2024-04-18 15:58:06,162 - distributed.core - INFO - Connection to tcp://10.201.4.54:47726 has been closed.
2024-04-18 15:58:06,162 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.4.54:40107', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713455886.162798')
2024-04-18 15:58:06,163 - distributed.core - INFO - Connection to tcp://10.201.4.54:47740 has been closed.
2024-04-18 15:58:06,163 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.4.54:41821', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713455886.1632829')
2024-04-18 15:58:06,163 - distributed.core - INFO - Connection to tcp://10.201.4.54:47772 has been closed.
2024-04-18 15:58:06,163 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.4.54:38559', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713455886.163769')
2024-04-18 15:58:06,164 - distributed.scheduler - INFO - Lost all workers
2024-04-18 15:58:06,165 - distributed.scheduler - INFO - Stopped scheduler at 'tcp://10.201.4.34:8786'
2024-04-18 15:58:06,166 - distributed.scheduler - INFO - End scheduler
