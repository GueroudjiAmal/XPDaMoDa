2024-04-19 19:01:30,908 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-19 19:01:31,153 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-19 19:01:31,154 - distributed.scheduler - INFO - -----------------------------------------------
2024-04-19 19:01:31,162 - distributed.preloading - INFO - Creating preload: MofkaSchedulerPlugin.py
2024-04-19 19:01:31,162 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-19 19:01:31,168 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-19 19:01:32,154 - distributed.http.proxy - INFO - To route to workers diagnostics web server please install jupyter-server-proxy: python -m pip install jupyter-server-proxy
2024-04-19 19:01:32,195 - distributed.scheduler - INFO - State start
2024-04-19 19:01:32,198 - distributed.scheduler - INFO - -----------------------------------------------
/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/utils.py:181: RuntimeWarning: Couldn't detect a suitable IP address for reaching '8.8.8.8', defaulting to hostname: [Errno 101] Network is unreachable
  warnings.warn(
2024-04-19 19:01:32,216 - distributed.scheduler - INFO -   Scheduler at:   tcp://10.201.3.173:8786
2024-04-19 19:01:32,217 - distributed.scheduler - INFO -   dashboard at:  http://10.201.3.173:8787/status
2024-04-19 19:01:32,219 - distributed.preloading - INFO - Run preload setup: MofkaSchedulerPlugin.py
2024-04-19 19:01:33,060 - distributed.scheduler - INFO - Registering Worker plugin shuffle
2024-04-19 19:01:39,603 - distributed.scheduler - INFO - Receive client connection: Client-3fa97407-fe7f-11ee-89c0-6805cae1d902
2024-04-19 19:01:41,887 - distributed.core - INFO - Starting established connection to tcp://10.201.2.152:44848
2024-04-19 19:01:41,889 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.2.137:38981', status: init, memory: 0, processing: 0>
2024-04-19 19:01:41,889 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.2.137:38981
2024-04-19 19:01:41,890 - distributed.core - INFO - Starting established connection to tcp://10.201.2.137:48224
2024-04-19 19:01:41,890 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.2.137:35217', status: init, memory: 0, processing: 0>
2024-04-19 19:01:41,891 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.2.137:35217
2024-04-19 19:01:41,891 - distributed.core - INFO - Starting established connection to tcp://10.201.2.137:48254
2024-04-19 19:01:41,899 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.2.137:38977', status: init, memory: 0, processing: 0>
2024-04-19 19:01:41,899 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.2.137:38977
2024-04-19 19:01:41,899 - distributed.core - INFO - Starting established connection to tcp://10.201.2.137:48252
2024-04-19 19:01:41,900 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.2.137:46839', status: init, memory: 0, processing: 0>
2024-04-19 19:01:41,900 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.2.137:46839
2024-04-19 19:01:41,900 - distributed.core - INFO - Starting established connection to tcp://10.201.2.137:48238
2024-04-19 19:01:41,901 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.2.137:46069', status: init, memory: 0, processing: 0>
2024-04-19 19:01:41,901 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.2.137:46069
2024-04-19 19:01:41,901 - distributed.core - INFO - Starting established connection to tcp://10.201.2.137:48200
2024-04-19 19:01:41,902 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.2.137:33089', status: init, memory: 0, processing: 0>
2024-04-19 19:01:41,902 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.2.137:33089
2024-04-19 19:01:41,902 - distributed.core - INFO - Starting established connection to tcp://10.201.2.137:48178
2024-04-19 19:01:41,903 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.2.137:35753', status: init, memory: 0, processing: 0>
2024-04-19 19:01:41,903 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.2.137:35753
2024-04-19 19:01:41,904 - distributed.core - INFO - Starting established connection to tcp://10.201.2.137:48214
2024-04-19 19:01:41,904 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.2.137:37489', status: init, memory: 0, processing: 0>
2024-04-19 19:01:41,905 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.2.137:37489
2024-04-19 19:01:41,905 - distributed.core - INFO - Starting established connection to tcp://10.201.2.137:48184
2024-04-19 19:01:46,896 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 4.59s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 19:02:20,303 - distributed.scheduler - INFO - Scheduler closing due to unknown reason...
2024-04-19 19:02:20,303 - distributed.scheduler - INFO - Scheduler closing all comms
2024-04-19 19:02:20,305 - distributed.core - INFO - Connection to tcp://10.201.2.137:48224 has been closed.
2024-04-19 19:02:20,305 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.2.137:38981', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.305115')
2024-04-19 19:02:20,326 - distributed.core - INFO - Connection to tcp://10.201.2.137:48254 has been closed.
2024-04-19 19:02:20,326 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.2.137:35217', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.3264897')
2024-04-19 19:02:20,326 - distributed.core - INFO - Connection to tcp://10.201.2.137:48252 has been closed.
2024-04-19 19:02:20,327 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.2.137:38977', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.3270504')
2024-04-19 19:02:20,327 - distributed.core - INFO - Connection to tcp://10.201.2.137:48238 has been closed.
2024-04-19 19:02:20,327 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.2.137:46839', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.3275847')
2024-04-19 19:02:20,328 - distributed.core - INFO - Connection to tcp://10.201.2.137:48200 has been closed.
2024-04-19 19:02:20,328 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.2.137:46069', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.3280575')
2024-04-19 19:02:20,328 - distributed.core - INFO - Connection to tcp://10.201.2.137:48178 has been closed.
2024-04-19 19:02:20,328 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.2.137:33089', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.328501')
2024-04-19 19:02:20,328 - distributed.core - INFO - Connection to tcp://10.201.2.137:48214 has been closed.
2024-04-19 19:02:20,328 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.2.137:35753', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.3289351')
2024-04-19 19:02:20,329 - distributed.core - INFO - Connection to tcp://10.201.2.137:48184 has been closed.
2024-04-19 19:02:20,329 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.2.137:37489', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713553340.3293688')
2024-04-19 19:02:20,329 - distributed.scheduler - INFO - Lost all workers
2024-04-19 19:02:20,331 - distributed.scheduler - INFO - Stopped scheduler at 'tcp://10.201.3.173:8786'
2024-04-19 19:02:20,331 - distributed.scheduler - INFO - End scheduler
