2024-04-18 19:26:30,234 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-18 19:26:30,439 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-18 19:26:30,440 - distributed.scheduler - INFO - -----------------------------------------------
2024-04-18 19:26:30,448 - distributed.preloading - INFO - Creating preload: MofkaSchedulerPlugin.py
2024-04-18 19:26:30,448 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-18 19:26:30,454 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-18 19:26:31,441 - distributed.http.proxy - INFO - To route to workers diagnostics web server please install jupyter-server-proxy: python -m pip install jupyter-server-proxy
2024-04-18 19:26:31,481 - distributed.scheduler - INFO - State start
2024-04-18 19:26:31,484 - distributed.scheduler - INFO - -----------------------------------------------
/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/utils.py:181: RuntimeWarning: Couldn't detect a suitable IP address for reaching '8.8.8.8', defaulting to hostname: [Errno 101] Network is unreachable
  warnings.warn(
2024-04-18 19:26:31,502 - distributed.scheduler - INFO -   Scheduler at:    tcp://10.201.3.27:8786
2024-04-18 19:26:31,503 - distributed.scheduler - INFO -   dashboard at:  http://10.201.3.27:8787/status
2024-04-18 19:26:31,505 - distributed.preloading - INFO - Run preload setup: MofkaSchedulerPlugin.py
2024-04-18 19:26:32,343 - distributed.scheduler - INFO - Registering Worker plugin shuffle
2024-04-18 19:26:39,909 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.4:36599', status: init, memory: 0, processing: 0>
2024-04-18 19:26:41,717 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.4:36599
2024-04-18 19:26:41,717 - distributed.core - INFO - Starting established connection to tcp://10.201.3.4:37678
2024-04-18 19:26:41,719 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.4:33919', status: init, memory: 0, processing: 0>
2024-04-18 19:26:41,719 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.4:33919
2024-04-18 19:26:41,719 - distributed.core - INFO - Starting established connection to tcp://10.201.3.4:37654
2024-04-18 19:26:41,722 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.4:46821', status: init, memory: 0, processing: 0>
2024-04-18 19:26:41,722 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.4:46821
2024-04-18 19:26:41,722 - distributed.core - INFO - Starting established connection to tcp://10.201.3.4:37672
2024-04-18 19:26:41,723 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.4:37935', status: init, memory: 0, processing: 0>
2024-04-18 19:26:41,723 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.4:37935
2024-04-18 19:26:41,724 - distributed.core - INFO - Starting established connection to tcp://10.201.3.4:37670
2024-04-18 19:26:41,724 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.2.255:40505', status: init, memory: 0, processing: 0>
2024-04-18 19:26:41,725 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.2.255:40505
2024-04-18 19:26:41,725 - distributed.core - INFO - Starting established connection to tcp://10.201.2.255:57624
2024-04-18 19:26:41,725 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.2.255:35261', status: init, memory: 0, processing: 0>
2024-04-18 19:26:41,726 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.2.255:35261
2024-04-18 19:26:41,726 - distributed.core - INFO - Starting established connection to tcp://10.201.2.255:57628
2024-04-18 19:26:41,726 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.2.255:42543', status: init, memory: 0, processing: 0>
2024-04-18 19:26:41,727 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.2.255:42543
2024-04-18 19:26:41,727 - distributed.core - INFO - Starting established connection to tcp://10.201.2.255:57642
2024-04-18 19:26:41,727 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.2.255:36919', status: init, memory: 0, processing: 0>
2024-04-18 19:26:41,728 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.2.255:36919
2024-04-18 19:26:41,728 - distributed.core - INFO - Starting established connection to tcp://10.201.2.255:57636
2024-04-18 19:26:47,176 - distributed.scheduler - INFO - Receive client connection: Client-97cc2393-fdb9-11ee-b491-6805cae19c84
2024-04-18 19:26:47,177 - distributed.core - INFO - Starting established connection to tcp://10.201.3.26:51512
2024-04-18 19:26:59,800 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 11.47s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 19:27:15,683 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 15.84s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 19:30:11,449 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 25.76s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 19:30:11,471 - distributed.scheduler - INFO - Scheduler closing due to unknown reason...
2024-04-18 19:30:11,473 - distributed.scheduler - INFO - Scheduler closing all comms
2024-04-18 19:30:11,474 - distributed.core - INFO - Connection to tcp://10.201.3.4:37678 has been closed.
2024-04-18 19:30:11,474 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.4:36599', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468611.4746733')
2024-04-18 19:30:11,986 - distributed.core - INFO - Connection to tcp://10.201.3.4:37654 has been closed.
2024-04-18 19:30:11,987 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.4:33919', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468611.9870114')
2024-04-18 19:30:11,987 - distributed.core - INFO - Connection to tcp://10.201.3.4:37672 has been closed.
2024-04-18 19:30:11,987 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.4:46821', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468611.9876294')
2024-04-18 19:30:11,988 - distributed.core - INFO - Connection to tcp://10.201.3.4:37670 has been closed.
2024-04-18 19:30:11,988 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.4:37935', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468611.9881783')
2024-04-18 19:30:11,988 - distributed.core - INFO - Connection to tcp://10.201.2.255:57624 has been closed.
2024-04-18 19:30:11,988 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.2.255:40505', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468611.9887059')
2024-04-18 19:30:11,989 - distributed.core - INFO - Connection to tcp://10.201.2.255:57628 has been closed.
2024-04-18 19:30:11,989 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.2.255:35261', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468611.9891758')
2024-04-18 19:30:11,989 - distributed.core - INFO - Connection to tcp://10.201.2.255:57642 has been closed.
2024-04-18 19:30:11,989 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.2.255:42543', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468611.989666')
2024-04-18 19:30:11,990 - distributed.core - INFO - Connection to tcp://10.201.2.255:57636 has been closed.
2024-04-18 19:30:11,990 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.2.255:36919', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713468611.9900916')
2024-04-18 19:30:11,990 - distributed.scheduler - INFO - Lost all workers
2024-04-18 19:30:11,992 - distributed.scheduler - INFO - Stopped scheduler at 'tcp://10.201.3.27:8786'
2024-04-18 19:30:11,992 - distributed.scheduler - INFO - End scheduler
