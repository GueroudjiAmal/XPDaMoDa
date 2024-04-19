2024-04-19 17:16:10,397 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-19 17:16:10,596 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-19 17:16:10,597 - distributed.scheduler - INFO - -----------------------------------------------
2024-04-19 17:16:10,604 - distributed.preloading - INFO - Creating preload: MofkaSchedulerPlugin.py
2024-04-19 17:16:10,605 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-19 17:16:10,611 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-19 17:16:11,589 - distributed.http.proxy - INFO - To route to workers diagnostics web server please install jupyter-server-proxy: python -m pip install jupyter-server-proxy
2024-04-19 17:16:11,630 - distributed.scheduler - INFO - State start
2024-04-19 17:16:11,634 - distributed.scheduler - INFO - -----------------------------------------------
/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/utils.py:181: RuntimeWarning: Couldn't detect a suitable IP address for reaching '8.8.8.8', defaulting to hostname: [Errno 101] Network is unreachable
  warnings.warn(
2024-04-19 17:16:11,651 - distributed.scheduler - INFO -   Scheduler at:    tcp://10.201.3.51:8786
2024-04-19 17:16:11,651 - distributed.scheduler - INFO -   dashboard at:  http://10.201.3.51:8787/status
2024-04-19 17:16:11,654 - distributed.preloading - INFO - Run preload setup: MofkaSchedulerPlugin.py
2024-04-19 17:16:12,519 - distributed.scheduler - INFO - Registering Worker plugin shuffle
2024-04-19 17:16:20,845 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.138:40137', status: init, memory: 0, processing: 0>
2024-04-19 17:16:22,753 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.138:40137
2024-04-19 17:16:22,753 - distributed.core - INFO - Starting established connection to tcp://10.201.3.138:56476
2024-04-19 17:16:22,755 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.138:42565', status: init, memory: 0, processing: 0>
2024-04-19 17:16:22,755 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.138:42565
2024-04-19 17:16:22,755 - distributed.core - INFO - Starting established connection to tcp://10.201.3.138:56446
2024-04-19 17:16:22,759 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.138:46757', status: init, memory: 0, processing: 0>
2024-04-19 17:16:22,759 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.138:46757
2024-04-19 17:16:22,759 - distributed.core - INFO - Starting established connection to tcp://10.201.3.138:56454
2024-04-19 17:16:22,760 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.138:40517', status: init, memory: 0, processing: 0>
2024-04-19 17:16:22,760 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.138:40517
2024-04-19 17:16:22,760 - distributed.core - INFO - Starting established connection to tcp://10.201.3.138:56464
2024-04-19 17:16:22,761 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.131:43355', status: init, memory: 0, processing: 0>
2024-04-19 17:16:22,761 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.131:43355
2024-04-19 17:16:22,761 - distributed.core - INFO - Starting established connection to tcp://10.201.3.131:48436
2024-04-19 17:16:22,762 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.131:37489', status: init, memory: 0, processing: 0>
2024-04-19 17:16:22,762 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.131:37489
2024-04-19 17:16:22,762 - distributed.core - INFO - Starting established connection to tcp://10.201.3.131:48422
2024-04-19 17:16:22,763 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.131:46391', status: init, memory: 0, processing: 0>
2024-04-19 17:16:22,763 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.131:46391
2024-04-19 17:16:22,764 - distributed.core - INFO - Starting established connection to tcp://10.201.3.131:48452
2024-04-19 17:16:22,764 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.3.131:41811', status: init, memory: 0, processing: 0>
2024-04-19 17:16:22,765 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.3.131:41811
2024-04-19 17:16:22,765 - distributed.core - INFO - Starting established connection to tcp://10.201.3.131:48416
2024-04-19 17:16:23,569 - distributed.scheduler - INFO - Receive client connection: Client-8c1c0252-fe70-11ee-8310-6805cac57d4a
2024-04-19 17:16:23,569 - distributed.core - INFO - Starting established connection to tcp://10.201.3.50:41706
2024-04-19 17:16:27,888 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 3.99s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:27:10,860 - distributed.worker - INFO - Run out-of-band function '_start_tracker'
2024-04-19 17:27:12,765 - distributed.scheduler - INFO - Receive client connection: Client-worker-0f11bc01-fe72-11ee-93f0-6805cad942a0
2024-04-19 17:27:12,769 - distributed.core - INFO - Starting established connection to tcp://10.201.3.131:43856
2024-04-19 17:27:12,894 - distributed.scheduler - INFO - Receive client connection: Client-worker-0f127ddf-fe72-11ee-93ee-6805cad942a0
2024-04-19 17:27:12,895 - distributed.core - INFO - Starting established connection to tcp://10.201.3.131:43866
2024-04-19 17:27:12,895 - distributed.scheduler - INFO - Receive client connection: Client-worker-0f127b23-fe72-11ee-b770-6805cacc0ca2
2024-04-19 17:27:12,896 - distributed.core - INFO - Starting established connection to tcp://10.201.3.138:42358
2024-04-19 17:27:12,896 - distributed.scheduler - INFO - Receive client connection: Client-worker-0f127c29-fe72-11ee-b772-6805cacc0ca2
2024-04-19 17:27:12,896 - distributed.core - INFO - Starting established connection to tcp://10.201.3.138:42364
2024-04-19 17:27:12,897 - distributed.scheduler - INFO - Receive client connection: Client-worker-0f127fab-fe72-11ee-93f1-6805cad942a0
2024-04-19 17:27:12,897 - distributed.core - INFO - Starting established connection to tcp://10.201.3.131:43870
2024-04-19 17:27:12,897 - distributed.scheduler - INFO - Receive client connection: Client-worker-0f1282aa-fe72-11ee-b773-6805cacc0ca2
2024-04-19 17:27:12,898 - distributed.core - INFO - Starting established connection to tcp://10.201.3.138:42376
2024-04-19 17:27:12,898 - distributed.scheduler - INFO - Receive client connection: Client-worker-0f128650-fe72-11ee-93ef-6805cad942a0
2024-04-19 17:27:12,898 - distributed.core - INFO - Starting established connection to tcp://10.201.3.131:43882
2024-04-19 17:27:12,898 - distributed.scheduler - INFO - Receive client connection: Client-worker-0f13304c-fe72-11ee-b771-6805cacc0ca2
2024-04-19 17:27:12,899 - distributed.core - INFO - Starting established connection to tcp://10.201.3.138:42378
2024-04-19 17:30:05,168 - distributed.worker - INFO - Run out-of-band function '_start_tracker'
2024-04-19 17:32:24,523 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 4.75s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:33:01,465 - distributed.worker - INFO - Run out-of-band function '_start_tracker'
2024-04-19 17:36:08,572 - distributed.worker - INFO - Run out-of-band function '_start_tracker'
2024-04-19 17:39:09,827 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 12.57s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-19 17:40:01,722 - distributed.worker - INFO - Run out-of-band function '_start_tracker'
2024-04-19 17:43:07,118 - distributed.scheduler - INFO - Retire worker addresses {'tcp://10.201.3.131:37489': {'type': 'Worker', 'id': 'tcp://10.201.3.131:37489', 'host': '10.201.3.131', 'resources': {}, 'local_directory': '/tmp/dask-scratch-space/worker-jjb6embj', 'name': 'tcp://10.201.3.131:37489', 'nthreads': 8, 'memory_limit': 540332154880, 'last_seen': 1713548586.3018832, 'services': {'dashboard': 35535}, 'metrics': {'task_counts': {'released': 356, 'memory': 65, 'waiting': 1, 'flight': 1}, 'bandwidth': {'total': 425439696.04953295, 'workers': {}, 'types': {}}, 'digests_total_since_heartbeat': {'latency': 0.0013360977172851562, 'tick-duration': 0.5355861186981201, 'transfer-bandwidth': 500317.32672015147, 'transfer-duration': 0.01053166389465332, ('gather-dep', 'decompress', 'seconds'): 1.620291732251644e-05, ('gather-dep', 'deserialize', 'seconds'): 0.00011307303793728352, ('gather-dep', 'network', 'seconds'): 0.01040238793939352, ('gather-dep', 'other', 'seconds'): 0.01874322211369872, ('execute', 'mean_combine-partial', 'memory-read', 'count'): 4, ('execute', 'mean_combine-partial', 'memory-read', 'bytes'): 1536, ('execute', 'mean_combine-partial', 'thread-cpu', 'seconds'): 0.00020447299999659663, ('execute', 'mean_combine-partial', 'thread-noncpu', 'seconds'): 4.4674415164536185e-05, ('execute', 'mean_combine-partial', 'executor', 'seconds'): 0.0011349705746397376, ('execute', 'mean_combine-partial', 'other', 'seconds'): 0.0004765280755236745, ('get-data', 'memory-read', 'count'): 1, ('get-data', 'memory-read', 'bytes'): 384, ('get-data', 'serialize', 'seconds'): 0.00010427297092974186, ('get-data', 'compress', 'seconds'): 8.450937457382679e-06, ('get-data', 'network', 'seconds'): 0.004047203459776938}, 'managed_bytes': 29229303667, 'spilled_bytes': {'memory': 0, 'disk': 0}, 'transfer': {'incoming_bytes': 384, 'incoming_count': 1, 'incoming_count_total': 111, 'outgoing_bytes': 0, 'outgoing_count': 0, 'outgoing_count_total': 157}, 'event_loop_interval': 0.02001246452331543, 'cpu': 101.9, 'memory': 41734533120, 'time': 1713548585.7660396, 'host_net_io': {'read_bps': 1968.153002422257, 'write_bps': 7031.401437080124}, 'host_disk_io': {'read_bps': 0.0, 'write_bps': 0.0}, 'num_fds': 66}, 'status': 'running', 'nanny': 'tcp://10.201.3.131:33697'}, 'tcp://10.201.3.131:41811': {'type': 'Worker', 'id': 'tcp://10.201.3.131:41811', 'host': '10.201.3.131', 'resources': {}, 'local_directory': '/tmp/dask-scratch-space/worker-2a9925c5', 'name': 'tcp://10.201.3.131:41811', 'nthreads': 8, 'memory_limit': 540332154880, 'last_seen': 1713548586.267137, 'services': {'dashboard': 45841}, 'metrics': {'task_counts': {'released': 479, 'memory': 91}, 'bandwidth': {'total': 421010126.0322892, 'workers': {}, 'types': {}}, 'digests_total_since_heartbeat': {'latency': 0.0014696121215820312, 'tick-duration': 0.49944424629211426}, 'managed_bytes': 34954743863, 'spilled_bytes': {'memory': 0, 'disk': 0}, 'transfer': {'incoming_bytes': 0, 'incoming_count': 0, 'incoming_count_total': 127, 'outgoing_bytes': 0, 'outgoing_count': 0, 'outgoing_count_total': 104}, 'event_loop_interval': 0.020008430480957032, 'cpu': 101.9, 'memory': 47004282880, 'time': 1713548585.7675736, 'host_net_io': {'read_bps': 2491.9774858087408, 'write_bps': 7032.292520097}, 'host_disk_io': {'read_bps': 0.0, 'write_bps': 0.0}, 'num_fds': 66}, 'status': 'running', 'nanny': 'tcp://10.201.3.131:33171'}, 'tcp://10.201.3.131:43355': {'type': 'Worker', 'id': 'tcp://10.201.3.131:43355', 'host': '10.201.3.131', 'resources': {}, 'local_directory': '/tmp/dask-scratch-space/worker-zw0amui4', 'name': 'tcp://10.201.3.131:43355', 'nthreads': 8, 'memory_limit': 540332154880, 'last_seen': 1713548586.2644675, 'services': {'dashboard': 36441}, 'metrics': {'task_counts': {'released': 586, 'memory': 112}, 'bandwidth': {'total': 1137725634.8131955, 'workers': {}, 'types': {}}, 'digests_total_since_heartbeat': {'latency': 0.0015833377838134766, 'tick-duration': 0.5001845359802246, ('get-data', 'memory-read', 'count'): 1, ('get-data', 'memory-read', 'bytes'): 384, ('get-data', 'serialize', 'seconds'): 0.000100292032584548, ('get-data', 'compress', 'seconds'): 6.215996108949184e-06, ('get-data', 'network', 'seconds'): 0.0006697828648611903}, 'managed_bytes': 41517471215, 'spilled_bytes': {'memory': 0, 'disk': 0}, 'transfer': {'incoming_bytes': 0, 'incoming_count': 0, 'incoming_count_total': 160, 'outgoing_bytes': 0, 'outgoing_count': 0, 'outgoing_count_total': 102}, 'event_loop_interval': 0.019985899925231934, 'cpu': 104.1, 'memory': 57980178432, 'time': 1713548585.7640924, 'host_net_io': {'read_bps': 1971.5186570078222, 'write_bps': 7043.425537066524}, 'host_disk_io': {'read_bps': 0.0, 'write_bps': 0.0}, 'num_fds': 66}, 'status': 'running', 'nanny': 'tcp://10.201.3.131:41911'}, 'tcp://10.201.3.131:46391': {'type': 'Worker', 'id': 'tcp://10.201.3.131:46391', 'host': '10.201.3.131', 'resources': {}, 'local_directory': '/tmp/dask-scratch-space/worker-0rt7p2vg', 'name': 'tcp://10.201.3.131:46391', 'nthreads': 8, 'memory_limit': 540332154880, 'last_seen': 1713548586.2662787, 'services': {'dashboard': 42981}, 'metrics': {'task_counts': {'released': 117, 'memory': 25}, 'bandwidth': {'total': 336256273.2693247, 'workers': {}, 'types': {}}, 'digests_total_since_heartbeat': {'latency': 0.001409292221069336, 'tick-duration': 0.4993627071380615, ('get-data', 'memory-read', 'count'): 1, ('get-data', 'memory-read', 'bytes'): 384, ('get-data', 'serialize', 'seconds'): 0.00020882603712379932, ('get-data', 'compress', 'seconds'): 6.704009138047695e-06, ('get-data', 'network', 'seconds'): 0.0039770606672391295}, 'managed_bytes': 17930448447, 'spilled_bytes': {'memory': 0, 'disk': 0}, 'transfer': {'incoming_bytes': 0, 'incoming_count': 0, 'incoming_count_total': 92, 'outgoing_bytes': 0, 'outgoing_count': 0, 'outgoing_count_total': 244}, 'event_loop_interval': 0.0200124454498291, 'cpu': 104.0, 'memory': 25023488000, 'time': 1713548585.7669709, 'host_net_io': {'read_bps': 1708.5984742904343, 'write_bps': 5427.901242095958}, 'host_disk_io': {'read_bps': 0.0, 'write_bps': 0.0}, 'num_fds': 66}, 'status': 'running', 'nanny': 'tcp://10.201.3.131:43555'}, 'tcp://10.201.3.138:40137': {'type': 'Worker', 'id': 'tcp://10.201.3.138:40137', 'host': '10.201.3.138', 'resources': {}, 'local_directory': '/tmp/dask-scratch-space/worker-zjjo96fb', 'name': 'tcp://10.201.3.138:40137', 'nthreads': 8, 'memory_limit': 540332154880, 'last_seen': 1713548586.2562058, 'services': {'dashboard': 42047}, 'metrics': {'task_counts': {'released': 418, 'memory': 68}, 'bandwidth': {'total': 295691534.6120895, 'workers': {}, 'types': {}}, 'digests_total_since_heartbeat': {'latency': 0.001508474349975586, 'tick-duration': 0.49880051612854004, ('get-data', 'memory-read', 'count'): 1, ('get-data', 'memory-read', 'bytes'): 384, ('get-data', 'serialize', 'seconds'): 0.00010993005707859993, ('get-data', 'compress', 'seconds'): 7.682014256715775e-06, ('get-data', 'network', 'seconds'): 0.0007583377882838249}, 'managed_bytes': 33070946552, 'spilled_bytes': {'memory': 0, 'disk': 0}, 'transfer': {'incoming_bytes': 0, 'incoming_count': 0, 'incoming_count_total': 82, 'outgoing_bytes': 0, 'outgoing_count': 0, 'outgoing_count_total': 184}, 'event_loop_interval': 0.020016188621520995, 'cpu': 100.1, 'memory': 45507735552, 'time': 1713548585.7558584, 'host_net_io': {'read_bps': 1889.1667415492618, 'write_bps': 6988.315954968243}, 'host_disk_io': {'read_bps': 0.0, 'write_bps': 0.0}, 'num_fds': 66}, 'status': 'running', 'nanny': 'tcp://10.201.3.138:36141'}, 'tcp://10.201.3.138:40517': {'type': 'Worker', 'id': 'tcp://10.201.3.138:40517', 'host': '10.201.3.138', 'resources': {}, 'local_directory': '/tmp/dask-scratch-space/worker-ycg67qib', 'name': 'tcp://10.201.3.138:40517', 'nthreads': 8, 'memory_limit': 540332154880, 'last_seen': 1713548586.263222, 'services': {'dashboard': 45749}, 'metrics': {'task_counts': {'released': 556, 'memory': 102}, 'bandwidth': {'total': 480985734.4490007, 'workers': {}, 'types': {}}, 'digests_total_since_heartbeat': {'latency': 0.0015287399291992188, 'tick-duration': 0.49901366233825684, ('get-data', 'memory-read', 'count'): 1, ('get-data', 'memory-read', 'bytes'): 384, ('get-data', 'serialize', 'seconds'): 0.00010092020966112614, ('get-data', 'compress', 'seconds'): 6.146961823105812e-06, ('get-data', 'network', 'seconds'): 0.004234535153955221, 'transfer-bandwidth': 397326.052019104, 'transfer-duration': 0.005691051483154297, ('gather-dep', 'decompress', 'seconds'): 1.2360978871583939e-05, ('gather-dep', 'deserialize', 'seconds'): 8.862907998263836e-05, ('gather-dep', 'network', 'seconds'): 0.005590061424300075, ('gather-dep', 'other', 'seconds'): 0.010162466438487172, ('execute', 'mean_combine-partial', 'memory-read', 'count'): 4, ('execute', 'mean_combine-partial', 'memory-read', 'bytes'): 1536, ('execute', 'mean_combine-partial', 'thread-cpu', 'seconds'): 0.0001445509999982164, ('execute', 'mean_combine-partial', 'thread-noncpu', 'seconds'): 3.116349279963515e-05, ('execute', 'mean_combine-partial', 'executor', 'seconds'): 0.0005553795490413904, ('execute', 'mean_combine-partial', 'other', 'seconds'): 0.0006794119253754616}, 'managed_bytes': 40875987256, 'spilled_bytes': {'memory': 0, 'disk': 0}, 'transfer': {'incoming_bytes': 0, 'incoming_count': 0, 'incoming_count_total': 147, 'outgoing_bytes': 0, 'outgoing_count': 0, 'outgoing_count_total': 99}, 'event_loop_interval': 0.02000269412994385, 'cpu': 101.7, 'memory': 55118364672, 'time': 1713548585.7637143, 'host_net_io': {'read_bps': 1621.8676920858877, 'write_bps': 6858.525369484972}, 'host_disk_io': {'read_bps': 0.0, 'write_bps': 0.0}, 'num_fds': 66}, 'status': 'running', 'nanny': 'tcp://10.201.3.138:42229'}, 'tcp://10.201.3.138:42565': {'type': 'Worker', 'id': 'tcp://10.201.3.138:42565', 'host': '10.201.3.138', 'resources': {}, 'local_directory': '/tmp/dask-scratch-space/worker-hiszxoez', 'name': 'tcp://10.201.3.138:42565', 'nthreads': 8, 'memory_limit': 540332154880, 'last_seen': 1713548586.2591681, 'services': {'dashboard': 32997}, 'metrics': {'task_counts': {'released': 574, 'memory': 106}, 'bandwidth': {'total': 399261538.1551016, 'workers': {}, 'types': {}}, 'digests_total_since_heartbeat': {'latency': 0.003685474395751953, 'profile-duration': 0.002588033676147461, 'tick-duration': 0.49971508979797363, ('execute', 'pd_split', 'memory-read', 'count'): 2, ('execute', 'pd_split', 'memory-read', 'bytes'): 2165954101, ('execute', 'pd_split', 'thread-cpu', 'seconds'): 14.862378500000034, ('execute', 'pd_split', 'thread-noncpu', 'seconds'): 1.5444544334258694, ('execute', 'pd_split', 'executor', 'seconds'): 0.764465423533693, ('execute', 'pd_split', 'other', 'seconds'): 0.001234087161719799, ('execute', 'mean_chunk', 'memory-read', 'count'): 1, ('execute', 'mean_chunk', 'memory-read', 'bytes'): 384, ('execute', 'mean_chunk', 'thread-cpu', 'seconds'): 6.845900000485017e-05, ('execute', 'mean_chunk', 'thread-noncpu', 'seconds'): 3.0961547480501395e-05, ('execute', 'mean_chunk', 'executor', 'seconds'): 0.0007063334342092276, ('execute', 'mean_chunk', 'other', 'seconds'): 0.0007269030902534723, 'transfer-bandwidth': 398765.2629010415, 'transfer-duration': 0.005910396575927734, ('gather-dep', 'decompress', 'seconds'): 1.250184141099453e-05, ('gather-dep', 'deserialize', 'seconds'): 0.00010029086843132973, ('gather-dep', 'network', 'seconds'): 0.00579760386608541, ('gather-dep', 'other', 'seconds'): 0.01076431036926806, ('execute', 'mean_combine-partial', 'memory-read', 'count'): 4, ('execute', 'mean_combine-partial', 'memory-read', 'bytes'): 1536, ('execute', 'mean_combine-partial', 'thread-cpu', 'seconds'): 0.00010768299995334019, ('execute', 'mean_combine-partial', 'thread-noncpu', 'seconds'): 2.0109358445097314e-05, ('execute', 'mean_combine-partial', 'executor', 'seconds'): 0.0005122288130223751, ('execute', 'mean_combine-partial', 'other', 'seconds'): 0.0006557358428835869, ('get-data', 'memory-read', 'count'): 1, ('get-data', 'memory-read', 'bytes'): 384, ('get-data', 'serialize', 'seconds'): 0.00010336400009691715, ('get-data', 'compress', 'seconds'): 7.612165063619614e-06, ('get-data', 'network', 'seconds'): 0.004322417313233018}, 'managed_bytes': 41724578412, 'spilled_bytes': {'memory': 0, 'disk': 0}, 'transfer': {'incoming_bytes': 0, 'incoming_count': 0, 'incoming_count_total': 155, 'outgoing_bytes': 0, 'outgoing_count': 0, 'outgoing_count_total': 93}, 'event_loop_interval': 0.02006167411804199, 'cpu': 203.0, 'memory': 61043556352, 'time': 1713548585.7607129, 'host_net_io': {'read_bps': 1617.6971046933916, 'write_bps': 6840.888863389767}, 'host_disk_io': {'read_bps': 0.0, 'write_bps': 0.0}, 'num_fds': 66}, 'status': 'running', 'nanny': 'tcp://10.201.3.138:34809'}, 'tcp://10.201.3.138:46757': {'type': 'Worker', 'id': 'tcp://10.201.3.138:46757', 'host': '10.201.3.138', 'resources': {}, 'local_directory': '/tmp/dask-scratch-space/worker-ec4vdmdn', 'name': 'tcp://10.201.3.138:46757', 'nthreads': 8, 'memory_limit': 540332154880, 'last_seen': 1713548586.3046117, 'services': {'dashboard': 46135}, 'metrics': {'task_counts': {'released': 507, 'memory': 90}, 'bandwidth': {'total': 273014241.97477174, 'workers': {}, 'types': {}}, 'digests_total_since_heartbeat': {'latency': 0.0013346672058105469, 'tick-duration': 0.5435051918029785, ('get-data', 'memory-read', 'count'): 1, ('get-data', 'memory-read', 'bytes'): 384, ('get-data', 'serialize', 'seconds'): 0.00011391099542379379, ('get-data', 'compress', 'seconds'): 7.262919098138809e-06, ('get-data', 'network', 'seconds'): 0.0007516765035688877}, 'managed_bytes': 38403759800, 'spilled_bytes': {'memory': 0, 'disk': 0}, 'transfer': {'incoming_bytes': 0, 'incoming_count': 0, 'incoming_count_total': 144, 'outgoing_bytes': 0, 'outgoing_count': 0, 'outgoing_count_total': 115}, 'event_loop_interval': 0.020007610321044922, 'cpu': 102.0, 'memory': 53809397760, 'time': 1713548585.7618508, 'host_net_io': {'read_bps': 1887.4696666441737, 'write_bps': 6982.03821601849}, 'host_disk_io': {'read_bps': 0.0, 'write_bps': 0.0}, 'num_fds': 66}, 'status': 'running', 'nanny': 'tcp://10.201.3.138:34167'}}
2024-04-19 17:43:07,118 - distributed.scheduler - INFO - Retiring worker 'tcp://10.201.3.131:41811' (stimulus_id='retire-workers-1713548587.1185577')
2024-04-19 17:43:07,127 - distributed.scheduler - INFO - Retiring worker 'tcp://10.201.3.138:40137' (stimulus_id='retire-workers-1713548587.1185577')
2024-04-19 17:43:07,133 - distributed.scheduler - INFO - Retiring worker 'tcp://10.201.3.138:46757' (stimulus_id='retire-workers-1713548587.1185577')
2024-04-19 17:43:07,139 - distributed.scheduler - INFO - Retiring worker 'tcp://10.201.3.138:42565' (stimulus_id='retire-workers-1713548587.1185577')
2024-04-19 17:43:07,146 - distributed.scheduler - INFO - Retiring worker 'tcp://10.201.3.138:40517' (stimulus_id='retire-workers-1713548587.1185577')
2024-04-19 17:43:07,147 - distributed.scheduler - INFO - Retiring worker 'tcp://10.201.3.131:43355' (stimulus_id='retire-workers-1713548587.1185577')
2024-04-19 17:43:07,147 - distributed.scheduler - INFO - Retiring worker 'tcp://10.201.3.131:46391' (stimulus_id='retire-workers-1713548587.1185577')
2024-04-19 17:43:07,147 - distributed.scheduler - INFO - Retiring worker 'tcp://10.201.3.131:37489' (stimulus_id='retire-workers-1713548587.1185577')
2024-04-19 17:43:07,148 - distributed.active_memory_manager - WARNING - Tried retiring worker tcp://10.201.3.138:40517, but 101 tasks could not be moved as there are no suitable workers to receive them. The worker will not be retired.
2024-04-19 17:43:07,148 - distributed.active_memory_manager - WARNING - Tried retiring worker tcp://10.201.3.138:40137, but 66 tasks could not be moved as there are no suitable workers to receive them. The worker will not be retired.
2024-04-19 17:43:07,148 - distributed.active_memory_manager - WARNING - Tried retiring worker tcp://10.201.3.138:42565, but 104 tasks could not be moved as there are no suitable workers to receive them. The worker will not be retired.
2024-04-19 17:43:07,149 - distributed.active_memory_manager - WARNING - Tried retiring worker tcp://10.201.3.131:41811, but 91 tasks could not be moved as there are no suitable workers to receive them. The worker will not be retired.
2024-04-19 17:43:07,149 - distributed.active_memory_manager - WARNING - Tried retiring worker tcp://10.201.3.131:46391, but 25 tasks could not be moved as there are no suitable workers to receive them. The worker will not be retired.
2024-04-19 17:43:07,149 - distributed.active_memory_manager - WARNING - Tried retiring worker tcp://10.201.3.138:46757, but 88 tasks could not be moved as there are no suitable workers to receive them. The worker will not be retired.
2024-04-19 17:43:07,149 - distributed.active_memory_manager - WARNING - Tried retiring worker tcp://10.201.3.131:37489, but 64 tasks could not be moved as there are no suitable workers to receive them. The worker will not be retired.
2024-04-19 17:43:07,150 - distributed.active_memory_manager - WARNING - Tried retiring worker tcp://10.201.3.131:43355, but 110 tasks could not be moved as there are no suitable workers to receive them. The worker will not be retired.
2024-04-19 17:43:07,150 - distributed.scheduler - WARNING - Could not retire worker 'tcp://10.201.3.131:41811': unique data could not be moved to any other worker (stimulus_id='retire-workers-1713548587.1185577')
2024-04-19 17:43:07,150 - distributed.scheduler - WARNING - Could not retire worker 'tcp://10.201.3.138:40137': unique data could not be moved to any other worker (stimulus_id='retire-workers-1713548587.1185577')
2024-04-19 17:43:07,150 - distributed.scheduler - WARNING - Could not retire worker 'tcp://10.201.3.138:46757': unique data could not be moved to any other worker (stimulus_id='retire-workers-1713548587.1185577')
2024-04-19 17:43:07,151 - distributed.scheduler - WARNING - Could not retire worker 'tcp://10.201.3.138:42565': unique data could not be moved to any other worker (stimulus_id='retire-workers-1713548587.1185577')
2024-04-19 17:43:07,151 - distributed.scheduler - WARNING - Could not retire worker 'tcp://10.201.3.138:40517': unique data could not be moved to any other worker (stimulus_id='retire-workers-1713548587.1185577')
2024-04-19 17:43:07,151 - distributed.scheduler - WARNING - Could not retire worker 'tcp://10.201.3.131:43355': unique data could not be moved to any other worker (stimulus_id='retire-workers-1713548587.1185577')
2024-04-19 17:43:07,151 - distributed.scheduler - WARNING - Could not retire worker 'tcp://10.201.3.131:46391': unique data could not be moved to any other worker (stimulus_id='retire-workers-1713548587.1185577')
2024-04-19 17:43:07,151 - distributed.scheduler - WARNING - Could not retire worker 'tcp://10.201.3.131:37489': unique data could not be moved to any other worker (stimulus_id='retire-workers-1713548587.1185577')
2024-04-19 17:43:10,258 - distributed.scheduler - INFO - Scheduler closing due to unknown reason...
2024-04-19 17:43:10,259 - distributed.scheduler - INFO - Scheduler closing all comms
2024-04-19 17:43:10,260 - distributed.core - INFO - Connection to tcp://10.201.3.138:56476 has been closed.
2024-04-19 17:43:10,260 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.138:40137', status: running, memory: 66, processing: 0> (stimulus_id='handle-worker-cleanup-1713548590.2604918')
2024-04-19 17:43:10,260 - distributed.scheduler - WARNING - Removing worker 'tcp://10.201.3.138:40137' caused the cluster to lose already computed task(s), which will be recomputed elsewhere: {'dict-d651137d-c60f-4f91-8e9c-31277d732216', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 55), 'dict-13261a4d-ab79-4ffa-93a9-13481ea74532', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 0), 'dict-f50ca52a-c2cc-47d6-a46a-a79b6cfe3be9', 'dict-9464afa8-ad19-4d41-b620-a2b7436bd173', 'dict-7d39c889-e821-4c3a-ab35-49ecd361d5a6', 'dict-2be88f09-2aff-4bdc-ac86-8a5422ae1990', 'dict-03d8bae3-6375-44b1-9410-66c3f7664a89', 'dict-b72c5c0a-128f-409c-a8dd-ef802f2c8484', 'dict-a527b0c4-b248-44ea-9313-4c4f1345cb05', 'dict-b304595d-9a44-4373-96b3-aed9db22dccf', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 33), 'dict-546a383c-c599-4660-b3b3-6bcc95094e13', 'dict-75baf14c-16c0-4dc4-958d-e9c8287ae83e', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 48), 'dict-683f87ac-5321-4fa9-bc1d-89603c906954', 'dict-1b801eec-4c59-4c50-a914-9531d576c07c', 'dict-c34a2754-a60e-43ab-8f01-554b14b54392', 'dict-fa81b328-5f93-4dec-9370-825098102c0d', 'dict-e7d3b595-e7df-438d-b473-a9e0af934c3a', 'dict-0a9b25ce-bc91-42be-918c-85c470a4324a', 'dict-838235a3-d7ff-4660-af38-c378cde8dd85', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 11), 'dict-a6b52836-a9c1-4ecb-9141-bdbdf4d542c6', 'dict-1f5a16d9-4663-49fa-9b98-f6c814b241e4', 'dict-1d33a985-f415-4564-b1d5-2ec91858a730', 'dict-cdda4f6e-a692-4d31-a3b6-9662f2e1c360', 'dict-b2e0a64c-2a6f-4a80-b592-969eba17b4f9', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 26), 'dict-c0b2cbe0-359d-4ffe-8128-145d26bbdc47', 'dict-3ac956d2-6ee0-4aa6-a7df-c8dd2864257e', 'dict-c7832c84-ba97-43eb-b63d-c17712bcfdd6', 'dict-a0886260-301f-4b7b-baeb-53f3f39be1dd', 'dict-1be28512-9851-44f5-9d8e-0e11abfdabcb', 'dict-7936cd98-144f-4746-aed8-052d5c59812c', 'dict-f14e0b7c-8ff7-4f21-9772-e8d211ed76de', 'dict-df7af46b-7826-40f1-8edb-f7f9933de5e4', 'dict-a088447e-ac38-4354-bc53-ce451ed33e85', 'dict-19d6534c-8675-4e25-8b42-0d87d1fe82aa', 'dict-a6c13e22-b699-467a-92ce-8c6b22b71ed6', 'dict-28d518af-fb53-4646-91f0-2b5a2467bb39', 'dict-6d8adaa3-a96e-4e01-98c3-f9e2abebc38c', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 62), 'dict-3d112f23-ef32-46df-a82d-533558e45d6f', 'dict-e87b64d4-a654-4bc9-bea3-2a61d8837a67', 'dict-cc24e32b-0796-4fc5-a496-cc5430c37f3f', 'dict-634944df-8d16-4295-a1e6-a1e83643aad1', 'dict-4944c25d-a5e8-4b45-bec6-5aa3bf0fd036', 'dict-459be495-88e0-4d9b-b46a-adf35bcce02c', 'dict-e76ec714-8d67-49ef-947b-a44a93a5236a', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 7), 'dict-f8db5fdc-9f35-4a3e-ab94-f3a4800e932c', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 19), 'dict-bbdabc52-4f1c-42d6-96d8-4d4957c3efd2', 'dict-958bf304-bec6-4005-a9f1-b6bd069473a3', 'dict-c6d8e7b3-9be5-41de-b4f2-a7ec7547865d', 'dict-e18bb8bb-70e9-4a01-bd3a-8eb60f59946b', 'dict-26c5ef78-003d-4e31-a03f-cbec1d6afd0c', 'dict-a6de481a-053f-46eb-b9f1-ff6e4fb9fd9f', 'dict-4055560b-9c46-4bc3-b49b-42763befb41e', 'dict-a11c7c36-1944-4b70-a10c-334120e37db2', 'dict-c4bc9313-d6bd-40da-875a-40d2a1af2507', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 40), 'dict-ffb426d5-5636-44ae-a357-9dc467d72377', 'dict-00766ce9-9341-42d5-a54e-02615d46708b'} (stimulus_id='handle-worker-cleanup-1713548590.2604918')
2024-04-19 17:43:10,892 - distributed.core - INFO - Connection to tcp://10.201.3.138:56446 has been closed.
2024-04-19 17:43:10,892 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.138:42565', status: running, memory: 104, processing: 2> (stimulus_id='handle-worker-cleanup-1713548590.8923175')
2024-04-19 17:43:10,892 - distributed.scheduler - WARNING - Removing worker 'tcp://10.201.3.138:42565' caused the cluster to lose already computed task(s), which will be recomputed elsewhere: {'dict-56946b55-9d18-4a5f-a655-58cecb0901d4', 'dict-dde1d2f6-c1c5-4d07-b72e-859d62539e35', 'dict-15ff2d6b-5e9e-489d-bd90-0c581b71246b', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 24), 'dict-9cdccf63-cc4b-4541-b206-c1ddda8365ad', 'dict-2aaad0ca-08cf-4f7a-9ea0-fab524a28403', 'dict-fab4cd4f-90b3-4499-837b-316fab84446e', 'dict-e0c70c36-531c-4822-a5c8-72823b70a29f', 'dict-d290d8e7-8caf-451f-860b-4c3f4214790b', 'dict-003a15f5-48e4-480e-9b7f-a329ce62a6b8', 'dict-ec07e0c1-dfcf-42fd-95d9-b1f8e9c6cc2e', 'dict-2344c653-e5bf-4df6-84a0-5558c24cfbcd', 'dict-7592ecea-3653-42d5-8cee-9c2328e51103', 'dict-f4afc2eb-daf4-4770-bc8c-6fa9b2eb2457', 'dict-f35deae3-c131-42b4-955f-e075cbbb977a', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 17), 'dict-d66af766-8116-45db-8739-279495e945c9', 'dict-02b3f560-0977-42c2-8f7c-8db7f68f6aca', 'dict-557cc00d-56f1-461a-98da-6fcd34047082', 'dict-16797e1b-ecb5-43ae-bbde-2cf322612944', 'dict-7acbfd99-e607-473d-9b6d-eed4180f1cd4', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 1), 'dict-1e98365a-20da-4814-9180-5d51f2ac3c6d', 'dict-af7e1a25-7a2e-4f2e-bdf1-5201bf90e5c1', 'dict-e0ed32e5-896c-4ea1-b7ad-2b1bae7b4872', 'dict-b74a5d16-abfd-4a85-bcf7-b8b42de79217', 'dict-c03586cb-b810-4cdd-81a6-54a23960ac24', 'dict-c98715eb-3ba3-4d98-af7e-9b0fe48c35ca', 'dict-6431bbe0-6b0b-478e-a942-a08489d1fb28', 'dict-9716f883-5b1d-482d-b399-1145ac34294b', 'dict-866ba090-6e2f-454f-96ef-39473bd85034', 'dict-aea747c0-9c7d-4853-9a75-ffc97dcf172d', 'dict-d4df662c-e960-446c-a28c-6f67bf94f515', 'dict-c229926f-a608-49f4-83c3-0a2de61bf819', 'dict-4cf9522f-d929-40cd-a056-57fd06531052', 'dict-b76a4ac1-3633-4eeb-b1a0-7662ce5e5330', 'dict-1a315f24-2d32-446f-aa6b-0785d51e7b77', 'dict-731fd52f-938d-4d6b-b76b-873ed0479368', 'dict-edfe95f8-b2a9-4c52-8215-0ce7ee90e02f', 'dict-b9ab484d-23df-4ed0-96ec-20578e5c1288', 'dict-36f34efc-c0d6-49ee-a92d-8c77d1f456b3', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 60), 'dict-b320e61b-ec13-41de-a4a3-7355be95b55a', 'dict-fdae234d-7d7c-4034-84e6-ef16284167d3', 'dict-430e970a-fb82-47d7-bdc0-17d71058e74b', 'dict-652a1c7b-d824-45b4-8b15-09750c479bc4', 'dict-722738fc-946e-48fa-81fa-26d033c66107', 'dict-9c407b75-fe6d-485a-8f3e-e21053f3f65a', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 53), 'dict-9593f24a-8691-49c5-a8aa-af6d64426348', 'dict-90743bc3-7515-4667-85c6-cc2773181aff', 'dict-c3b6a2d4-32f3-43d0-bd5e-9236fa60cf2a', 'dict-eda4588d-9559-4f95-b585-f09aaf94cb17', 'dict-14789df2-4d3b-43f8-a28c-869dd393470e', 'dict-b3ac86aa-76f1-44fa-ab59-4c9bb8e561df', 'dict-be397a08-d22e-4974-b9be-5f517a1f8cd6', 'dict-ccce164d-4176-49f9-a7be-fbb675c69bc2', 'dict-d3fb3381-ddfc-45b2-86c9-f5a6f871a0f8', 'dict-126f0784-c712-4c0c-b06e-e12264f94c2c', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 46), 'dict-dbd19887-2795-4902-9b3f-97c9938714af', 'dict-d4ff3f96-bbf4-48cc-876c-9fb683c5801c', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 39), 'dict-566d036b-0634-4796-a8f8-a36dd2eb901b', 'dict-77e1a4af-9108-4031-a31c-39e0d068cf67', 'dict-6de7c1fa-3a1b-46fb-8fe4-75cd3aca869b', 'dict-7b976aaa-4f7b-4d96-9261-bde4df5381fc', 'dict-6a692eb8-609c-4210-a9bc-9b4945f86755', 'dict-5788c410-c21c-47c4-9d45-3f9c2ae72063', 'dict-2b9cb0fe-f0e6-4a01-be6b-f13e7ef2722b', 'dict-0b77a97b-b071-4256-87da-3a03560284c2', 'dict-26de3d64-73a9-4e24-9b27-1fc19e165662', 'dict-eba2256e-85d3-4b72-bfde-06f2a01f28fd', 'dict-75fa6477-3e05-49a8-b72b-60f1e79201ce', 'dict-771aee82-1842-4bdf-8a21-007fe1783db7', 'dict-ddd2165d-3353-4297-b89b-141dc10eb172', 'dict-5aff5b86-25a7-406a-bc0a-e9cc13ff0306', 'dict-32ce4d6f-19bd-4e80-a8e5-242fb7714097', 'dict-35d6d321-70e3-4549-990e-209c3a05d453', 'dict-569424bb-f42f-483b-b5b6-bc57b8dbd8c2', 'dict-6eb1e4b5-7829-4088-855a-6a49cc018088', 'dict-aa3b5196-82bf-4b6f-b415-f6485cfa8c5d', 'dict-e3913ed4-3ddd-4660-ad3c-67ed3cc9bf52', 'dict-f8b6199c-49eb-4b11-bb38-289daa94ecdc', 'dict-a88f6cf8-50bf-434b-81b1-0e6ba89f86c6', 'dict-197e2759-dc46-4d0f-9443-fd65723f9c46', 'dict-66cd7255-7231-49bf-9a12-000b5406cf94', 'dict-a512434a-cd16-4af3-8456-a56250854788', 'dict-fe01bc64-63ce-4ed7-a696-f32a48ae55e2', 'dict-97a0b6ea-ac24-4b52-8fa1-ee3fc3499e75', 'dict-e28b418a-0be3-42eb-a811-66d2b96e9c8e', 'dict-0f623a5e-424b-4e8b-9f24-b9674bac5a67', 'dict-d2a669f9-64ab-4951-809b-b1224003b3f9', 'dict-befe643b-a6a0-4937-8f6d-a1f79e50a747', 'dict-053eff9f-3f61-4efa-8e28-b26c4735e0b3', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 68), 'dict-6a5b8055-f9aa-440c-8e84-70dac5b69893', 'dict-53c0c7c1-00fa-458f-9c98-1f5ed8017e7d', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 31), 'dict-53975063-d8d3-4e1f-9f6c-cf3b800ff1f7', 'dict-f88a2e06-ff27-49fd-a54d-02b5f78f9bf9', 'dict-d9e26bd2-f590-4f94-abcf-61695150d1c6', 'dict-8cd49e5d-76e2-45ec-9b1a-8a1a743e6a33'} (stimulus_id='handle-worker-cleanup-1713548590.8923175')
2024-04-19 17:43:10,892 - distributed.scheduler - ERROR - Removing worker 'tcp://10.201.3.138:42565' caused the cluster to lose scattered data, which can't be recovered: {'Booster-9738d99cf6fae64ecb63559a49ca5f90'} (stimulus_id='handle-worker-cleanup-1713548590.8923175')
2024-04-19 17:43:10,950 - distributed.core - INFO - Connection to tcp://10.201.3.138:56454 has been closed.
2024-04-19 17:43:10,950 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.138:46757', status: running, memory: 88, processing: 4> (stimulus_id='handle-worker-cleanup-1713548590.9501033')
2024-04-19 17:43:10,950 - distributed.scheduler - WARNING - Removing worker 'tcp://10.201.3.138:46757' caused the cluster to lose already computed task(s), which will be recomputed elsewhere: {('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 52), 'dict-2e73caa1-4086-4c62-87bf-513e7ce7edcb', 'dict-656ff3ca-da90-488e-91d3-08e8c4bd799c', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 6), 'dict-63726dd9-8162-468b-ae38-17befd876f2f', 'dict-a364d55c-2aa0-4601-acbe-805eb9c2fac9', 'dict-5854195d-4b21-4f72-8dfb-82de5d540588', 'dict-e07094e2-54bb-40fb-8547-854e44ba3c52', 'dict-c25e4db8-e900-449f-8c02-d6f2d43ff0d1', 'dict-404c8b0c-5207-4dcb-b471-fe1c1a1af3a7', 'dict-8c58c94b-5bcb-4a3f-83df-b293c09713a8', 'dict-403f1749-5f0d-450a-a251-7d836ad31b6c', 'dict-0e5a9e0d-311e-4914-979c-d51c64b91afa', 'dict-ca910850-ee14-46f5-8658-4c4c247373b3', 'dict-abc78dc8-65e5-4245-b5a7-3790e2159ac5', 'dict-f902b31d-6dbb-4deb-a84a-2bb00675958f', 'dict-3c786e67-a21c-470b-93f6-dca98dcbbd02', 'dict-8103559d-cc39-4481-b799-f1dec5d04d26', 'dict-ddf5b750-f778-4dad-8d54-f6f7c93fc57e', 'dict-fe619f59-7d76-425e-8503-407877fbfe52', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 67), 'dict-27f512b2-86a3-4ba5-b7d4-3a78db7c620b', 'dict-8a74477d-bdd5-41ac-9b70-7069fa716419', 'dict-542b089b-7209-4e6d-bea9-2d327317ce4f', 'dict-5ffb4954-8460-4103-840c-fc9bc30a93e9', 'dict-665977c3-cf1a-4b3b-b8a0-82bbdfad9459', 'dict-4e75f4d5-106d-4735-8333-5dbb6d902c7d', 'dict-5b50198c-8320-4313-9698-8ad923794517', 'dict-e0a9410b-91e5-4ad3-bb7c-92dc1232e9c0', 'dict-589929c4-7db9-4ff4-b1ff-1c45e62d8b08', 'dict-0a81b82e-37bf-48d8-87b4-99514ff04adc', 'dict-07ed0fe5-ae0d-4ee2-ad4f-d783a71359fd', 'dict-4514822c-e89b-46d7-a6e4-7d5e30fc4753', 'dict-aa7e1a1e-e11d-415e-9a15-f978fbcd3342', 'dict-5495125e-efc8-4c49-ad26-42a45243d605', 'dict-00e3f30a-a1bc-4420-a191-eb265f9f0f7c', 'dict-387dea7f-292a-4e26-97e9-a70e170cd609', 'dict-9dd024e7-79ed-400c-8d75-bced5866a253', 'dict-70761635-7b15-4caa-a689-e02736ad43c4', 'dict-6add97d6-1366-40ac-bb36-abecb50c679d', 'dict-2ffdb26a-1dff-4679-a187-cf04b0e8f829', 'dict-a62a215a-8662-48be-89ba-43e406f54e11', 'dict-2f159d66-60a7-49ee-aeba-36414c37d167', 'dict-5f1c0113-bed4-442e-b2bf-dd3b5cb8c634', 'dict-42596d3f-49b6-4092-be0b-29fe89bd9480', 'dict-0a881871-ff89-4653-91eb-96f13542f1c2', 'dict-49385928-1da3-40e7-93dc-a5d0673d7b1a', 'dict-0586d200-5785-48c6-abd7-61ada6dccd9e', 'dict-11a73e3e-f7e1-488b-8f73-caa1aced45be', 'dict-105b59d6-5711-4e9f-afde-ae1ce9238e98', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 30), 'dict-881c554b-0493-4271-897a-b0f6c86ec995', 'dict-740693e8-6a39-4c74-9239-537356e9be69', 'dict-f5af8fe0-22f4-4895-a3b9-e60c4e5b30c3', 'dict-10e68daf-120e-40af-b1bc-c35333d96e48', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 23), 'dict-410c860c-ec62-4e68-9a59-0ba89f560bf8', 'dict-5c1c00ee-88a1-4fd9-8056-32892a0611ab', 'dict-84cc40e6-cf7a-4a9f-9741-8760dc6e1f8c', 'dict-bf79c64a-6f7e-4765-bf43-eff6a489fbba', 'dict-daf6d8be-c694-4a80-b07b-1d94b03f6d0c', 'dict-7de5239c-a9da-4764-b62d-2aada698c8dc', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 16), 'dict-38abf169-7af7-45c2-8a81-172e0b2dda90', 'dict-7b37d320-3b65-421e-9168-0277d061a423', 'dict-29eab9c8-a7e8-44e7-ad3c-0c1cd3e14e44', 'dict-b441d69a-1864-4f9e-9e0f-0742762f9859', 'dict-36a43168-4a75-4e1b-a2a3-e8778d264996', 'dict-711df8f8-d905-4d24-8662-e4566a3d0ed7', 'dict-dba928ca-b90d-4559-bb8a-b9fee0b7c9c2', 'dict-fd9a8e84-b7e4-43ca-8b63-f985ee3e3b1d', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 45), 'dict-acf3d7b4-40fa-4d30-a366-d0831aa300be', 'dict-489cb0f2-4475-4e38-9266-aa129ef41c5d', 'dict-7495959e-1e35-4ac5-a917-34e7c0f01c5c', 'dict-bfe43c97-445c-473c-8688-60f5cb6f6cbd', 'dict-05a09738-bcc3-4433-ad1b-e05f617fd641', 'dict-74ccd158-5fbe-48ba-9d13-9d00f3d40f4b', 'dict-349842bb-430b-4c5e-a06f-c3378d7a3c6c', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 38), 'dict-ff258f68-f9c9-4a21-b059-95829b087e31', 'dict-6b43254b-aaf3-4645-8816-eacabf303df2', 'dict-01e46926-eba4-4096-8935-d2d564e5a0b4', 'dict-b0dee5ce-98fe-4c06-a9e4-2f13263edf5c', 'dict-4cc29b91-c9d1-4366-b4b1-230f2b829b8a', 'dict-35bd2789-97dc-444d-bf7c-7b369aa9150c', 'dict-12a1e05a-9312-4aca-9fc9-3d1612e8f3c9', 'dict-08d98426-c8c0-48fd-8f78-ebe96b8316a6'} (stimulus_id='handle-worker-cleanup-1713548590.9501033')
2024-04-19 17:43:10,999 - distributed.core - INFO - Connection to tcp://10.201.3.138:56464 has been closed.
2024-04-19 17:43:10,999 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.138:40517', status: running, memory: 101, processing: 8> (stimulus_id='handle-worker-cleanup-1713548590.999741')
2024-04-19 17:43:11,000 - distributed.scheduler - WARNING - Removing worker 'tcp://10.201.3.138:40517' caused the cluster to lose already computed task(s), which will be recomputed elsewhere: {'dict-d16d1671-0b27-471f-a503-542f5f7c497f', 'dict-e88b6467-b225-45da-a870-0c2e54ae01f3', 'dict-f8fa67aa-594d-498a-ae8e-2b4e1c335234', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 61), 'dict-ffdcae6f-acef-4814-82e9-38f347e888c1', 'dict-61f2a053-a2a3-4312-b359-e62f00bcb7dc', 'dict-748491b4-81ce-44f4-9e13-750e4a726b1d', 'dict-7e0d01d5-b63b-4ca5-bc81-184d7dc84d4f', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 54), 'dict-a640f4cb-ef65-433e-830a-77243fdb2340', 'dict-f4614864-8489-4547-b90f-d45003f74927', 'dict-1d6dae43-3f6f-4f0a-a718-e81b2ba40024', 'dict-68cef5cf-1693-4c48-9db6-b55346748896', 'dict-d2f7a762-f2f9-432f-8c19-90675bfc414d', 'dict-ed37f682-e621-46af-9888-55e08e750995', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 47), ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 10), 'dict-d880d67b-e3f0-4085-975f-61c67b17e6ac', 'dict-0dfffc4f-0a46-4eaf-bc58-3334605bc7e4', 'dict-cdaef686-85af-49f4-9537-688bbadfb4cb', 'dict-f739e6fb-bc58-499b-9e40-fc2aa5fbcd64', 'dict-d15d2cd0-49c9-4f65-867f-0ad794d5722f', 'dict-06001bf9-5dec-4c69-9fab-490ee35b9125', 'dict-ed68af11-7e0d-43f0-a5cf-076ec4341ca6', 'dict-bfe65dd9-02cd-4b4c-a89c-c3b33ee320e2', 'dict-243214a4-a86a-4b1e-9aa4-b4acbb07654a', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 69), 'dict-82016ba2-70ae-4cc5-bf09-1c41707fb9f1', 'dict-46fb2bb6-308e-4816-a95f-0245dbe30a27', 'dict-95347427-18b4-4192-9154-a4defe36789d', 'dict-5e6b8f9f-2cb5-4faa-bca8-ea3dbd778a38', 'dict-1a744d67-e1c1-4cc3-87e2-aafa104edd43', 'dict-8f765854-d24f-4e38-aadd-32ea0e090502', 'dict-0d3c0e6b-d3ce-4de0-aae1-d4f2aa9538a9', 'dict-7b08c2aa-654c-4e34-bcef-b3dd0b22c3a9', 'dict-89e1899b-fa82-4a72-afb7-5b84dd81ad9b', 'dict-3e0b5fb6-03e1-4873-9a53-722814aeab80', 'dict-55efa173-8590-4170-a373-fb38febe2098', 'dict-157cf50e-b924-425f-be1a-edb45d6e82b3', 'dict-dd2f668e-67b8-4e41-95b7-58b23d5876a7', 'dict-9cec4e31-2fc0-4f5c-9c9e-0312387efbd0', 'dict-a9bd6480-2f44-4716-ab32-effb50220848', 'dict-383b179c-37a3-4365-83ae-3602e3e5db6d', 'dict-7514f003-a13b-4580-846b-a6ded24e4934', 'dict-f1b62013-7f7c-48fa-b4a5-48a53f2ea0bc', 'dict-2d1070b0-ce2d-443e-866d-639eb210d4fd', 'dict-efb65ad8-8d3e-4429-bee4-728c6d321096', 'dict-82a9fd57-af81-472a-be89-cb7a1faf879a', 'dict-d428bbb2-64f2-4ce7-97c7-041bc0cb760c', 'dict-aa6e1aa5-d351-4ea4-b100-39aba47daea4', 'dict-f6711bda-9471-46f1-87ac-8ff4b1e8bdae', 'dict-d04e577f-ee3f-4bb8-94c6-fe6b4d4cea81', 'dict-2ecac9ef-6738-43ac-803d-f834a710a62f', 'dict-bb058c92-3fb6-4007-ad0f-1070b0172690', 'dict-f66dfef6-17ea-49e3-9b24-381e5a6b07e5', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 32), 'dict-20fa3ae7-4e26-49dd-8714-ae9f949383ce', 'dict-977708cb-46e0-4a5d-9749-1df9095e7738', 'dict-b9f963d0-c2c3-4fdc-b615-f04b6c0397b3', 'dict-812567db-0ae6-4d0d-a1e4-a5d2cfd038b3', 'dict-9665791b-01a1-41d0-9cca-f19ce2ec0b9a', 'dict-5102bbeb-5270-4560-9529-11a3a469fe4b', 'dict-45529359-6872-4a4b-83ad-1fca1596be73', 'dict-f10d0875-0304-4de6-ad10-38bda22cb464', 'dict-61df470b-e0ef-438d-960d-af8c92ea2212', 'dict-570a260e-0dca-4ecd-ad1a-c98fc31cf284', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 25), 'dict-7fd2bc68-fea8-4a73-8c84-3a70bbff2f42', 'dict-d71442e4-5abf-4dc0-97af-f4e1c299624e', 'dict-eeba6fab-8d8a-4c90-930f-39f422dc43ea', 'dict-51190fe0-c30e-4ae7-9df6-f2666f60ae62', 'dict-b70d7d54-0886-4961-990c-ec8af0df2581', 'dict-7fd31e66-504e-4768-8475-a3362f2c6def', 'dict-f254ca1b-dfd0-4887-955a-045d0f4d2696', 'dict-714d1f90-8e3b-4af4-9d6f-6a0f011f03b6', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 18), 'dict-28d51fa5-9591-4b8d-9858-a5b87ca10051', 'dict-9720e0ac-fdb1-455d-b06d-7de17878dd14', 'dict-eb8ac107-4feb-4eab-8910-9cb7404fa5f7', 'dict-d05d1a0b-a517-4289-8cca-4965ef1a8908', 'dict-ee1e2883-ffe1-4b4e-a544-09b33191cb9a', 'dict-00f14878-7295-4b32-b73c-c71920e60c72', 'dict-02410cb7-35c5-4a14-9719-7720c9b2408d', 'dict-0ae2c0dd-0c65-4700-bf4e-1231a182d8c8', 'dict-3b70d2d2-f741-4250-b442-b4d90e8ed7c7', 'dict-7cfe28d5-9df1-4892-8a24-85f48b0c5922', 'dict-3b5c0b93-5415-4163-92f2-ab3bafdf1de3', 'dict-32462c1e-b945-420a-8abb-ebb501b0cb14', 'dict-fac05c94-2b4f-42ed-a39b-88e92218cf8c', 'dict-b82a2755-fdfe-4cc2-b177-250182697adb', 'dict-90a65695-c390-498b-ae7f-ced44366644a', 'dict-73d951a6-9348-4ce3-866b-1f46ecc01603', 'dict-deb9d3d2-1cab-46e4-8b5d-694c380ef206', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 4), 'dict-81b237ae-72ff-4d38-84ed-eee6881ea270', 'dict-6dcdae6a-3ba8-44dd-95c4-d74c5d2b8f64', 'dict-92da613f-0f46-411d-8fc3-daf22c18354b', 'dict-7f63cc97-c3c7-4e5d-932d-45a5320c104f', 'dict-d8d25d5a-d29b-4983-8c01-56ae69c6566f', 'dict-37cd20b1-ebf6-43f4-ad05-385391c54aca', 'dict-10ba0280-07e8-4162-8164-3e95af997350'} (stimulus_id='handle-worker-cleanup-1713548590.999741')
2024-04-19 17:43:11,057 - distributed.core - INFO - Connection to tcp://10.201.3.131:48436 has been closed.
2024-04-19 17:43:11,057 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.131:43355', status: running, memory: 110, processing: 9> (stimulus_id='handle-worker-cleanup-1713548591.0575428')
2024-04-19 17:43:11,057 - distributed.scheduler - WARNING - Removing worker 'tcp://10.201.3.131:43355' caused the cluster to lose already computed task(s), which will be recomputed elsewhere: {'dict-9fb97add-a7a5-4420-84fc-468beabad828', 'dict-6df4c86c-e894-4d07-8fb1-5310da44850f', 'dict-6b4cc507-a6c8-4a4d-8d9b-09f339efc7fc', 'dict-ea153c3b-b9bd-42e0-b1e9-a93f920ca8f3', 'dict-8bb4a01a-5ae3-4ff3-adf1-8bc3e1c9d013', 'dict-7f60c7ba-5359-466c-b3ec-c5fcd4e15ffe', 'dict-8ca875bc-1181-428b-8bb3-f155e5871d19', 'dict-bbc1cf88-d211-4aae-bb51-d60242b13bf0', 'dict-6a3ca415-9279-47f8-8f22-450d755372c8', 'dict-af25bb6a-b6c3-497e-9eb3-a40d40b86f25', 'dict-03cca256-5c17-49d4-8ab8-ff91d533fb9f', 'dict-900e99da-9295-4700-aa44-8918f06f801f', 'dict-d0ea8201-d057-43ff-8c2e-0fb802073134', 'dict-da2710fb-13fd-4c86-8333-390f0bba66d6', 'dict-4f3363ba-d95e-4788-8370-2f9467789f65', 'dict-cbb92dcb-2c26-4059-80b8-d0c6190ea5bf', 'dict-0e2d8a65-7323-48cc-af83-8c32338f8197', 'dict-5a66cbea-562a-49e4-91f1-ad7c74ac3c61', 'dict-baddb067-318b-4b43-97d7-96f9a72596ba', 'dict-83e2faf0-84a1-41ea-8f3d-3c338bfd4116', 'dict-37b45c4e-c7f9-4f5c-857c-a66decfc6bd8', 'dict-9444a84e-2716-47b8-ae65-24146e2b3d85', 'dict-00e06aca-a9bf-47e9-89a3-fb5c1b824001', 'dict-17e14e47-e83d-4dca-8c86-d5b53afe329c', 'dict-dc0df99a-da83-4b2c-a997-27959ce256e6', 'dict-c64880fc-e60a-4d4d-a7c2-c5ba2c0e6d56', 'dict-340581a7-e814-48ae-aaba-e4c67582dc71', 'dict-1b7622ee-e1a1-403f-b159-031eb4ac9678', 'dict-e8d9bd8e-2cf3-404c-9ce7-0a1e1a58fcf0', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 42), 'dict-2a1ba546-0bb2-4f73-8c8c-e5add7379ca3', 'dict-fbf9bca3-dd8c-4ec4-ab48-774ed4a785dd', 'dict-a9bf70f7-2f69-48a0-8ac1-d4c5739380ef', 'dict-5dbcdebb-aef5-46f9-8d3f-999938a22daa', 'dict-f8fa6dc8-ce23-4e3c-9e46-28569c5a8896', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 5), 'dict-64cc23f8-7111-4eda-9c61-1028b68c0d14', 'dict-eee3d09c-e5a5-4af6-9467-0387c61bac2a', 'dict-c51ffac3-8a1b-4191-a6fc-2f8123f01e55', 'dict-3ea2e34b-e76c-45d1-98d7-bb10cd4b71aa', 'dict-4957582d-4a4d-47e5-8950-bda9ff4c1691', 'dict-3ea427fb-440d-4658-a85e-fed218f45f32', 'dict-b9e63afa-d76f-40e7-886d-28b49bb296c4', 'dict-19d58316-bb73-439f-bbd5-10a3355ff9ed', 'dict-ce363979-2d43-496a-abcc-7be71eb0c2cc', 'dict-f04bae79-bb2b-460f-8fe2-a165ffc49a55', 'dict-ea3e33be-180b-4bdd-80fa-cada1b87aa7d', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 35), 'dict-6636472b-4029-49f4-b62d-1467002bfe1b', 'dict-18760035-f334-422e-a0cb-43991d0cced2', 'dict-81c7e720-bfa9-4a36-9fbd-53ecc4c98b35', 'dict-7c5b1086-54bc-4576-85d8-97e77ec9b004', 'dict-6c183730-ec6c-4706-b436-06b7fa3d000e', 'dict-34477938-c6e6-47a8-aecd-a4b994c65920', 'dict-2ca24408-418f-4fbe-b16f-92fd1400f606', 'dict-d076ee20-10ef-4f83-8bb5-cb1217a1fbd2', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 28), 'dict-d787b72e-d631-4e57-842d-ea830d683078', 'dict-5bef0273-9986-4c68-aafd-06204125ceeb', 'dict-9fff9bcf-9595-4908-b592-717f059f8986', 'dict-9e4bc252-7c37-4845-94ad-25e07638058d', 'dict-bdc70e9e-102b-4420-ab14-277517c8bbd3', 'dict-d6c3cf47-3f55-4d68-928d-ecb1562f40de', 'dict-f1510ff7-4df1-4626-8090-7aec9a5fdeaa', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 64), 'dict-ec07e84a-88ba-4479-8398-0b36349f9c60', 'dict-c8df8745-74a5-4085-856f-8414e93f62b5', 'dict-44cf6464-879a-4b72-985d-a717db8efa6f', 'dict-337b5dcc-c726-4864-b24c-f652aa4c9531', 'dict-04d88346-eecd-4b18-be4a-2d2389205342', 'dict-f0531af8-5e54-451e-90b1-0773f3239776', 'dict-83fe1272-e86f-4028-9c83-1c2d677ba66e', 'dict-890ccd4e-0631-4eff-9374-bbe96686c13d', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 57), 'dict-f1f97f85-5d1c-4694-b8f9-ad13be7c36ed', 'dict-6ec8ebf9-06f9-4c12-8703-ebb3044b89ec', 'dict-680f1b3a-6ed2-4d4b-a455-4bbee130e0b8', 'dict-46cd71df-74fa-4c38-abad-a34577a45256', 'dict-3c8b09f6-cafd-427b-b3ae-913004cd9856', 'dict-8451c126-fba8-4a15-b2a2-a5ce888b9ce6', 'dict-d6fe7a93-d647-43f2-bec6-200d4e8f168e', 'dict-7d4e8e8d-3bb1-41c4-aa38-96477b34563e', 'dict-9a66b7c3-22f3-4083-8076-8e3b4e77b164', 'dict-5936a77e-d6e0-4c83-bd31-87a34533766c', 'dict-92a5cb0b-3e6c-4cb0-9923-9b0e7602a860', 'dict-eb27e498-9756-44d5-b2ee-065882410767', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 71), 'dict-8f6872e2-c44a-46b7-895a-6aafadef9504', 'dict-87ef4bce-cde8-4bfb-bd78-4357a655f29b', 'dict-47e583cc-b30a-4a68-8f1f-5d190ab743c5', 'dict-9ac18fc5-154c-4d3a-aa62-ab30fa76ad91', 'dict-a58c1b11-7bba-4614-b4b9-579850d5b614', 'dict-05771a39-6dbb-41d7-a9fe-a288087cf1f4', 'dict-1e83c365-a243-4a13-b5b3-6812f9c72a9f', 'dict-32c8f8ae-649b-447b-9b3f-0a2e7d09ba7b', 'dict-036ec976-60c8-40c1-9287-1ef47241f2a7', 'dict-786d95f7-aaac-4cce-ad81-2d4c25cb90de', 'dict-32060255-c56d-45c1-8703-4eae128183ae', 'dict-0ab8ce99-914b-4d79-aa4c-bbc65b08a413', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 20), 'dict-3b04d674-947a-4430-9f12-b7846c02e841', 'dict-9cec3225-7782-417b-ab85-2dfd798ce346', 'dict-12e7174f-21f4-4241-a0de-a5e5fcfeb188', 'dict-56caeb39-1ebc-4c20-8dda-8ba32eece8d8', 'dict-fe49d613-001c-439e-933f-45356d4b62fd', 'dict-cea67240-6b2d-4256-ab0e-b233997bcbdb', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 13), 'dict-9cf55e11-fc9f-431b-a631-cfe0ab52cad8', 'dict-30046751-cac9-45b4-a188-cd7311150228', 'dict-a0c22627-905b-4f5d-a07e-1cca77467a35'} (stimulus_id='handle-worker-cleanup-1713548591.0575428')
2024-04-19 17:43:11,119 - distributed.core - INFO - Connection to tcp://10.201.3.131:48422 has been closed.
2024-04-19 17:43:11,119 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.131:37489', status: running, memory: 64, processing: 11> (stimulus_id='handle-worker-cleanup-1713548591.11967')
2024-04-19 17:43:11,119 - distributed.scheduler - WARNING - Removing worker 'tcp://10.201.3.131:37489' caused the cluster to lose already computed task(s), which will be recomputed elsewhere: {'dict-83bda730-d85f-4cb2-9866-931b853bb484', 'dict-058f0907-eec5-43a5-9508-e77bbee836d7', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 3), 'dict-b620ba04-ca85-4464-8f06-9f8409281f9f', 'dict-75b50be9-4a3d-4697-b21d-d55a184dd52e', 'dict-03c2e093-9138-4d61-aa97-a9b4b2796eaa', 'dict-4e194181-cd77-4731-b4be-1cd81316a27c', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 9), ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 15), 'dict-e48e95da-c22d-47a6-88f9-59f01b21348b', 'dict-b33bc183-1f89-4ea3-bdd0-878ed277dcd4', 'dict-a2abb0c0-0060-4036-a7db-350bc57bc23d', 'dict-983f223f-218b-410c-ab6e-bcb2e85b8a93', 'dict-b65d8dd2-794e-48e4-b47c-3ed66e2c65eb', 'dict-4884a8d2-edc9-4f0f-a621-375201b7aed5', 'dict-03378c49-e011-490f-9dd9-3eaea6d64ec6', 'dict-d0d1bbda-b53f-49ec-92e9-6f67599488b3', 'dict-3556aacd-5a07-41ff-9c47-6c75ebbbfd95', 'dict-41370a52-0a82-47ad-a1ab-aba3e57588a5', 'dict-0e1c058e-8bd4-47e5-a48f-b4b07c6e00b3', 'dict-e9b1f9fc-765c-48ee-8a22-a4af6f152ff8', 'dict-1c85d30f-f333-4941-bd43-830c931ca5d9', 'dict-7a927119-2df5-4f15-aa42-1aa7bed60503', 'dict-7177c958-bc40-47d2-b7cf-6597b2c7b87d', 'dict-afa3c50f-1955-4cc0-a61c-df052c0869d4', 'dict-2fc910aa-f44e-4362-ba85-d316b9ba1e3c', 'dict-18730889-a799-4b2e-b9a8-623ebb7486a7', 'dict-6b32e6ae-8547-4f10-b9ec-56eac8c06090', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 51), 'dict-4c84b522-e1e1-4e9e-851b-68261fee378f', 'dict-3c383b03-cd0a-4a80-a3ad-2d627a68d825', 'dict-53a6ef5a-45db-4893-a927-64d6c3b3950e', 'dict-4b340067-01eb-4fdc-b6f3-94c9b30ee553', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 66), 'dict-27973112-8adc-4bd8-a52f-8d22d8b98cbf', 'dict-865f3990-9b77-4b7d-8944-6292bd8e8919', 'dict-fbefd756-b640-4447-81ea-29edbd1d9d17', 'dict-49298333-1889-4762-8a1c-008c743e39b3', 'dict-1757315e-603c-486d-8c3e-ab0bfb601411', 'dict-23c8cb3e-a75d-4f78-b04b-adbe1a7b94d2', 'dict-f8007426-8485-483e-bd35-f921446ec863', 'dict-f454d72b-d32c-4d51-bbb1-6f54cdad624d', 'dict-6b9331e6-2d4f-4260-bc82-1a5371005650', 'dict-3ebbd2c9-0728-4166-b1cd-c43f0575d5aa', 'dict-77eabbef-9baa-4070-b3e6-94393dd377b2', 'dict-880df11c-0d35-448e-ac9e-0d776c5a346e', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 44), 'dict-d44be59d-6a10-4f79-8118-2f01f32d1e08', 'dict-3d2f852d-de3b-47df-b0c7-c0d5930c62de', 'dict-f68c5c7a-e920-43d3-bedd-c223d794faf1', 'dict-6c54fd85-6541-4c10-87f1-643a8622dfe1', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 59), 'dict-e5c3db4b-f39f-402f-ac92-07efe7bbd286', 'dict-659211a0-8126-49a3-97d7-e8dfb1be1c4b', 'dict-e589c80c-e103-4297-8505-64019a7195c5', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 22), 'dict-669d425a-f83b-4703-a801-15208a751ca5', 'dict-467071fe-f9f3-46f3-8eb2-ba577a32af4f', 'dict-0dbb7c88-d8a5-4146-a608-36ea306ce43d', 'dict-8bc597cc-edf0-434d-9f36-e96f79d5bcfb', 'dict-d3f50ede-fb46-416a-9590-8b8febc781cb', 'dict-6bfd3a13-6620-4656-bcd1-0241976ff956', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 37), 'dict-78a535f7-fc6f-4223-b8ac-4c15d457d1d9'} (stimulus_id='handle-worker-cleanup-1713548591.11967')
2024-04-19 17:43:11,154 - distributed.core - INFO - Connection to tcp://10.201.3.131:48452 has been closed.
2024-04-19 17:43:11,154 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.131:46391', status: running, memory: 25, processing: 10> (stimulus_id='handle-worker-cleanup-1713548591.1542444')
2024-04-19 17:43:11,154 - distributed.scheduler - WARNING - Removing worker 'tcp://10.201.3.131:46391' caused the cluster to lose already computed task(s), which will be recomputed elsewhere: {('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 49), 'dict-d42236d1-749a-4b14-9c6d-942400b63656', 'dict-a230d6d7-4434-4343-8091-9f3c6e9fde6d', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 12), ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 70), 'dict-e89a3bdb-de8f-457d-8f83-cf03631a8028', 'dict-58f14412-4c67-4456-9ec0-348c16deaea2', 'dict-505ebf41-6bf3-4a2b-ba6b-276b0550bac1', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 27), ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 63), ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 2), 'dict-60ee702f-4229-44e8-ba5f-42a35a561801', 'dict-f9649362-c25a-4bff-a8bc-3b753e1031c0', 'dict-f9759171-7fe6-491a-aae6-583c6edffd3c', 'dict-95c5ee07-0aa0-4a08-b5e7-330077a7a7f8', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 41), 'dict-9a4cab49-23eb-4a76-a2e7-d51407967cfd', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 56), 'dict-7c8cc1dc-ec94-4277-8d88-228bbc584117', 'dict-4f79b8e8-32c0-48eb-b687-db6c18b8226b', 'dict-7d94957d-52e6-4fb8-bc6d-189b5a3d9920', 'dict-1f90e3b2-05c1-413a-8a38-57eb2bc30b01', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 34), 'dict-72f1af51-fde4-4126-bd84-75024cc1b6b1', 'dict-4b957e39-149c-4b72-8c01-4370b62bb5fb'} (stimulus_id='handle-worker-cleanup-1713548591.1542444')
2024-04-19 17:43:11,168 - distributed.core - INFO - Connection to tcp://10.201.3.131:48416 has been closed.
2024-04-19 17:43:11,168 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.3.131:41811', status: running, memory: 91, processing: 9> (stimulus_id='handle-worker-cleanup-1713548591.168521')
2024-04-19 17:43:11,168 - distributed.scheduler - WARNING - Removing worker 'tcp://10.201.3.131:41811' caused the cluster to lose already computed task(s), which will be recomputed elsewhere: {'dict-89a1b844-5b1b-4634-9c4c-85351f582c41', 'dict-c18add1a-d8e4-4178-a231-69f36eb19711', 'dict-53a879c1-6bf3-4ad6-97c1-fcfa949cbc9d', 'dict-89d17f17-bb80-4332-8935-81e277b17fad', 'dict-1c8c0e58-6135-430d-9fff-d633dbfc4ecb', 'dict-86451417-4efd-4b5a-b382-c56185516c2c', 'dict-f717d820-d31a-4868-a978-72b5f03424d6', 'dict-7db06f8a-0371-4398-ba2d-aed82b67f613', 'dict-62011495-c0b3-4e54-8418-18653f4b05eb', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 8), 'dict-11f1da9e-8c90-4d6a-9a4f-eaa9dd96076a', 'dict-0609aff3-0535-487f-afca-ff4d041fae34', 'dict-81c5ef18-0e67-4b60-9012-90033247b60e', 'dict-09f5a894-50b2-4060-8f79-25fcda2220c1', 'dict-478d86a8-2392-4d5c-9bec-ccbc1cd17953', 'dict-7c0e0e75-f2c0-4f77-805e-6851ae6035f3', 'dict-bafb604f-f4a6-4278-821b-0aa9114dcb91', 'dict-e803e223-143c-4c3b-9ffa-7a08de2cec1d', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 65), 'dict-3da80729-af13-4fae-b434-bfa68e0060da', 'dict-efed5125-8fb4-4bcf-96ad-e691bdd696f4', 'dict-6bfeac16-7c82-4870-b149-d11fdf98431f', 'dict-f67e0505-cdac-43bf-98c1-3ea55a1e07b6', 'dict-3c6868eb-c554-438a-9aea-b921a09797f9', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 58), 'dict-9ab86886-c7c6-4c6a-a252-eedd57640b29', 'dict-1a115a68-a318-4b70-b7db-3668dac6311c', 'dict-ba8e2b59-dd37-4c59-8091-94683726368d', 'dict-376816b1-84d2-4479-90d3-9525f349598a', 'dict-6939d3a5-0b38-4142-acd0-886143aaa3d1', 'dict-0bc0aa43-c31c-4e8d-b128-fee59a659d01', 'dict-355fc6ef-b770-4b87-84bc-b0bd2ae947a0', 'dict-5eab40cc-bc53-4e33-81ea-290fb57dc502', 'dict-95b44165-8fd1-435c-b549-06ea994d8e84', 'dict-0e78e721-69d0-4c00-8d46-6f6549d98368', 'dict-b5b31359-b80b-47d9-b3e8-1cce988e1119', 'dict-24f9a922-2aa8-4755-b372-fd0990dfed7b', 'dict-a47b1f18-fe6b-49ef-bbe9-2ee15b594824', 'dict-17c78100-2924-4f2d-ab43-9cc96914425d', 'dict-a757c467-b328-41b8-bc51-725301740cf1', 'dict-864a5a08-4f1b-4099-821e-ddd2d291d14a', 'dict-f2437d7b-c2f5-4825-a00e-7273c8eaca4b', 'dict-9951a3b7-8c3d-4697-b017-4901008757d4', 'dict-501d2398-bfa0-4ccb-b9ea-49bbbcb07659', 'dict-f57d3a22-d79b-427e-91b4-0450b4a8bdc1', 'dict-0cd611fd-1df9-49d2-b1ae-793b87a00831', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 21), 'dict-7942d2c9-3b64-488b-a7c7-1b7a466e922e', 'dict-74dfa452-0299-49da-b9cf-558803d37e92', 'dict-6a00ec27-d383-466d-8117-6df9903af2bb', 'dict-b08edb3e-a342-4765-b3d6-26e000f72638', 'dict-28a0ab28-7752-4df3-9a27-451ad4761b4b', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 14), 'dict-1cf81d3e-2b9d-4377-9766-3aa0aa431bdc', 'dict-8bf23595-4fbb-43b4-9ef4-39f3fb4fdc43', 'dict-01bf05db-280a-4614-adb0-5dbab738fcb7', 'dict-24d9a10a-e0fb-4c67-a74b-a6524e787f0a', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 50), 'dict-1e5cc934-47c5-4f09-a8ba-cdcb6a92174b', 'dict-d37ddbbb-ae7c-4589-96c7-1bd512e08e6f', 'dict-3b756700-abe1-4b1f-a65f-adb76613ebe2', 'dict-9e64d3b4-78ef-4678-b242-fbfafe4616bf', 'dict-9dc91606-c856-4987-b8ba-19cf274522d2', 'dict-de0c31c7-3a46-4c4d-90ab-a54c528e79a0', 'dict-f54766a9-f780-4549-86e8-58105c5cd5ac', 'dict-f47f5119-b8eb-4f92-af28-2ec9d13671a0', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 43), 'dict-1126486f-ec3f-4ee5-a70d-0d20c9344222', 'dict-529644ba-059c-4d5a-b0be-6afab5dcb783', 'dict-da4f7af7-8c7d-48a8-9967-2498f2e937d7', 'dict-c937dbfb-a212-44f9-8844-b03abf1adc31', 'dict-0d1718f5-a082-43c6-bc52-b60baa753556', 'dict-43711eb4-fa09-4c13-a64f-5d447e2a141e', 'dict-dee82643-685e-4983-a7c2-e6bd9b417706', 'dict-9df19bac-01e0-4264-a1ae-e42932eb9dde', 'dict-15b5fe19-f918-4e0d-9055-6ed0c62d8272', 'dict-c043f619-6cf2-4bda-b6f6-a18479bff749', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 36), 'dict-ee5a1a47-6cb1-4d6d-8ba3-11058b824393', 'dict-4f098a1b-af61-4792-bdbf-ebeee58e8cc1', 'dict-ccaa7ad5-7962-4338-87ae-4d5302d24479', 'dict-24c66c39-9340-43f0-adef-7cada701aaa4', ('_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9', 29), 'dict-701b088b-7b80-402f-af90-983a6c248b42', 'dict-5cae067b-fa7f-4a3d-b77a-f71a4f7571ed', 'dict-e27611e9-d7b6-47e3-a8de-b6b5dfc2488f', 'dict-2fa9ea43-3177-401f-93ca-06313e0674c0', 'dict-8f3ab03e-de8f-461d-8fc0-754eb5505a20', 'dict-87860d60-fff7-4b98-b695-9855eac76f02', 'dict-4b50859d-1993-4353-98d2-8b471409ced1', 'dict-d0e9bf6b-b884-4dad-bedf-f82ec13d6700'} (stimulus_id='handle-worker-cleanup-1713548591.168521')
2024-04-19 17:43:11,219 - distributed.scheduler - INFO - Lost all workers
Function returned HG_PERMISSION
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PERMISSION

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'drop_by_shallow_copy-0d3b31127f44bfccc1b6d00a2de9ae49\', 276)", \'thread\': None, \'worker\': None, \'prefix\': \'drop_by_shallow_copy\', \'group\': \'drop_by_shallow_copy-0d3b31127f44bfccc1b6d00a2de9ae49\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.823613}',)
Function returned HG_PERMISSION
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PERMISSION

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'drop_by_shallow_copy-0d3b31127f44bfccc1b6d00a2de9ae49\', 276)", \'thread\': None, \'worker\': None, \'prefix\': \'drop_by_shallow_copy\', \'group\': \'drop_by_shallow_copy-0d3b31127f44bfccc1b6d00a2de9ae49\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.8325295}',)
Function returned HG_PERMISSION
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PERMISSION

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'getitem-09bbd0256fb9f650d4bbbe5830555226\', 276)", \'thread\': None, \'worker\': None, \'prefix\': \'getitem\', \'group\': \'getitem-09bbd0256fb9f650d4bbbe5830555226\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.8340683}',)
Function returned HG_PERMISSION
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PERMISSION

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'getitem-09bbd0256fb9f650d4bbbe5830555226\', 276)", \'thread\': None, \'worker\': None, \'prefix\': \'getitem\', \'group\': \'getitem-09bbd0256fb9f650d4bbbe5830555226\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.8355649}',)
Function returned HG_PERMISSION
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PERMISSION

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'getitem-9a0eee1e327d09d8d766f2069a0b5242\', 276)", \'thread\': None, \'worker\': None, \'prefix\': \'getitem\', \'group\': \'getitem-9a0eee1e327d09d8d766f2069a0b5242\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.8370528}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'getitem-9a0eee1e327d09d8d766f2069a0b5242\', 276)", \'thread\': None, \'worker\': None, \'prefix\': \'getitem\', \'group\': \'getitem-9a0eee1e327d09d8d766f2069a0b5242\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.8385255}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'stackpartition-4c23c748e2a7408fc90d5cd59c0a2142\', 276)", \'thread\': None, \'worker\': None, \'prefix\': \'stackpartition\', \'group\': \'stackpartition-4c23c748e2a7408fc90d5cd59c0a2142\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.8399932}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'stackpartition-4c23c748e2a7408fc90d5cd59c0a2142\', 276)", \'thread\': None, \'worker\': None, \'prefix\': \'stackpartition\', \'group\': \'stackpartition-4c23c748e2a7408fc90d5cd59c0a2142\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.8414705}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'_random_split_take-a83ba47820a8197af5086ff61abf5198\', 60)", \'thread\': None, \'worker\': None, \'prefix\': \'random_split_take\', \'group\': \'_random_split_take-a83ba47820a8197af5086ff61abf5198\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.8429625}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'_random_split_take-a83ba47820a8197af5086ff61abf5198\', 60)", \'thread\': None, \'worker\': None, \'prefix\': \'random_split_take\', \'group\': \'_random_split_take-a83ba47820a8197af5086ff61abf5198\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.8444343}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'pd_split-dcdb8b56cff2088b81e53abcd3155611\', 60)", \'thread\': None, \'worker\': None, \'prefix\': \'pd_split\', \'group\': \'pd_split-dcdb8b56cff2088b81e53abcd3155611\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.8458822}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'pd_split-dcdb8b56cff2088b81e53abcd3155611\', 60)", \'thread\': None, \'worker\': None, \'prefix\': \'pd_split\', \'group\': \'pd_split-dcdb8b56cff2088b81e53abcd3155611\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.8473244}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'categorize_block-e2023cc84fe23e3143e2a898eda1daa5\', 60)", \'thread\': None, \'worker\': None, \'prefix\': \'categorize_block\', \'group\': \'categorize_block-e2023cc84fe23e3143e2a898eda1daa5\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.8488016}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'categorize_block-e2023cc84fe23e3143e2a898eda1daa5\', 60)", \'thread\': None, \'worker\': None, \'prefix\': \'categorize_block\', \'group\': \'categorize_block-e2023cc84fe23e3143e2a898eda1daa5\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.8502526}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9\', 60)", \'thread\': None, \'worker\': None, \'prefix\': \'categorize_block\', \'group\': \'_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.8516839}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9\', 60)", \'thread\': None, \'worker\': None, \'prefix\': \'categorize_block\', \'group\': \'_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.8531365}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'read_parquet-fused-assign-907d26dd53bbb860b233df22292df44d\', 60)", \'thread\': None, \'worker\': None, \'prefix\': \'read_parquet-fused-assign\', \'group\': \'read_parquet-fused-assign-907d26dd53bbb860b233df22292df44d\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.854574}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'read_parquet-fused-assign-907d26dd53bbb860b233df22292df44d\', 60)", \'thread\': None, \'worker\': None, \'prefix\': \'read_parquet-fused-assign\', \'group\': \'read_parquet-fused-assign-907d26dd53bbb860b233df22292df44d\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.8560264}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'read_parquet-fused-assign-b18e07d587eb97812f7fd58d5163ecd2\', 60)", \'thread\': None, \'worker\': None, \'prefix\': \'read_parquet-fused-assign\', \'group\': \'read_parquet-fused-assign-b18e07d587eb97812f7fd58d5163ecd2\', \'start\': \'queued\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.8574698}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'read_parquet-fused-assign-b18e07d587eb97812f7fd58d5163ecd2\', 60)", \'thread\': None, \'worker\': None, \'prefix\': \'read_parquet-fused-assign\', \'group\': \'read_parquet-fused-assign-b18e07d587eb97812f7fd58d5163ecd2\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.858913}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ("{'key': 'dict-fab4cd4f-90b3-4499-837b-316fab84446e', 'thread': None, 'worker': None, 'prefix': 'dict', 'group': 'dict', 'start': 'waiting', 'finish': 'released', 'stimulus_id': 'remove-client-1713548591.221806', 'called_from': 'tcp://10.201.3.51:8786', 'begins': None, 'ends': None, 'duration': None, 'size': None, 'time': 1713548591.860386}",)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ("{'key': 'dict-fab4cd4f-90b3-4499-837b-316fab84446e', 'thread': None, 'worker': None, 'prefix': 'dict', 'group': 'dict', 'start': 'released', 'finish': 'forgotten', 'stimulus_id': 'remove-client-1713548591.221806', 'called_from': 'tcp://10.201.3.51:8786', 'begins': None, 'ends': None, 'duration': None, 'size': None, 'time': 1713548591.8618395}",)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'drop_by_shallow_copy-92d867f651cbdbf13696d43796050432\', 51)", \'thread\': None, \'worker\': None, \'prefix\': \'drop_by_shallow_copy\', \'group\': \'drop_by_shallow_copy-92d867f651cbdbf13696d43796050432\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.8632557}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'drop_by_shallow_copy-92d867f651cbdbf13696d43796050432\', 51)", \'thread\': None, \'worker\': None, \'prefix\': \'drop_by_shallow_copy\', \'group\': \'drop_by_shallow_copy-92d867f651cbdbf13696d43796050432\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.8647108}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'drop_by_shallow_copy-0d3b31127f44bfccc1b6d00a2de9ae49\', 51)", \'thread\': None, \'worker\': None, \'prefix\': \'drop_by_shallow_copy\', \'group\': \'drop_by_shallow_copy-0d3b31127f44bfccc1b6d00a2de9ae49\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.8661492}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'drop_by_shallow_copy-0d3b31127f44bfccc1b6d00a2de9ae49\', 51)", \'thread\': None, \'worker\': None, \'prefix\': \'drop_by_shallow_copy\', \'group\': \'drop_by_shallow_copy-0d3b31127f44bfccc1b6d00a2de9ae49\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.8675883}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'getitem-09bbd0256fb9f650d4bbbe5830555226\', 51)", \'thread\': None, \'worker\': None, \'prefix\': \'getitem\', \'group\': \'getitem-09bbd0256fb9f650d4bbbe5830555226\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.8690548}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'getitem-09bbd0256fb9f650d4bbbe5830555226\', 51)", \'thread\': None, \'worker\': None, \'prefix\': \'getitem\', \'group\': \'getitem-09bbd0256fb9f650d4bbbe5830555226\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.8704915}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'getitem-9a0eee1e327d09d8d766f2069a0b5242\', 51)", \'thread\': None, \'worker\': None, \'prefix\': \'getitem\', \'group\': \'getitem-9a0eee1e327d09d8d766f2069a0b5242\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.8719227}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'getitem-9a0eee1e327d09d8d766f2069a0b5242\', 51)", \'thread\': None, \'worker\': None, \'prefix\': \'getitem\', \'group\': \'getitem-9a0eee1e327d09d8d766f2069a0b5242\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.8733683}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'stackpartition-4c23c748e2a7408fc90d5cd59c0a2142\', 51)", \'thread\': None, \'worker\': None, \'prefix\': \'stackpartition\', \'group\': \'stackpartition-4c23c748e2a7408fc90d5cd59c0a2142\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.8748026}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'stackpartition-4c23c748e2a7408fc90d5cd59c0a2142\', 51)", \'thread\': None, \'worker\': None, \'prefix\': \'stackpartition\', \'group\': \'stackpartition-4c23c748e2a7408fc90d5cd59c0a2142\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.8762686}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'_random_split_take-a7b4582ea981315477db3c48648c3b08\', 51)", \'thread\': None, \'worker\': None, \'prefix\': \'random_split_take\', \'group\': \'_random_split_take-a7b4582ea981315477db3c48648c3b08\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.8777099}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'_random_split_take-a7b4582ea981315477db3c48648c3b08\', 51)", \'thread\': None, \'worker\': None, \'prefix\': \'random_split_take\', \'group\': \'_random_split_take-a7b4582ea981315477db3c48648c3b08\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.8791611}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'pd_split-dcdb8b56cff2088b81e53abcd3155611\', 51)", \'thread\': None, \'worker\': None, \'prefix\': \'pd_split\', \'group\': \'pd_split-dcdb8b56cff2088b81e53abcd3155611\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.8806195}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'pd_split-dcdb8b56cff2088b81e53abcd3155611\', 51)", \'thread\': None, \'worker\': None, \'prefix\': \'pd_split\', \'group\': \'pd_split-dcdb8b56cff2088b81e53abcd3155611\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.8820746}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'categorize_block-e2023cc84fe23e3143e2a898eda1daa5\', 51)", \'thread\': None, \'worker\': None, \'prefix\': \'categorize_block\', \'group\': \'categorize_block-e2023cc84fe23e3143e2a898eda1daa5\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.8835356}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'categorize_block-e2023cc84fe23e3143e2a898eda1daa5\', 51)", \'thread\': None, \'worker\': None, \'prefix\': \'categorize_block\', \'group\': \'categorize_block-e2023cc84fe23e3143e2a898eda1daa5\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.8849878}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9\', 51)", \'thread\': None, \'worker\': None, \'prefix\': \'categorize_block\', \'group\': \'_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.886417}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9\', 51)", \'thread\': None, \'worker\': None, \'prefix\': \'categorize_block\', \'group\': \'_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.887851}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'read_parquet-fused-assign-907d26dd53bbb860b233df22292df44d\', 51)", \'thread\': None, \'worker\': None, \'prefix\': \'read_parquet-fused-assign\', \'group\': \'read_parquet-fused-assign-907d26dd53bbb860b233df22292df44d\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.8893347}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'read_parquet-fused-assign-907d26dd53bbb860b233df22292df44d\', 51)", \'thread\': None, \'worker\': None, \'prefix\': \'read_parquet-fused-assign\', \'group\': \'read_parquet-fused-assign-907d26dd53bbb860b233df22292df44d\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.8908007}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'read_parquet-fused-assign-b18e07d587eb97812f7fd58d5163ecd2\', 51)", \'thread\': None, \'worker\': None, \'prefix\': \'read_parquet-fused-assign\', \'group\': \'read_parquet-fused-assign-b18e07d587eb97812f7fd58d5163ecd2\', \'start\': \'queued\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.8922586}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'read_parquet-fused-assign-b18e07d587eb97812f7fd58d5163ecd2\', 51)", \'thread\': None, \'worker\': None, \'prefix\': \'read_parquet-fused-assign\', \'group\': \'read_parquet-fused-assign-b18e07d587eb97812f7fd58d5163ecd2\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.893696}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ("{'key': 'dict-2e73caa1-4086-4c62-87bf-513e7ce7edcb', 'thread': None, 'worker': None, 'prefix': 'dict', 'group': 'dict', 'start': 'waiting', 'finish': 'released', 'stimulus_id': 'remove-client-1713548591.221806', 'called_from': 'tcp://10.201.3.51:8786', 'begins': None, 'ends': None, 'duration': None, 'size': None, 'time': 1713548591.8951283}",)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ("{'key': 'dict-2e73caa1-4086-4c62-87bf-513e7ce7edcb', 'thread': None, 'worker': None, 'prefix': 'dict', 'group': 'dict', 'start': 'released', 'finish': 'forgotten', 'stimulus_id': 'remove-client-1713548591.221806', 'called_from': 'tcp://10.201.3.51:8786', 'begins': None, 'ends': None, 'duration': None, 'size': None, 'time': 1713548591.89659}",)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'drop_by_shallow_copy-92d867f651cbdbf13696d43796050432\', 250)", \'thread\': None, \'worker\': None, \'prefix\': \'drop_by_shallow_copy\', \'group\': \'drop_by_shallow_copy-92d867f651cbdbf13696d43796050432\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.8980265}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'drop_by_shallow_copy-92d867f651cbdbf13696d43796050432\', 250)", \'thread\': None, \'worker\': None, \'prefix\': \'drop_by_shallow_copy\', \'group\': \'drop_by_shallow_copy-92d867f651cbdbf13696d43796050432\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.8994598}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'drop_by_shallow_copy-0d3b31127f44bfccc1b6d00a2de9ae49\', 250)", \'thread\': None, \'worker\': None, \'prefix\': \'drop_by_shallow_copy\', \'group\': \'drop_by_shallow_copy-0d3b31127f44bfccc1b6d00a2de9ae49\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9008937}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'drop_by_shallow_copy-0d3b31127f44bfccc1b6d00a2de9ae49\', 250)", \'thread\': None, \'worker\': None, \'prefix\': \'drop_by_shallow_copy\', \'group\': \'drop_by_shallow_copy-0d3b31127f44bfccc1b6d00a2de9ae49\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9023218}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'getitem-09bbd0256fb9f650d4bbbe5830555226\', 250)", \'thread\': None, \'worker\': None, \'prefix\': \'getitem\', \'group\': \'getitem-09bbd0256fb9f650d4bbbe5830555226\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.903759}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'getitem-09bbd0256fb9f650d4bbbe5830555226\', 250)", \'thread\': None, \'worker\': None, \'prefix\': \'getitem\', \'group\': \'getitem-09bbd0256fb9f650d4bbbe5830555226\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9052107}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'getitem-9a0eee1e327d09d8d766f2069a0b5242\', 250)", \'thread\': None, \'worker\': None, \'prefix\': \'getitem\', \'group\': \'getitem-9a0eee1e327d09d8d766f2069a0b5242\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9066508}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'getitem-9a0eee1e327d09d8d766f2069a0b5242\', 250)", \'thread\': None, \'worker\': None, \'prefix\': \'getitem\', \'group\': \'getitem-9a0eee1e327d09d8d766f2069a0b5242\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9081028}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'stackpartition-4c23c748e2a7408fc90d5cd59c0a2142\', 250)", \'thread\': None, \'worker\': None, \'prefix\': \'stackpartition\', \'group\': \'stackpartition-4c23c748e2a7408fc90d5cd59c0a2142\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.909547}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'stackpartition-4c23c748e2a7408fc90d5cd59c0a2142\', 250)", \'thread\': None, \'worker\': None, \'prefix\': \'stackpartition\', \'group\': \'stackpartition-4c23c748e2a7408fc90d5cd59c0a2142\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9109888}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'_random_split_take-a83ba47820a8197af5086ff61abf5198\', 34)", \'thread\': None, \'worker\': None, \'prefix\': \'random_split_take\', \'group\': \'_random_split_take-a83ba47820a8197af5086ff61abf5198\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.912438}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'_random_split_take-a83ba47820a8197af5086ff61abf5198\', 34)", \'thread\': None, \'worker\': None, \'prefix\': \'random_split_take\', \'group\': \'_random_split_take-a83ba47820a8197af5086ff61abf5198\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9138799}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'pd_split-dcdb8b56cff2088b81e53abcd3155611\', 34)", \'thread\': None, \'worker\': None, \'prefix\': \'pd_split\', \'group\': \'pd_split-dcdb8b56cff2088b81e53abcd3155611\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.915311}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'pd_split-dcdb8b56cff2088b81e53abcd3155611\', 34)", \'thread\': None, \'worker\': None, \'prefix\': \'pd_split\', \'group\': \'pd_split-dcdb8b56cff2088b81e53abcd3155611\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.916763}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'categorize_block-e2023cc84fe23e3143e2a898eda1daa5\', 34)", \'thread\': None, \'worker\': None, \'prefix\': \'categorize_block\', \'group\': \'categorize_block-e2023cc84fe23e3143e2a898eda1daa5\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.918202}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'categorize_block-e2023cc84fe23e3143e2a898eda1daa5\', 34)", \'thread\': None, \'worker\': None, \'prefix\': \'categorize_block\', \'group\': \'categorize_block-e2023cc84fe23e3143e2a898eda1daa5\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9196582}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9\', 34)", \'thread\': None, \'worker\': None, \'prefix\': \'categorize_block\', \'group\': \'_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9210882}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9\', 34)", \'thread\': None, \'worker\': None, \'prefix\': \'categorize_block\', \'group\': \'_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9225187}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'read_parquet-fused-assign-907d26dd53bbb860b233df22292df44d\', 34)", \'thread\': None, \'worker\': None, \'prefix\': \'read_parquet-fused-assign\', \'group\': \'read_parquet-fused-assign-907d26dd53bbb860b233df22292df44d\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9239483}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'read_parquet-fused-assign-907d26dd53bbb860b233df22292df44d\', 34)", \'thread\': None, \'worker\': None, \'prefix\': \'read_parquet-fused-assign\', \'group\': \'read_parquet-fused-assign-907d26dd53bbb860b233df22292df44d\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.925388}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'read_parquet-fused-assign-b18e07d587eb97812f7fd58d5163ecd2\', 34)", \'thread\': None, \'worker\': None, \'prefix\': \'read_parquet-fused-assign\', \'group\': \'read_parquet-fused-assign-b18e07d587eb97812f7fd58d5163ecd2\', \'start\': \'queued\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.92682}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'read_parquet-fused-assign-b18e07d587eb97812f7fd58d5163ecd2\', 34)", \'thread\': None, \'worker\': None, \'prefix\': \'read_parquet-fused-assign\', \'group\': \'read_parquet-fused-assign-b18e07d587eb97812f7fd58d5163ecd2\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9282744}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ("{'key': 'dict-6b4cc507-a6c8-4a4d-8d9b-09f339efc7fc', 'thread': None, 'worker': None, 'prefix': 'dict', 'group': 'dict', 'start': 'waiting', 'finish': 'released', 'stimulus_id': 'remove-client-1713548591.221806', 'called_from': 'tcp://10.201.3.51:8786', 'begins': None, 'ends': None, 'duration': None, 'size': None, 'time': 1713548591.9297054}",)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ("{'key': 'dict-6b4cc507-a6c8-4a4d-8d9b-09f339efc7fc', 'thread': None, 'worker': None, 'prefix': 'dict', 'group': 'dict', 'start': 'released', 'finish': 'forgotten', 'stimulus_id': 'remove-client-1713548591.221806', 'called_from': 'tcp://10.201.3.51:8786', 'begins': None, 'ends': None, 'duration': None, 'size': None, 'time': 1713548591.93113}",)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'getitem-d8570c38365c6c06cb568d1a4d4a8648\', 201)", \'thread\': None, \'worker\': None, \'prefix\': \'getitem\', \'group\': \'getitem-d8570c38365c6c06cb568d1a4d4a8648\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9325747}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'getitem-d8570c38365c6c06cb568d1a4d4a8648\', 201)", \'thread\': None, \'worker\': None, \'prefix\': \'getitem\', \'group\': \'getitem-d8570c38365c6c06cb568d1a4d4a8648\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9340014}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'getitem-0b87010b2f2fd3d2696acd3fe39aa91c\', 201)", \'thread\': None, \'worker\': None, \'prefix\': \'getitem\', \'group\': \'getitem-0b87010b2f2fd3d2696acd3fe39aa91c\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9354148}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'getitem-0b87010b2f2fd3d2696acd3fe39aa91c\', 201)", \'thread\': None, \'worker\': None, \'prefix\': \'getitem\', \'group\': \'getitem-0b87010b2f2fd3d2696acd3fe39aa91c\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9368603}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'drop_by_shallow_copy-41795bcb8cb9d9837d90a3c0ff6f87da\', 201)", \'thread\': None, \'worker\': None, \'prefix\': \'drop_by_shallow_copy\', \'group\': \'drop_by_shallow_copy-41795bcb8cb9d9837d90a3c0ff6f87da\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.938287}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'drop_by_shallow_copy-41795bcb8cb9d9837d90a3c0ff6f87da\', 201)", \'thread\': None, \'worker\': None, \'prefix\': \'drop_by_shallow_copy\', \'group\': \'drop_by_shallow_copy-41795bcb8cb9d9837d90a3c0ff6f87da\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.939726}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'drop_by_shallow_copy-3b93c1cb54f062b9336b5d6de0f6b83c\', 201)", \'thread\': None, \'worker\': None, \'prefix\': \'drop_by_shallow_copy\', \'group\': \'drop_by_shallow_copy-3b93c1cb54f062b9336b5d6de0f6b83c\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9411714}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'drop_by_shallow_copy-3b93c1cb54f062b9336b5d6de0f6b83c\', 201)", \'thread\': None, \'worker\': None, \'prefix\': \'drop_by_shallow_copy\', \'group\': \'drop_by_shallow_copy-3b93c1cb54f062b9336b5d6de0f6b83c\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9425948}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'stackpartition-e1ea4d78924ca719b448282acdd806f8\', 201)", \'thread\': None, \'worker\': None, \'prefix\': \'stackpartition\', \'group\': \'stackpartition-e1ea4d78924ca719b448282acdd806f8\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9440174}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'stackpartition-e1ea4d78924ca719b448282acdd806f8\', 201)", \'thread\': None, \'worker\': None, \'prefix\': \'stackpartition\', \'group\': \'stackpartition-e1ea4d78924ca719b448282acdd806f8\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9454362}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'_random_split_take-f08281eaf48d5a937573c975746c87ee\', 57)", \'thread\': None, \'worker\': None, \'prefix\': \'random_split_take\', \'group\': \'_random_split_take-f08281eaf48d5a937573c975746c87ee\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9468539}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'_random_split_take-f08281eaf48d5a937573c975746c87ee\', 57)", \'thread\': None, \'worker\': None, \'prefix\': \'random_split_take\', \'group\': \'_random_split_take-f08281eaf48d5a937573c975746c87ee\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9482813}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'pd_split-dcdb8b56cff2088b81e53abcd3155611\', 57)", \'thread\': None, \'worker\': None, \'prefix\': \'pd_split\', \'group\': \'pd_split-dcdb8b56cff2088b81e53abcd3155611\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9497044}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'pd_split-dcdb8b56cff2088b81e53abcd3155611\', 57)", \'thread\': None, \'worker\': None, \'prefix\': \'pd_split\', \'group\': \'pd_split-dcdb8b56cff2088b81e53abcd3155611\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9511247}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'categorize_block-e2023cc84fe23e3143e2a898eda1daa5\', 57)", \'thread\': None, \'worker\': None, \'prefix\': \'categorize_block\', \'group\': \'categorize_block-e2023cc84fe23e3143e2a898eda1daa5\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9525604}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'categorize_block-e2023cc84fe23e3143e2a898eda1daa5\', 57)", \'thread\': None, \'worker\': None, \'prefix\': \'categorize_block\', \'group\': \'categorize_block-e2023cc84fe23e3143e2a898eda1daa5\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9540024}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9\', 57)", \'thread\': None, \'worker\': None, \'prefix\': \'categorize_block\', \'group\': \'_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.955412}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9\', 57)", \'thread\': None, \'worker\': None, \'prefix\': \'categorize_block\', \'group\': \'_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9568512}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'read_parquet-fused-assign-907d26dd53bbb860b233df22292df44d\', 57)", \'thread\': None, \'worker\': None, \'prefix\': \'read_parquet-fused-assign\', \'group\': \'read_parquet-fused-assign-907d26dd53bbb860b233df22292df44d\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9582763}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'read_parquet-fused-assign-907d26dd53bbb860b233df22292df44d\', 57)", \'thread\': None, \'worker\': None, \'prefix\': \'read_parquet-fused-assign\', \'group\': \'read_parquet-fused-assign-907d26dd53bbb860b233df22292df44d\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9597304}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'read_parquet-fused-assign-b18e07d587eb97812f7fd58d5163ecd2\', 57)", \'thread\': None, \'worker\': None, \'prefix\': \'read_parquet-fused-assign\', \'group\': \'read_parquet-fused-assign-b18e07d587eb97812f7fd58d5163ecd2\', \'start\': \'queued\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9611871}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'read_parquet-fused-assign-b18e07d587eb97812f7fd58d5163ecd2\', 57)", \'thread\': None, \'worker\': None, \'prefix\': \'read_parquet-fused-assign\', \'group\': \'read_parquet-fused-assign-b18e07d587eb97812f7fd58d5163ecd2\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9626186}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ("{'key': 'dict-dde1d2f6-c1c5-4d07-b72e-859d62539e35', 'thread': None, 'worker': None, 'prefix': 'dict', 'group': 'dict', 'start': 'waiting', 'finish': 'released', 'stimulus_id': 'remove-client-1713548591.221806', 'called_from': 'tcp://10.201.3.51:8786', 'begins': None, 'ends': None, 'duration': None, 'size': None, 'time': 1713548591.9640515}",)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ("{'key': 'dict-dde1d2f6-c1c5-4d07-b72e-859d62539e35', 'thread': None, 'worker': None, 'prefix': 'dict', 'group': 'dict', 'start': 'released', 'finish': 'forgotten', 'stimulus_id': 'remove-client-1713548591.221806', 'called_from': 'tcp://10.201.3.51:8786', 'begins': None, 'ends': None, 'duration': None, 'size': None, 'time': 1713548591.9654818}",)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'drop_by_shallow_copy-41795bcb8cb9d9837d90a3c0ff6f87da\', 68)", \'thread\': None, \'worker\': None, \'prefix\': \'drop_by_shallow_copy\', \'group\': \'drop_by_shallow_copy-41795bcb8cb9d9837d90a3c0ff6f87da\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9669034}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'drop_by_shallow_copy-41795bcb8cb9d9837d90a3c0ff6f87da\', 68)", \'thread\': None, \'worker\': None, \'prefix\': \'drop_by_shallow_copy\', \'group\': \'drop_by_shallow_copy-41795bcb8cb9d9837d90a3c0ff6f87da\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9683483}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'drop_by_shallow_copy-3b93c1cb54f062b9336b5d6de0f6b83c\', 68)", \'thread\': None, \'worker\': None, \'prefix\': \'drop_by_shallow_copy\', \'group\': \'drop_by_shallow_copy-3b93c1cb54f062b9336b5d6de0f6b83c\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9697976}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'drop_by_shallow_copy-3b93c1cb54f062b9336b5d6de0f6b83c\', 68)", \'thread\': None, \'worker\': None, \'prefix\': \'drop_by_shallow_copy\', \'group\': \'drop_by_shallow_copy-3b93c1cb54f062b9336b5d6de0f6b83c\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.971224}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'getitem-d8570c38365c6c06cb568d1a4d4a8648\', 68)", \'thread\': None, \'worker\': None, \'prefix\': \'getitem\', \'group\': \'getitem-d8570c38365c6c06cb568d1a4d4a8648\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9726481}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'getitem-d8570c38365c6c06cb568d1a4d4a8648\', 68)", \'thread\': None, \'worker\': None, \'prefix\': \'getitem\', \'group\': \'getitem-d8570c38365c6c06cb568d1a4d4a8648\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.974084}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'getitem-0b87010b2f2fd3d2696acd3fe39aa91c\', 68)", \'thread\': None, \'worker\': None, \'prefix\': \'getitem\', \'group\': \'getitem-0b87010b2f2fd3d2696acd3fe39aa91c\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9754968}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'getitem-0b87010b2f2fd3d2696acd3fe39aa91c\', 68)", \'thread\': None, \'worker\': None, \'prefix\': \'getitem\', \'group\': \'getitem-0b87010b2f2fd3d2696acd3fe39aa91c\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9769237}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'stackpartition-e1ea4d78924ca719b448282acdd806f8\', 68)", \'thread\': None, \'worker\': None, \'prefix\': \'stackpartition\', \'group\': \'stackpartition-e1ea4d78924ca719b448282acdd806f8\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9783478}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'stackpartition-e1ea4d78924ca719b448282acdd806f8\', 68)", \'thread\': None, \'worker\': None, \'prefix\': \'stackpartition\', \'group\': \'stackpartition-e1ea4d78924ca719b448282acdd806f8\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9797673}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'_random_split_take-a7b4582ea981315477db3c48648c3b08\', 68)", \'thread\': None, \'worker\': None, \'prefix\': \'random_split_take\', \'group\': \'_random_split_take-a7b4582ea981315477db3c48648c3b08\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9812117}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'_random_split_take-a7b4582ea981315477db3c48648c3b08\', 68)", \'thread\': None, \'worker\': None, \'prefix\': \'random_split_take\', \'group\': \'_random_split_take-a7b4582ea981315477db3c48648c3b08\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9826415}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'pd_split-dcdb8b56cff2088b81e53abcd3155611\', 68)", \'thread\': None, \'worker\': None, \'prefix\': \'pd_split\', \'group\': \'pd_split-dcdb8b56cff2088b81e53abcd3155611\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9840746}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'pd_split-dcdb8b56cff2088b81e53abcd3155611\', 68)", \'thread\': None, \'worker\': None, \'prefix\': \'pd_split\', \'group\': \'pd_split-dcdb8b56cff2088b81e53abcd3155611\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9855049}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'categorize_block-e2023cc84fe23e3143e2a898eda1daa5\', 68)", \'thread\': None, \'worker\': None, \'prefix\': \'categorize_block\', \'group\': \'categorize_block-e2023cc84fe23e3143e2a898eda1daa5\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.986928}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'categorize_block-e2023cc84fe23e3143e2a898eda1daa5\', 68)", \'thread\': None, \'worker\': None, \'prefix\': \'categorize_block\', \'group\': \'categorize_block-e2023cc84fe23e3143e2a898eda1daa5\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9883707}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9\', 68)", \'thread\': None, \'worker\': None, \'prefix\': \'categorize_block\', \'group\': \'_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.989795}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9\', 68)", \'thread\': None, \'worker\': None, \'prefix\': \'categorize_block\', \'group\': \'_categorize_block-31f4ab7da802d19db0ddea7f8ec20ce9\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.991222}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'read_parquet-fused-assign-907d26dd53bbb860b233df22292df44d\', 68)", \'thread\': None, \'worker\': None, \'prefix\': \'read_parquet-fused-assign\', \'group\': \'read_parquet-fused-assign-907d26dd53bbb860b233df22292df44d\', \'start\': \'waiting\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9926715}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'read_parquet-fused-assign-907d26dd53bbb860b233df22292df44d\', 68)", \'thread\': None, \'worker\': None, \'prefix\': \'read_parquet-fused-assign\', \'group\': \'read_parquet-fused-assign-907d26dd53bbb860b233df22292df44d\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9941013}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 1964, in _transition
    a_recs, a_cmsgs, a_wmsgs = self._transition(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'read_parquet-fused-assign-b18e07d587eb97812f7fd58d5163ecd2\', 68)", \'thread\': None, \'worker\': None, \'prefix\': \'read_parquet-fused-assign\', \'group\': \'read_parquet-fused-assign-b18e07d587eb97812f7fd58d5163ecd2\', \'start\': \'queued\', \'finish\': \'released\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.995526}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 229, in transition
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5741, in remove_client
    self.client_releases_keys(
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5430, in client_releases_keys
    self.transitions(recommendations, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 7984, in transitions
    self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2065, in _transitions
    new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 2022, in _transition
    plugin.transition(
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 231, in transition
    logging.exception("Exception while calling transition method when sending", str(transition_data))
Message: 'Exception while calling transition method when sending'
Arguments: ('{\'key\': "(\'read_parquet-fused-assign-b18e07d587eb97812f7fd58d5163ecd2\', 68)", \'thread\': None, \'worker\': None, \'prefix\': \'read_parquet-fused-assign\', \'group\': \'read_parquet-fused-assign-b18e07d587eb97812f7fd58d5163ecd2\', \'start\': \'released\', \'finish\': \'forgotten\', \'stimulus_id\': \'remove-client-1713548591.221806\', \'called_from\': \'tcp://10.201.3.51:8786\', \'begins\': None, \'ends\': None, \'duration\': None, \'size\': None, \'time\': 1713548591.9969761}',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 298, in remove_client
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5750, in remove_client
    plugin.remove_client(scheduler=self, client=client)
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 300, in remove_client
    logging.exception("Exception while calling remove client method when sending", str(rm_client))
Message: 'Exception while calling remove client method when sending'
Arguments: ('b"{\'client\': \'Client-8c1c0252-fe70-11ee-8310-6805cac57d4a\', \'action\': \'remove\', \'time\': 1713548591.9984307}"',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 298, in remove_client
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5750, in remove_client
    plugin.remove_client(scheduler=self, client=client)
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 300, in remove_client
    logging.exception("Exception while calling remove client method when sending", str(rm_client))
Message: 'Exception while calling remove client method when sending'
Arguments: ('b"{\'client\': \'Client-worker-0f11bc01-fe72-11ee-93f0-6805cad942a0\', \'action\': \'remove\', \'time\': 1713548592.0000272}"',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 298, in remove_client
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5750, in remove_client
    plugin.remove_client(scheduler=self, client=client)
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 300, in remove_client
    logging.exception("Exception while calling remove client method when sending", str(rm_client))
Message: 'Exception while calling remove client method when sending'
Arguments: ('b"{\'client\': \'Client-worker-0f127ddf-fe72-11ee-93ee-6805cad942a0\', \'action\': \'remove\', \'time\': 1713548592.0015225}"',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 298, in remove_client
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5750, in remove_client
    plugin.remove_client(scheduler=self, client=client)
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 300, in remove_client
    logging.exception("Exception while calling remove client method when sending", str(rm_client))
Message: 'Exception while calling remove client method when sending'
Arguments: ('b"{\'client\': \'Client-worker-0f127b23-fe72-11ee-b770-6805cacc0ca2\', \'action\': \'remove\', \'time\': 1713548592.0030122}"',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 298, in remove_client
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5750, in remove_client
    plugin.remove_client(scheduler=self, client=client)
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 300, in remove_client
    logging.exception("Exception while calling remove client method when sending", str(rm_client))
Message: 'Exception while calling remove client method when sending'
Arguments: ('b"{\'client\': \'Client-worker-0f127c29-fe72-11ee-b772-6805cacc0ca2\', \'action\': \'remove\', \'time\': 1713548592.0045073}"',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 298, in remove_client
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5750, in remove_client
    plugin.remove_client(scheduler=self, client=client)
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 300, in remove_client
    logging.exception("Exception while calling remove client method when sending", str(rm_client))
Message: 'Exception while calling remove client method when sending'
Arguments: ('b"{\'client\': \'Client-worker-0f127fab-fe72-11ee-93f1-6805cad942a0\', \'action\': \'remove\', \'time\': 1713548592.0059907}"',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 298, in remove_client
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5750, in remove_client
    plugin.remove_client(scheduler=self, client=client)
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 300, in remove_client
    logging.exception("Exception while calling remove client method when sending", str(rm_client))
Message: 'Exception while calling remove client method when sending'
Arguments: ('b"{\'client\': \'Client-worker-0f1282aa-fe72-11ee-b773-6805cacc0ca2\', \'action\': \'remove\', \'time\': 1713548592.0074613}"',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 298, in remove_client
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5750, in remove_client
    plugin.remove_client(scheduler=self, client=client)
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 300, in remove_client
    logging.exception("Exception while calling remove client method when sending", str(rm_client))
Message: 'Exception while calling remove client method when sending'
Arguments: ('b"{\'client\': \'Client-worker-0f128650-fe72-11ee-93ef-6805cad942a0\', \'action\': \'remove\', \'time\': 1713548592.0089462}"',)
Function returned HG_PROTOCOL_ERROR
--- Logging error ---
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5713, in add_client
    await self.handle_stream(comm=comm, extra={"client": client})
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 1025, in handle_stream
    msgs = await comm.read()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 225, in read
    frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 298, in remove_client
    f.wait()
pymofka_client.Exception: Unexpected error when sending batch: [/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/mochi-thallium-0.12.0-ow3bjypntyfbafke33olmxpdg46qgnpp/include/thallium/callable_remote_procedure.hpp:117][margo_provider_forward] Function returned HG_PROTOCOL_ERROR

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 1100, in emit
    msg = self.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 943, in format
    return fmt.format(record)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 678, in format
    record.message = record.getMessage()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/logging/__init__.py", line 368, in getMessage
    msg = msg % self.args
TypeError: not all arguments converted during string formatting
Call stack:
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask", line 8, in <module>
    sys.exit(main())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/__main__.py", line 7, in main
    run_cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/dask/cli.py", line 209, in run_cli
    cli()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/cli/dask_scheduler.py", line 251, in main
    asyncio_run(run(), loop_factory=get_loop_factory())
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/compatibility.py", line 236, in asyncio_run
    return loop.run_until_complete(main)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/base_events.py", line 1909, in _run_once
    handle._run()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/lib/python3.10/asyncio/events.py", line 80, in _run
    self._context.run(self._callback, *self._args)
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/core.py", line 970, in _handle_comm
    result = await result
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5715, in add_client
    self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/scheduler.py", line 5750, in remove_client
    plugin.remove_client(scheduler=self, client=client)
  File "/lus/eagle/projects/radix-io/agueroudji/XGBoost/D2024-04-19_17:14:59_R42_W8/MofkaSchedulerPlugin.py", line 300, in remove_client
    logging.exception("Exception while calling remove client method when sending", str(rm_client))
Message: 'Exception while calling remove client method when sending'
Arguments: ('b"{\'client\': \'Client-worker-0f13304c-fe72-11ee-b771-6805cacc0ca2\', \'action\': \'remove\', \'time\': 1713548592.0104325}"',)
2024-04-19 17:43:12,012 - distributed.batched - INFO - Batched Comm Closed <TCP (closed) Scheduler->Client local=tcp://10.201.3.51:8786 remote=tcp://10.201.3.50:41706>
Traceback (most recent call last):
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/batched.py", line 115, in _background_send
    nbytes = yield coro
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/tornado/gen.py", line 767, in run
    value = future.result()
  File "/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/comm/tcp.py", line 262, in write
    raise CommClosedError()
distributed.comm.core.CommClosedError
2024-04-19 17:43:12,018 - distributed.scheduler - INFO - Stopped scheduler at 'tcp://10.201.3.51:8786'
2024-04-19 17:43:12,018 - distributed.scheduler - INFO - End scheduler
