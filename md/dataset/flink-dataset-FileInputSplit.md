# Flink1.7.2  Dataset 文件切片计算方式和切片数据读取源码分析

## 源码
- https://github.com/opensourceteams/flink-maven-scala

## 概述
- 了解读取的文件或目录，具体进行切片拆分的实现
- 了解任务读取切片中的数据规则

## 数据文件读取结论
### 开始位置索引从0开始的
- 实际开始位置，0
- 结束位置:按行一直读，直到位置索引大于等于切片大小时，再读下一个切片的1m数据，由于此时当前切片数据已全部读完了，所以就overLimit=true,但是也会读取下一个切片的一行数据

### 开始位置索引从大于0开始的
- 实际开始位置，由切片分到的位置开始算，找到第一个换行符的位置 +1开始计算
- 结束位置,当读到的位置索引，大于等于切片数据大小时，说明本切片已读完，如果下一个切片还有数据，就从下一个切片读到第一个换行符的数据，如果没有下一个切片，就到当前读到的位置结束

## 图解
- https://github.com/opensourceteams/flink-maven-scala/blob/master/md/images/wordCount/dataset/%E5%88%87%E5%88%86%E6%95%B0%E6%8D%AE%E6%A1%88%E4%BE%8B%E4%B8%89.png
- https://github.com/opensourceteams/flink-maven-scala/blob/master/md/images/wordCount/dataset/%E5%88%87%E5%88%86%E6%95%B0%E6%8D%AE%E6%A1%88%E4%BE%8B%E4%B8%80.png
- https://github.com/opensourceteams/flink-maven-scala/blob/master/md/images/wordCount/dataset/%E5%88%87%E5%AE%89%E6%95%B0%E6%8D%AE%E6%A1%88%E4%BE%8B%E4%BA%8C.png

- ![](https://github.com/opensourceteams/flink-maven-scala/blob/master/md/images/wordCount/dataset/%E5%88%87%E5%88%86%E6%95%B0%E6%8D%AE%E6%A1%88%E4%BE%8B%E4%B8%89.png)
- ![](https://github.com/opensourceteams/flink-maven-scala/blob/master/md/images/wordCount/dataset/%E5%88%87%E5%88%86%E6%95%B0%E6%8D%AE%E6%A1%88%E4%BE%8B%E4%B8%80.png)
- ![](https://github.com/opensourceteams/flink-maven-scala/blob/master/md/images/wordCount/dataset/%E5%88%87%E5%AE%89%E6%95%B0%E6%8D%AE%E6%A1%88%E4%BE%8B%E4%BA%8C.png)
## 输入数据
- 注意空格，第一行6个byte,第二行3个byte,(一共9个byte的数据,9个byte中包括一个byte的换行符)
```
c a a
b c
```
-转志Integer
```
99 32 97 32 97 32 10
98 32 99
```

## WordCount.scala
- java的也不影响分析，只是 WordCount.scala写的方式不一样，整个过程，逻辑是一样的

```
package com.opensourceteams.module.bigdata.flink.example.dataset.worldcount

import com.opensourceteams.module.bigdata.flink.common.ConfigurationUtil
import org.apache.flink.api.scala.ExecutionEnvironment


/**
  * 批处理，DataSet WordCount分析
  */
object WordCountRun {


  def main(args: Array[String]): Unit = {

    //调试设置超时问题
    val env : ExecutionEnvironment= ExecutionEnvironment.createLocalEnvironment(ConfigurationUtil.getConfiguration(true))
    env.setParallelism(2)

    val dataSet = env.readTextFile("file:/opt/n_001_workspaces/bigdata/flink/flink-maven-scala-2/src/main/resources/data/line.txt")


    import org.apache.flink.streaming.api.scala._
    val result = dataSet.flatMap(x => x.split(" ")).map((_,1)).groupBy(0).sum(1)



    result.print()




  }

}

```
## 源码分析(文件拆分成切片)
- 预拆分数据，之所以叫做预，就不是实际的，实际读取时，会考虑更多因素，会有一定变化，下面有详细说明
- 把文件按并行度拆分成FileInputSplit的个数，当然并不是完全有几个并行度就生成几个FileInputSplit对象，根据具体算法得到，但是FileInputSplit个数，一定是(并行度个数，或者并行度个数+1),因为计算FileInputSplit个数时，参照物是文件大小 / 并行度 ，如果没有余数，刚好整除，那么FileInputSplit个数一定是并行度，如果有余数，FileInputSplit个数就为是(并行度个数，或者并行度个数+1)
- 本示例拆分的结果
    
    ```
    [0] file:/opt/n_001_workspaces/bigdata/flink/flink-maven-scala-2/src/main/resources/data/line.txt:0+5
    [1] file:/opt/n_001_workspaces/bigdata/flink/flink-maven-scala-2/src/main/resources/data/line.txt:5+4
    ```

### ExecutionGraphBuilder.buildGraph
- JobMaster在实例化时，构建ExecutionGraph,会调用 ExecutionGraphBuilder.buildGraph(jobGraph) 
- 把jobGraph是由JobVertex组成，调用executionGraph.attachJobGraph(sortedTopology) 把JobGraph转成ExecutionGraph,ExecutionGraph由ExecutionJobVertex组成，即把JobVertex转成ExecutionJobVertex

    ```
    executionGraph.attachJobGraph(sortedTopology);
    ```
    
    ```
    sortedTopology = {ArrayList@5177}  size = 3
 0 = {InputFormatVertex@5459} "CHAIN DataSource (at com.opensourceteams.module.bigdata.flink.example.dataset.worldcount.WordCountRun$.main(WordCountRun.scala:19) (org.apache.flink.api.java.io.TextInp) -> FlatMap (FlatMap at com.opensourceteams.module.bigdata.flink.example.dataset.worldcount.WordCountRun$.main(WordCountRun.scala:23)) -> Map (Map at com.opensourceteams.module.bigdata.flink.example.dataset.worldcount.WordCountRun$.main(WordCountRun.scala:23)) -> Combine (SUM(1)) (org.apache.flink.runtime.operators.DataSourceTask)"
 1 = {JobVertex@5460} "Reduce (SUM(1)) (org.apache.flink.runtime.operators.BatchTask)"
 2 = {OutputFormatVertex@5461} "DataSink (collect()) (org.apache.flink.runtime.operators.DataSinkTask)"
    ```
- 调用ExecutionGraph.attachJobGraph


```
/**
	 * Builds the ExecutionGraph from the JobGraph.
	 * If a prior execution graph exists, the JobGraph will be attached. If no prior execution
	 * graph exists, then the JobGraph will become attach to a new empty execution graph.
	 */
	@Deprecated
	public static ExecutionGraph buildGraph(
			@Nullable ExecutionGraph prior,
			JobGraph jobGraph,
			Configuration jobManagerConfig,
			ScheduledExecutorService futureExecutor,
			Executor ioExecutor,
			SlotProvider slotProvider,
			ClassLoader classLoader,
			CheckpointRecoveryFactory recoveryFactory,
			Time rpcTimeout,
			RestartStrategy restartStrategy,
			MetricGroup metrics,
			int parallelismForAutoMax,
			BlobWriter blobWriter,
			Time allocationTimeout,
			Logger log)
		throws JobExecutionException, JobException {

		checkNotNull(jobGraph, "job graph cannot be null");

		final String jobName = jobGraph.getName();
		final JobID jobId = jobGraph.getJobID();

		final FailoverStrategy.Factory failoverStrategy =
				FailoverStrategyLoader.loadFailoverStrategy(jobManagerConfig, log);

		final JobInformation jobInformation = new JobInformation(
			jobId,
			jobName,
			jobGraph.getSerializedExecutionConfig(),
			jobGraph.getJobConfiguration(),
			jobGraph.getUserJarBlobKeys(),
			jobGraph.getClasspaths());

		// create a new execution graph, if none exists so far
		final ExecutionGraph executionGraph;
		try {
			executionGraph = (prior != null) ? prior :
				new ExecutionGraph(
					jobInformation,
					futureExecutor,
					ioExecutor,
					rpcTimeout,
					restartStrategy,
					failoverStrategy,
					slotProvider,
					classLoader,
					blobWriter,
					allocationTimeout);
		} catch (IOException e) {
			throw new JobException("Could not create the ExecutionGraph.", e);
		}

		// set the basic properties

		executionGraph.setScheduleMode(jobGraph.getScheduleMode());
		executionGraph.setQueuedSchedulingAllowed(jobGraph.getAllowQueuedScheduling());

		try {
			executionGraph.setJsonPlan(JsonPlanGenerator.generatePlan(jobGraph));
		}
		catch (Throwable t) {
			log.warn("Cannot create JSON plan for job", t);
			// give the graph an empty plan
			executionGraph.setJsonPlan("{}");
		}

		// initialize the vertices that have a master initialization hook
		// file output formats create directories here, input formats create splits

		final long initMasterStart = System.nanoTime();
		log.info("Running initialization on master for job {} ({}).", jobName, jobId);

		for (JobVertex vertex : jobGraph.getVertices()) {
			String executableClass = vertex.getInvokableClassName();
			if (executableClass == null || executableClass.isEmpty()) {
				throw new JobSubmissionException(jobId,
						"The vertex " + vertex.getID() + " (" + vertex.getName() + ") has no invokable class.");
			}

			if (vertex.getParallelism() == ExecutionConfig.PARALLELISM_AUTO_MAX) {
				if (parallelismForAutoMax < 0) {
					throw new JobSubmissionException(
						jobId,
						PARALLELISM_AUTO_MAX_ERROR_MESSAGE);
				}
				else {
					vertex.setParallelism(parallelismForAutoMax);
				}
			}

			try {
				vertex.initializeOnMaster(classLoader);
			}
			catch (Throwable t) {
					throw new JobExecutionException(jobId,
							"Cannot initialize task '" + vertex.getName() + "': " + t.getMessage(), t);
			}
		}

		log.info("Successfully ran initialization on master in {} ms.",
				(System.nanoTime() - initMasterStart) / 1_000_000);

		// topologically sort the job vertices and attach the graph to the existing one
		List<JobVertex> sortedTopology = jobGraph.getVerticesSortedTopologicallyFromSources();
		if (log.isDebugEnabled()) {
			log.debug("Adding {} vertices from job graph {} ({}).", sortedTopology.size(), jobName, jobId);
		}
		executionGraph.attachJobGraph(sortedTopology);

		if (log.isDebugEnabled()) {
			log.debug("Successfully created execution graph from job graph {} ({}).", jobName, jobId);
		}

		// configure the state checkpointing
		JobCheckpointingSettings snapshotSettings = jobGraph.getCheckpointingSettings();
		if (snapshotSettings != null) {
			List<ExecutionJobVertex> triggerVertices =
					idToVertex(snapshotSettings.getVerticesToTrigger(), executionGraph);

			List<ExecutionJobVertex> ackVertices =
					idToVertex(snapshotSettings.getVerticesToAcknowledge(), executionGraph);

			List<ExecutionJobVertex> confirmVertices =
					idToVertex(snapshotSettings.getVerticesToConfirm(), executionGraph);

			CompletedCheckpointStore completedCheckpoints;
			CheckpointIDCounter checkpointIdCounter;
			try {
				int maxNumberOfCheckpointsToRetain = jobManagerConfig.getInteger(
						CheckpointingOptions.MAX_RETAINED_CHECKPOINTS);

				if (maxNumberOfCheckpointsToRetain <= 0) {
					// warning and use 1 as the default value if the setting in
					// state.checkpoints.max-retained-checkpoints is not greater than 0.
					log.warn("The setting for '{} : {}' is invalid. Using default value of {}",
							CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.key(),
							maxNumberOfCheckpointsToRetain,
							CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.defaultValue());

					maxNumberOfCheckpointsToRetain = CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.defaultValue();
				}

				completedCheckpoints = recoveryFactory.createCheckpointStore(jobId, maxNumberOfCheckpointsToRetain, classLoader);
				checkpointIdCounter = recoveryFactory.createCheckpointIDCounter(jobId);
			}
			catch (Exception e) {
				throw new JobExecutionException(jobId, "Failed to initialize high-availability checkpoint handler", e);
			}

			// Maximum number of remembered checkpoints
			int historySize = jobManagerConfig.getInteger(WebOptions.CHECKPOINTS_HISTORY_SIZE);

			CheckpointStatsTracker checkpointStatsTracker = new CheckpointStatsTracker(
					historySize,
					ackVertices,
					snapshotSettings.getCheckpointCoordinatorConfiguration(),
					metrics);

			// The default directory for externalized checkpoints
			String externalizedCheckpointsDir = jobManagerConfig.getString(CheckpointingOptions.CHECKPOINTS_DIRECTORY);

			// load the state backend from the application settings
			final StateBackend applicationConfiguredBackend;
			final SerializedValue<StateBackend> serializedAppConfigured = snapshotSettings.getDefaultStateBackend();

			if (serializedAppConfigured == null) {
				applicationConfiguredBackend = null;
			}
			else {
				try {
					applicationConfiguredBackend = serializedAppConfigured.deserializeValue(classLoader);
				} catch (IOException | ClassNotFoundException e) {
					throw new JobExecutionException(jobId,
							"Could not deserialize application-defined state backend.", e);
				}
			}

			final StateBackend rootBackend;
			try {
				rootBackend = StateBackendLoader.fromApplicationOrConfigOrDefault(
						applicationConfiguredBackend, jobManagerConfig, classLoader, log);
			}
			catch (IllegalConfigurationException | IOException | DynamicCodeLoadingException e) {
				throw new JobExecutionException(jobId, "Could not instantiate configured state backend", e);
			}

			// instantiate the user-defined checkpoint hooks

			final SerializedValue<MasterTriggerRestoreHook.Factory[]> serializedHooks = snapshotSettings.getMasterHooks();
			final List<MasterTriggerRestoreHook<?>> hooks;

			if (serializedHooks == null) {
				hooks = Collections.emptyList();
			}
			else {
				final MasterTriggerRestoreHook.Factory[] hookFactories;
				try {
					hookFactories = serializedHooks.deserializeValue(classLoader);
				}
				catch (IOException | ClassNotFoundException e) {
					throw new JobExecutionException(jobId, "Could not instantiate user-defined checkpoint hooks", e);
				}

				final Thread thread = Thread.currentThread();
				final ClassLoader originalClassLoader = thread.getContextClassLoader();
				thread.setContextClassLoader(classLoader);

				try {
					hooks = new ArrayList<>(hookFactories.length);
					for (MasterTriggerRestoreHook.Factory factory : hookFactories) {
						hooks.add(MasterHooks.wrapHook(factory.create(), classLoader));
					}
				}
				finally {
					thread.setContextClassLoader(originalClassLoader);
				}
			}

			final CheckpointCoordinatorConfiguration chkConfig = snapshotSettings.getCheckpointCoordinatorConfiguration();

			executionGraph.enableCheckpointing(
				chkConfig.getCheckpointInterval(),
				chkConfig.getCheckpointTimeout(),
				chkConfig.getMinPauseBetweenCheckpoints(),
				chkConfig.getMaxConcurrentCheckpoints(),
				chkConfig.getCheckpointRetentionPolicy(),
				triggerVertices,
				ackVertices,
				confirmVertices,
				hooks,
				checkpointIdCounter,
				completedCheckpoints,
				rootBackend,
				checkpointStatsTracker);
		}

		// create all the metrics for the Execution Graph

		metrics.gauge(RestartTimeGauge.METRIC_NAME, new RestartTimeGauge(executionGraph));
		metrics.gauge(DownTimeGauge.METRIC_NAME, new DownTimeGauge(executionGraph));
		metrics.gauge(UpTimeGauge.METRIC_NAME, new UpTimeGauge(executionGraph));
		metrics.gauge(NumberOfFullRestartsGauge.METRIC_NAME, new NumberOfFullRestartsGauge(executionGraph));

		executionGraph.getFailoverStrategy().registerMetrics(metrics);

		return executionGraph;
	}

```

### ExecutionGraph.attachJobGraph

- 把JobVertex 转化为ExecutionJobVertex,调用new ExecutionJobVertex(),ExecutionJobVertex中存了inputSplits，所以会根据并行并来计算inputSplits的个数

    ```
    // create the execution job vertex and attach it to the graph
			ExecutionJobVertex ejv = new ExecutionJobVertex(
				this,
				jobVertex,
				1,
				rpcTimeout,
				globalModVersion,
				createTimestamp);
    ```

```
// --------------------------------------------------------------------------------------------
	//  Actions
	// --------------------------------------------------------------------------------------------

	public void attachJobGraph(List<JobVertex> topologiallySorted) throws JobException {

		LOG.debug("Attaching {} topologically sorted vertices to existing job graph with {} " +
				"vertices and {} intermediate results.",
				topologiallySorted.size(), tasks.size(), intermediateResults.size());

		final ArrayList<ExecutionJobVertex> newExecJobVertices = new ArrayList<>(topologiallySorted.size());
		final long createTimestamp = System.currentTimeMillis();

		for (JobVertex jobVertex : topologiallySorted) {

			if (jobVertex.isInputVertex() && !jobVertex.isStoppable()) {
				this.isStoppable = false;
			}

			// create the execution job vertex and attach it to the graph
			ExecutionJobVertex ejv = new ExecutionJobVertex(
				this,
				jobVertex,
				1,
				rpcTimeout,
				globalModVersion,
				createTimestamp);

			ejv.connectToPredecessors(this.intermediateResults);

			ExecutionJobVertex previousTask = this.tasks.putIfAbsent(jobVertex.getID(), ejv);
			if (previousTask != null) {
				throw new JobException(String.format("Encountered two job vertices with ID %s : previous=[%s] / new=[%s]",
						jobVertex.getID(), ejv, previousTask));
			}

			for (IntermediateResult res : ejv.getProducedDataSets()) {
				IntermediateResult previousDataSet = this.intermediateResults.putIfAbsent(res.getId(), res);
				if (previousDataSet != null) {
					throw new JobException(String.format("Encountered two intermediate data set with ID %s : previous=[%s] / new=[%s]",
							res.getId(), res, previousDataSet));
				}
			}

			this.verticesInCreationOrder.add(ejv);
			this.numVerticesTotal += ejv.getParallelism();
			newExecJobVertices.add(ejv);
		}

		terminationFuture = new CompletableFuture<>();
		failoverStrategy.notifyNewVertices(newExecJobVertices);
	}
```

### ExecutionJobVerte

- 调用FileInputFormat.createInputSplits(并行度)再实际处理

    ```
    	@SuppressWarnings("unchecked")
			InputSplitSource<InputSplit> splitSource = (InputSplitSource<InputSplit>) jobVertex.getInputSplitSource();

			if (splitSource != null) {
				Thread currentThread = Thread.currentThread();
				ClassLoader oldContextClassLoader = currentThread.getContextClassLoader();
				currentThread.setContextClassLoader(graph.getUserClassLoader());
				try {
					inputSplits = splitSource.createInputSplits(numTaskVertices);

					if (inputSplits != null) {
						splitAssigner = splitSource.getInputSplitAssigner(inputSplits);
					}
				} finally {
					currentThread.setContextClassLoader(oldContextClassLoader);
				}
			}
    ```

```
	public ExecutionJobVertex(
			ExecutionGraph graph,
			JobVertex jobVertex,
			int defaultParallelism,
			Time timeout,
			long initialGlobalModVersion,
			long createTimestamp) throws JobException {

		if (graph == null || jobVertex == null) {
			throw new NullPointerException();
		}

		this.graph = graph;
		this.jobVertex = jobVertex;

		int vertexParallelism = jobVertex.getParallelism();
		int numTaskVertices = vertexParallelism > 0 ? vertexParallelism : defaultParallelism;

		final int configuredMaxParallelism = jobVertex.getMaxParallelism();

		this.maxParallelismConfigured = (VALUE_NOT_SET != configuredMaxParallelism);

		// if no max parallelism was configured by the user, we calculate and set a default
		setMaxParallelismInternal(maxParallelismConfigured ?
				configuredMaxParallelism : KeyGroupRangeAssignment.computeDefaultMaxParallelism(numTaskVertices));

		// verify that our parallelism is not higher than the maximum parallelism
		if (numTaskVertices > maxParallelism) {
			throw new JobException(
				String.format("Vertex %s's parallelism (%s) is higher than the max parallelism (%s). Please lower the parallelism or increase the max parallelism.",
					jobVertex.getName(),
					numTaskVertices,
					maxParallelism));
		}

		this.parallelism = numTaskVertices;

		this.serializedTaskInformation = null;

		this.taskVertices = new ExecutionVertex[numTaskVertices];
		this.operatorIDs = Collections.unmodifiableList(jobVertex.getOperatorIDs());
		this.userDefinedOperatorIds = Collections.unmodifiableList(jobVertex.getUserDefinedOperatorIDs());

		this.inputs = new ArrayList<>(jobVertex.getInputs().size());

		// take the sharing group
		this.slotSharingGroup = jobVertex.getSlotSharingGroup();
		this.coLocationGroup = jobVertex.getCoLocationGroup();

		// setup the coLocation group
		if (coLocationGroup != null && slotSharingGroup == null) {
			throw new JobException("Vertex uses a co-location constraint without using slot sharing");
		}

		// create the intermediate results
		this.producedDataSets = new IntermediateResult[jobVertex.getNumberOfProducedIntermediateDataSets()];

		for (int i = 0; i < jobVertex.getProducedDataSets().size(); i++) {
			final IntermediateDataSet result = jobVertex.getProducedDataSets().get(i);

			this.producedDataSets[i] = new IntermediateResult(
					result.getId(),
					this,
					numTaskVertices,
					result.getResultType());
		}

		Configuration jobConfiguration = graph.getJobConfiguration();
		int maxPriorAttemptsHistoryLength = jobConfiguration != null ?
				jobConfiguration.getInteger(JobManagerOptions.MAX_ATTEMPTS_HISTORY_SIZE) :
				JobManagerOptions.MAX_ATTEMPTS_HISTORY_SIZE.defaultValue();

		// create all task vertices
		for (int i = 0; i < numTaskVertices; i++) {
			ExecutionVertex vertex = new ExecutionVertex(
					this,
					i,
					producedDataSets,
					timeout,
					initialGlobalModVersion,
					createTimestamp,
					maxPriorAttemptsHistoryLength);

			this.taskVertices[i] = vertex;
		}

		// sanity check for the double referencing between intermediate result partitions and execution vertices
		for (IntermediateResult ir : this.producedDataSets) {
			if (ir.getNumberOfAssignedPartitions() != parallelism) {
				throw new RuntimeException("The intermediate result's partitions were not correctly assigned.");
			}
		}

		// set up the input splits, if the vertex has any
		try {
			@SuppressWarnings("unchecked")
			InputSplitSource<InputSplit> splitSource = (InputSplitSource<InputSplit>) jobVertex.getInputSplitSource();

			if (splitSource != null) {
				Thread currentThread = Thread.currentThread();
				ClassLoader oldContextClassLoader = currentThread.getContextClassLoader();
				currentThread.setContextClassLoader(graph.getUserClassLoader());
				try {
					inputSplits = splitSource.createInputSplits(numTaskVertices);

					if (inputSplits != null) {
						splitAssigner = splitSource.getInputSplitAssigner(inputSplits);
					}
				} finally {
					currentThread.setContextClassLoader(oldContextClassLoader);
				}
			}
			else {
				inputSplits = null;
			}
		}
		catch (Throwable t) {
			throw new JobException("Creating the input splits caused an error: " + t.getMessage(), t);
		}
	}

```

### FileInputFormat.createInputSplits
- 真正的方法在这里，根据并行度，把文件拆分成FileInputSplit[] 
- 首先遍历路径是文件或目录，计算出所有文件放到List<FileStatus> files = new ArrayList<>()中存储,计算出所有文件总大小totalLength,计算文件切片，当然是所有文件总大小来计算

    ```
    // get all the files that are involved in the splits
		List<FileStatus> files = new ArrayList<>();
		long totalLength = 0;

		for (Path path : getFilePaths()) {
			final FileSystem fs = path.getFileSystem();
			final FileStatus pathFile = fs.getFileStatus(path);

			if (pathFile.isDir()) {
				totalLength += addFilesInDir(path, files, true);
			} else {
				testForUnsplittable(pathFile);

				files.add(pathFile);
				totalLength += pathFile.getLen();
			}
		}
    ```
- 每个切片最大长度计算,totalLength  = 9 为文件总长度，minNumSplits = 2 为并行度,也就是9不能整除并行度2，说明有余数，如果把余数的数据单独在分配一个切片，有可能这一个切片的数据量很少，就浪费资源了，这里的做法是，余数的最大值，也就是每个切片+1,就把这里多的余数分配到前面的每个切片中，也就是每个切片的最大值为 9 / 2 + 1 = 5 
 
    ```
final long maxSplitSize = totalLength / minNumSplits + (totalLength % minNumSplits == 0 ? 0 : 1);
    ```
- 计算实际切片的大小，blockSize 此处为文件大小，maxSplitSize 一般都小于blockSize，所以最后取的是切片的最大长度maxSplitSize

    ```
    final long splitSize = Math.max(minSplitSize, Math.min(maxSplitSize, blockSize));
    ```

- 实际计算时，当计算最后一个切片时，如果剩下的数据大小小于 切片大小的1.1倍，就放在一个切片中，不在切分了，直接把剩下的数据放到最后一个切片中，因为如果切后后，导致最后一切片数据量很小，浪费资源
 
    ```
    final long maxBytesForLastSplit = (long) (splitSize * MAX_SPLIT_SIZE_DISCREPANCY);
    ```



- 切片拆分的计算方法，初使值 bytesUnassigned = len(文件总数据长度),每分一次bytesUnassigned会减去当前切片的大小，也就是bytesUnassigned每次都是还剩下总的数据大小，当bytesUnassigned > maxBytesForLastSplit 就一直循环拆分切片，切片的长度为splitSize(切片大小) = 5, 开始位置从0开始，以后每个切片开始位置都需要加上之前所有切片大小 position += splitSize ;

    ```
    			while (bytesUnassigned > maxBytesForLastSplit) {
					// get the block containing the majority of the data
					blockIndex = getBlockIndexForPosition(blocks, position, halfSplit, blockIndex);
					// create a new split
					FileInputSplit fis = new FileInputSplit(splitNum++, file.getPath(), position, splitSize,
						blocks[blockIndex].getHosts());
					inputSplits.add(fis);

					// adjust the positions
					position += splitSize;
					bytesUnassigned -= splitSize;
				}
    ```
- 由于while循环拆分切片是有条件的，bytesUnassigned > maxBytesForLastSplit，那如果bytesUnassigned <= maxBytesForLastSplit,就需要把剩下的数据，都放到最后一个切片中
 
    ```
    	// assign the last split
				if (bytesUnassigned > 0) {
					blockIndex = getBlockIndexForPosition(blocks, position, halfSplit, blockIndex);
					final FileInputSplit fis = new FileInputSplit(splitNum++, file.getPath(), position,
						bytesUnassigned, blocks[blockIndex].getHosts());
					inputSplits.add(fis);
				}
    ```

```
/**
	 * Computes the input splits for the file. By default, one file block is one split. If more splits
	 * are requested than blocks are available, then a split may be a fraction of a block and splits may cross
	 * block boundaries.
	 * 
	 * @param minNumSplits The minimum desired number of file splits.
	 * @return The computed file splits.
	 * 
	 * @see org.apache.flink.api.common.io.InputFormat#createInputSplits(int)
	 */
	@Override
	public FileInputSplit[] createInputSplits(int minNumSplits) throws IOException {
		if (minNumSplits < 1) {
			throw new IllegalArgumentException("Number of input splits has to be at least 1.");
		}
		
		// take the desired number of splits into account
		minNumSplits = Math.max(minNumSplits, this.numSplits);
		
		final List<FileInputSplit> inputSplits = new ArrayList<FileInputSplit>(minNumSplits);

		// get all the files that are involved in the splits
		List<FileStatus> files = new ArrayList<>();
		long totalLength = 0;

		for (Path path : getFilePaths()) {
			final FileSystem fs = path.getFileSystem();
			final FileStatus pathFile = fs.getFileStatus(path);

			if (pathFile.isDir()) {
				totalLength += addFilesInDir(path, files, true);
			} else {
				testForUnsplittable(pathFile);

				files.add(pathFile);
				totalLength += pathFile.getLen();
			}
		}

		// returns if unsplittable
		if (unsplittable) {
			int splitNum = 0;
			for (final FileStatus file : files) {
				final FileSystem fs = file.getPath().getFileSystem();
				final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, file.getLen());
				Set<String> hosts = new HashSet<String>();
				for(BlockLocation block : blocks) {
					hosts.addAll(Arrays.asList(block.getHosts()));
				}
				long len = file.getLen();
				if(testForUnsplittable(file)) {
					len = READ_WHOLE_SPLIT_FLAG;
				}
				FileInputSplit fis = new FileInputSplit(splitNum++, file.getPath(), 0, len,
						hosts.toArray(new String[hosts.size()]));
				inputSplits.add(fis);
			}
			return inputSplits.toArray(new FileInputSplit[inputSplits.size()]);
		}
		

		final long maxSplitSize = totalLength / minNumSplits + (totalLength % minNumSplits == 0 ? 0 : 1);

		// now that we have the files, generate the splits
		int splitNum = 0;
		for (final FileStatus file : files) {

			final FileSystem fs = file.getPath().getFileSystem();
			final long len = file.getLen();
			final long blockSize = file.getBlockSize();
			
			final long minSplitSize;
			if (this.minSplitSize <= blockSize) {
				minSplitSize = this.minSplitSize;
			}
			else {
				if (LOG.isWarnEnabled()) {
					LOG.warn("Minimal split size of " + this.minSplitSize + " is larger than the block size of " + 
						blockSize + ". Decreasing minimal split size to block size.");
				}
				minSplitSize = blockSize;
			}

			final long splitSize = Math.max(minSplitSize, Math.min(maxSplitSize, blockSize));
			final long halfSplit = splitSize >>> 1;

			final long maxBytesForLastSplit = (long) (splitSize * MAX_SPLIT_SIZE_DISCREPANCY);

			if (len > 0) {

				// get the block locations and make sure they are in order with respect to their offset
				final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, len);
				Arrays.sort(blocks);

				long bytesUnassigned = len;
				long position = 0;

				int blockIndex = 0;

				while (bytesUnassigned > maxBytesForLastSplit) {
					// get the block containing the majority of the data
					blockIndex = getBlockIndexForPosition(blocks, position, halfSplit, blockIndex);
					// create a new split
					FileInputSplit fis = new FileInputSplit(splitNum++, file.getPath(), position, splitSize,
						blocks[blockIndex].getHosts());
					inputSplits.add(fis);

					// adjust the positions
					position += splitSize;
					bytesUnassigned -= splitSize;
				}

				// assign the last split
				if (bytesUnassigned > 0) {
					blockIndex = getBlockIndexForPosition(blocks, position, halfSplit, blockIndex);
					final FileInputSplit fis = new FileInputSplit(splitNum++, file.getPath(), position,
						bytesUnassigned, blocks[blockIndex].getHosts());
					inputSplits.add(fis);
				}
			} else {
				// special case with a file of zero bytes size
				final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, 0);
				String[] hosts;
				if (blocks.length > 0) {
					hosts = blocks[0].getHosts();
				} else {
					hosts = new String[0];
				}
				final FileInputSplit fis = new FileInputSplit(splitNum++, file.getPath(), 0, 0, hosts);
				inputSplits.add(fis);
			}
		}

		return inputSplits.toArray(new FileInputSplit[inputSplits.size()]);
	}

```

## 源码分析(切分文件实际读取)（切片一）
## DataSourceTask
### DataSourceTask.invoke()

- Source 的操作链(ChainedFlatMapDriver,ChainedMapDriver,SynchronousChainedCombineDriver) 即 FlatMap -> Map -> Combine (SUM(1)),也就是source读到的数据，都需要经过链上的算子操作
 
    ```
// start all chained tasks
			BatchTask.openChainedTasks(this.chainedTasks, this);
    ```

    ```
    this.chainedTasks = {ArrayList@5459}  size = 3
 0 = {ChainedFlatMapDriver@5458} 
 1 = {ChainedMapDriver@5505} 
 2 = {SynchronousChainedCombineDriver@5506} 
    ```

- 随机读到一个切片，给当前DataSourceTask使用,因为在Source读取数据时是不按key分区，也就不分谁处理，有任务来处理，就给一个切片处理就行，每给出一个从总的切片中移除

    ```
    final InputSplit split = splitIterator.next();
    ```
- 当前切片信息
 
    ```
        LOG.debug(getLogString("Opening input split " + split.toString()));
        
        
        
        
        13:04:02,082 DEBUG [CHAIN DataSource (at com.opensourceteams.module.bigdata.flink.example.dataset.worldcount.WordCountRun$.main(WordCountRun.scala:19) (org.apache.flink.api.java.io.TextInp) -> FlatMap (FlatMap at com.opensourceteams.module.bigdata.flink.example.dataset.worldcount.WordCountRun$.main(WordCountRun.scala:23)) -> Map (Map at com.opensourceteams.module.bigdata.flink.example.dataset.worldcount.WordCountRun$.main(WordCountRun.scala:23)) -> Combine (SUM(1)) (2/2)] org.apache.flink.runtime.operators.DataSourceTask.invoke(DataSourceTask.java:165)      - Opening input split [1] file:/opt/n_001_workspaces/bigdata/flink/flink-maven-scala-2/src/main/resources/data/line.txt:5+4:  CHAIN DataSource (at com.opensourceteams.module.bigdata.flink.example.dataset.worldcount.WordCountRun$.main(WordCountRun.scala:19) (org.apache.flink.api.java.io.TextInp) -> FlatMap (FlatMap at com.opensourceteams.module.bigdata.flink.example.dataset.worldcount.WordCountRun$.main(WordCountRun.scala:23)) -> Map (Map at com.opensourceteams.module.bigdata.flink.example.dataset.worldcount.WordCountRun$.main(WordCountRun.scala:23)) -> Combine (SUM(1)) (2/2)
    ```
   
- 对当前切片进行处理  ,调用 DelimitedInputFormat.open()，//open还没开始真正的读数据，只是定位，把第一个换行符，分到前一个分片，自己从第二个换行符开始读取数据
    ```
    format.open(split);
    ```

```
@Override
	public void invoke() throws Exception {
		// --------------------------------------------------------------------
		// Initialize
		// --------------------------------------------------------------------
		initInputFormat();

		LOG.debug(getLogString("Start registering input and output"));

		try {
			initOutputs(getUserCodeClassLoader());
		} catch (Exception ex) {
			throw new RuntimeException("The initialization of the DataSource's outputs caused an error: " +
					ex.getMessage(), ex);
		}

		LOG.debug(getLogString("Finished registering input and output"));

		// --------------------------------------------------------------------
		// Invoke
		// --------------------------------------------------------------------
		LOG.debug(getLogString("Starting data source operator"));

		RuntimeContext ctx = createRuntimeContext();

		final Counter numRecordsOut;
		{
			Counter tmpNumRecordsOut;
			try {
				OperatorIOMetricGroup ioMetricGroup = ((OperatorMetricGroup) ctx.getMetricGroup()).getIOMetricGroup();
				ioMetricGroup.reuseInputMetricsForTask();
				if (this.config.getNumberOfChainedStubs() == 0) {
					ioMetricGroup.reuseOutputMetricsForTask();
				}
				tmpNumRecordsOut = ioMetricGroup.getNumRecordsOutCounter();
			} catch (Exception e) {
				LOG.warn("An exception occurred during the metrics setup.", e);
				tmpNumRecordsOut = new SimpleCounter();
			}
			numRecordsOut = tmpNumRecordsOut;
		}
		
		Counter completedSplitsCounter = ctx.getMetricGroup().counter("numSplitsProcessed");

		if (RichInputFormat.class.isAssignableFrom(this.format.getClass())) {
			((RichInputFormat) this.format).setRuntimeContext(ctx);
			LOG.debug(getLogString("Rich Source detected. Initializing runtime context."));
			((RichInputFormat) this.format).openInputFormat();
			LOG.debug(getLogString("Rich Source detected. Opening the InputFormat."));
		}

		ExecutionConfig executionConfig = getExecutionConfig();

		boolean objectReuseEnabled = executionConfig.isObjectReuseEnabled();

		LOG.debug("DataSourceTask object reuse: " + (objectReuseEnabled ? "ENABLED" : "DISABLED") + ".");
		
		final TypeSerializer<OT> serializer = this.serializerFactory.getSerializer();
		
		try {
			// start all chained tasks
			BatchTask.openChainedTasks(this.chainedTasks, this);
			
			// get input splits to read
			final Iterator<InputSplit> splitIterator = getInputSplits();
			
			// for each assigned input split
			while (!this.taskCanceled && splitIterator.hasNext())
			{
				// get start and end
				final InputSplit split = splitIterator.next();

				LOG.debug(getLogString("Opening input split " + split.toString()));
				
				final InputFormat<OT, InputSplit> format = this.format;
			
				// open input format
				format.open(split);
	
				LOG.debug(getLogString("Starting to read input from split " + split.toString()));
				
				try {
					final Collector<OT> output = new CountingCollector<>(this.output, numRecordsOut);

					if (objectReuseEnabled) {
						OT reuse = serializer.createInstance();

						// as long as there is data to read
						while (!this.taskCanceled && !format.reachedEnd()) {

							OT returned;
							if ((returned = format.nextRecord(reuse)) != null) {
								output.collect(returned);
							}
						}
					} else {
						// as long as there is data to read
						while (!this.taskCanceled && !format.reachedEnd()) {
							OT returned;
							if ((returned = format.nextRecord(serializer.createInstance())) != null) {
								output.collect(returned);
							}
						}
					}

					if (LOG.isDebugEnabled() && !this.taskCanceled) {
						LOG.debug(getLogString("Closing input split " + split.toString()));
					}
				} finally {
					// close. We close here such that a regular close throwing an exception marks a task as failed.
					format.close();
				}
				completedSplitsCounter.inc();
			} // end for all input splits

			// close the collector. if it is a chaining task collector, it will close its chained tasks
			this.output.close();

			// close all chained tasks letting them report failure
			BatchTask.closeChainedTasks(this.chainedTasks, this);

		}
		catch (Exception ex) {
			// close the input, but do not report any exceptions, since we already have another root cause
			try {
				this.format.close();
			} catch (Throwable ignored) {}

			BatchTask.cancelChainedTasks(this.chainedTasks);

			ex = ExceptionInChainedStubException.exceptionUnwrap(ex);

			if (ex instanceof CancelTaskException) {
				// forward canceling exception
				throw ex;
			}
			else if (!this.taskCanceled) {
				// drop exception, if the task was canceled
				BatchTask.logAndThrowException(ex, this);
			}
		} finally {
			BatchTask.clearWriters(eventualOutputs);
			// --------------------------------------------------------------------
			// Closing
			// --------------------------------------------------------------------
			if (this.format != null && RichInputFormat.class.isAssignableFrom(this.format.getClass())) {
				((RichInputFormat) this.format).closeInputFormat();
				LOG.debug(getLogString("Rich Source detected. Closing the InputFormat."));
			}
		}

		if (!this.taskCanceled) {
			LOG.debug(getLogString("Finished data source operator"));
		}
		else {
			LOG.debug(getLogString("Data source operator cancelled"));
		}
	}

```



### DelimitedInputFormat.open()
- 调用FileInputFormat.open(split),设置当前切片信息(切片的开始位置，切片长度)，和定位开始位置
- initBuffers();// 初使化Buffers信息,默认的readBuffer大小为1M,wrapBuffer 为256 byte
- 调用 DelimitedInputFormat.readLine()

```
/**
	 * Opens the given input split. This method opens the input stream to the specified file, allocates read buffers
	 * and positions the stream at the correct position, making sure that any partial record at the beginning is skipped.
	 *
	 * @param split The input split to open.
	 *
	 * @see org.apache.flink.api.common.io.FileInputFormat#open(org.apache.flink.core.fs.FileInputSplit)
	 */
	@Override
	public void open(FileInputSplit split) throws IOException {
		super.open(split);
		initBuffers();

		this.offset = splitStart;
		if (this.splitStart != 0) {
			this.stream.seek(offset);
			readLine();
			// if the first partial record already pushes the stream over
			// the limit of our split, then no record starts within this split
			if (this.overLimit) {
				this.end = true;
			}
		} else {
			fillBuffer(0);
		}
	}
```

### FileInputFormat.open(split)
- 调置当前分片 
 
    ```
    this.currentSplit = fileSplit;
    ```
- 调置当前分片的开始位置
 
    ```
    this.splitStart = fileSplit.getStart();
    ```
- 调置当前分片的长度
 
    ```
   this.splitLength = fileSplit.getLength();
    ```

- 流定位到开始位置
     
    ```
    	// get FSDataInputStream
    		if (this.splitStart != 0) {
    			this.stream.seek(this.splitStart);
    		}
    ```

```
/**
	 * Opens an input stream to the file defined in the input format.
	 * The stream is positioned at the beginning of the given split.
	 * <p>
	 * The stream is actually opened in an asynchronous thread to make sure any interruptions to the thread 
	 * working on the input format do not reach the file system.
	 */
	@Override
	public void open(FileInputSplit fileSplit) throws IOException {

		this.currentSplit = fileSplit;
		this.splitStart = fileSplit.getStart();
		this.splitLength = fileSplit.getLength();

		if (LOG.isDebugEnabled()) {
			LOG.debug("Opening input split " + fileSplit.getPath() + " [" + this.splitStart + "," + this.splitLength + "]");
		}

		
		// open the split in an asynchronous thread
		final InputSplitOpenThread isot = new InputSplitOpenThread(fileSplit, this.openTimeout);
		isot.start();
		
		try {
			this.stream = isot.waitForCompletion();
			this.stream = decorateInputStream(this.stream, fileSplit);
		}
		catch (Throwable t) {
			throw new IOException("Error opening the Input Split " + fileSplit.getPath() + 
					" [" + splitStart + "," + splitLength + "]: " + t.getMessage(), t);
		}
		
		// get FSDataInputStream
		if (this.splitStart != 0) {
			this.stream.seek(this.splitStart);
		}
	}

```

### DelimitedInputFormat.initBuffers
- 默认的readBuffer大小为1M,wrapBuffer 为256 byte
- 当前切片默认值设置

    ```
    	this.readPos = 0;
    		this.limit = 0;
    		this.overLimit = false;
    		this.end = false;
    ```

```
private void initBuffers() {
		this.bufferSize = this.bufferSize <= 0 ? DEFAULT_READ_BUFFER_SIZE : this.bufferSize;

		if (this.bufferSize <= this.delimiter.length) {
			throw new IllegalArgumentException("Buffer size must be greater than length of delimiter.");
		}

		if (this.readBuffer == null || this.readBuffer.length != this.bufferSize) {
			this.readBuffer = new byte[this.bufferSize];
		}
		if (this.wrapBuffer == null || this.wrapBuffer.length < 256) {
			this.wrapBuffer = new byte[256];
		}

		this.readPos = 0;
		this.limit = 0;
		this.overLimit = false;
		this.end = false;
	}
```

### DelimitedInputFormat.readLine()
- (读取数据到缓存中)调用 DelimitedInputFormat.fillBuffer()，读到数据到缓存中，如果数据大于1m就读1m的数据，如果小于1m，就把当前切片的数据全部读完
- 读取一行数据,也就是读到第一个换行符
    ```
    			// Search for next occurrence of delimiter in read buffer.
    			while (this.readPos < this.limit && delimPos < this.delimiter.length) {
    				if ((this.readBuffer[this.readPos]) == this.delimiter[delimPos]) {
    					// Found the expected delimiter character. Continue looking for the next character of delimiter.
    					delimPos++;
    				} else {
    					// Delimiter does not match.
    					// We have to reset the read position to the character after the first matching character
    					//   and search for the whole delimiter again.
    					readPos -= delimPos;
    					delimPos = 0;
    				}
    				readPos++;
    			}
    ```
- 第一次，startPos =0 ,count = 0，没读到数据
    ```
    setResult(this.readBuffer, startPos, count);
    ```

```
protected final boolean readLine() throws IOException {
		if (this.stream == null || this.overLimit) {
			return false;
		}

		int countInWrapBuffer = 0;

		// position of matching positions in the delimiter byte array
		int delimPos = 0;

		while (true) {
			if (this.readPos >= this.limit) {
				// readBuffer is completely consumed. Fill it again but keep partially read delimiter bytes.
				if (!fillBuffer(delimPos)) {
					int countInReadBuffer = delimPos;
					if (countInWrapBuffer + countInReadBuffer > 0) {
						// we have bytes left to emit
						if (countInReadBuffer > 0) {
							// we have bytes left in the readBuffer. Move them into the wrapBuffer
							if (this.wrapBuffer.length - countInWrapBuffer < countInReadBuffer) {
								// reallocate
								byte[] tmp = new byte[countInWrapBuffer + countInReadBuffer];
								System.arraycopy(this.wrapBuffer, 0, tmp, 0, countInWrapBuffer);
								this.wrapBuffer = tmp;
							}

							// copy readBuffer bytes to wrapBuffer
							System.arraycopy(this.readBuffer, 0, this.wrapBuffer, countInWrapBuffer, countInReadBuffer);
							countInWrapBuffer += countInReadBuffer;
						}

						this.offset += countInWrapBuffer;
						setResult(this.wrapBuffer, 0, countInWrapBuffer);
						return true;
					} else {
						return false;
					}
				}
			}

			int startPos = this.readPos - delimPos;
			int count;

			// Search for next occurrence of delimiter in read buffer.
			while (this.readPos < this.limit && delimPos < this.delimiter.length) {
				if ((this.readBuffer[this.readPos]) == this.delimiter[delimPos]) {
					// Found the expected delimiter character. Continue looking for the next character of delimiter.
					delimPos++;
				} else {
					// Delimiter does not match.
					// We have to reset the read position to the character after the first matching character
					//   and search for the whole delimiter again.
					readPos -= delimPos;
					delimPos = 0;
				}
				readPos++;
			}

			// check why we dropped out
			if (delimPos == this.delimiter.length) {
				// we found a delimiter
				int readBufferBytesRead = this.readPos - startPos;
				this.offset += countInWrapBuffer + readBufferBytesRead;
				count = readBufferBytesRead - this.delimiter.length;

				// copy to byte array
				if (countInWrapBuffer > 0) {
					// check wrap buffer size
					if (this.wrapBuffer.length < countInWrapBuffer + count) {
						final byte[] nb = new byte[countInWrapBuffer + count];
						System.arraycopy(this.wrapBuffer, 0, nb, 0, countInWrapBuffer);
						this.wrapBuffer = nb;
					}
					if (count >= 0) {
						System.arraycopy(this.readBuffer, 0, this.wrapBuffer, countInWrapBuffer, count);
					}
					setResult(this.wrapBuffer, 0, countInWrapBuffer + count);
					return true;
				} else {
					setResult(this.readBuffer, startPos, count);
					return true;
				}
			} else {
				// we reached the end of the readBuffer
				count = this.limit - startPos;
				
				// check against the maximum record length
				if (((long) countInWrapBuffer) + count > this.lineLengthLimit) {
					throw new IOException("The record length exceeded the maximum record length (" + 
							this.lineLengthLimit + ").");
				}

				// Compute number of bytes to move to wrapBuffer
				// Chars of partially read delimiter must remain in the readBuffer. We might need to go back.
				int bytesToMove = count - delimPos;
				// ensure wrapBuffer is large enough
				if (this.wrapBuffer.length - countInWrapBuffer < bytesToMove) {
					// reallocate
					byte[] tmp = new byte[Math.max(this.wrapBuffer.length * 2, countInWrapBuffer + bytesToMove)];
					System.arraycopy(this.wrapBuffer, 0, tmp, 0, countInWrapBuffer);
					this.wrapBuffer = tmp;
				}

				// copy readBuffer to wrapBuffer (except delimiter chars)
				System.arraycopy(this.readBuffer, startPos, this.wrapBuffer, countInWrapBuffer, bytesToMove);
				countInWrapBuffer += bytesToMove;
				// move delimiter chars to the beginning of the readBuffer
				System.arraycopy(this.readBuffer, this.readPos - delimPos, this.readBuffer, 0, delimPos);

			}
		}
	}
```


### DelimitedInputFormat.fillBuffer()
- 读到数据到缓存中，如果数据大于1m就读1m的数据，如果小于1m，就把当前切片的数据全部读完
```
0 = 10
1 = 98
2 = 32
3 = 99
```

```
/**
	 * Fills the read buffer with bytes read from the file starting from an offset.
	 */
	private boolean fillBuffer(int offset) throws IOException {
		int maxReadLength = this.readBuffer.length - offset;
		// special case for reading the whole split.
		if (this.splitLength == FileInputFormat.READ_WHOLE_SPLIT_FLAG) {
			int read = this.stream.read(this.readBuffer, offset, maxReadLength);
			if (read == -1) {
				this.stream.close();
				this.stream = null;
				return false;
			} else {
				this.readPos = offset;
				this.limit = read;
				return true;
			}
		}
		
		// else ..
		int toRead;
		if (this.splitLength > 0) {
			// if we have more data, read that
			toRead = this.splitLength > maxReadLength ? maxReadLength : (int) this.splitLength;
		}
		else {
			// if we have exhausted our split, we need to complete the current record, or read one
			// more across the next split.
			// the reason is that the next split will skip over the beginning until it finds the first
			// delimiter, discarding it as an incomplete chunk of data that belongs to the last record in the
			// previous split.
			toRead = maxReadLength;
			this.overLimit = true;
		}

		int read = this.stream.read(this.readBuffer, offset, toRead);

		if (read == -1) {
			this.stream.close();
			this.stream = null;
			return false;
		} else {
			this.splitLength -= read;
			this.readPos = offset; // position from where to start reading
			this.limit = read + offset; // number of valid bytes in the read buffer
			return true;
		}
	}
```



### DelimitedInputFormat.nextRecord
- 读取一行数据,从当前分片，DelimitedInputFormat.open()已重新计算当前切片的开始位置
- 调用DelimitedInputFormat.readLine() 读取当前切片的一行数据

```
public OT nextRecord(OT record) throws IOException {
		if (readLine()) {
			return readRecord(record, this.currBuffer, this.currOffset, this.currLen);
		} else {
			this.end = true;
			return null;
		}
	}
```

### DelimitedInputFormat.readLine() 
- DelimitedInputFormat.open()已重新计算当前切片的开始位置,但是切片的长度不变，还是读取以前计算的长度
- 读到第一个换行符的数据，即读一行数据，如果没有换行符，当读取到当前切片最大长度
 
```
			// Search for next occurrence of delimiter in read buffer.
			while (this.readPos < this.limit && delimPos < this.delimiter.length) {
				if ((this.readBuffer[this.readPos]) == this.delimiter[delimPos]) {
					// Found the expected delimiter character. Continue looking for the next character of delimiter.
					delimPos++;
				} else {
					// Delimiter does not match.
					// We have to reset the read position to the character after the first matching character
					//   and search for the whole delimiter again.
					readPos -= delimPos;
					delimPos = 0;
				}
				readPos++;
			}
```
- 从缓存区readBuffer复制当前行数据到 wrapBuffer

    ```
    System.arraycopy(this.readBuffer, startPos, this.wrapBuffer, countInWrapBuffer, bytesToMove);
    ```
- 如果有换行符，需要删除换行符，在readBuffer
 
    ```
    // move delimiter chars to the beginning of the readBuffer
    				System.arraycopy(this.readBuffer, this.readPos - delimPos, this.readBuffer, 0, delimPos);
    ```

- 当前切片需要读取的个数this.limit，读取完后,继续读一个1m的数据到缓存中,最后将当前行数据返回setResult
- fillBuffer函数中，如果当前切处数据读完了，会设置overLimit = true,读下一行数据时就不满足条件就不会读了


```
if (this.readPos >= this.limit) {
				// readBuffer is completely consumed. Fill it again but keep partially read delimiter bytes.
				if (!fillBuffer(delimPos)) {
					int countInReadBuffer = delimPos;
					if (countInWrapBuffer + countInReadBuffer > 0) {
						// we have bytes left to emit
						if (countInReadBuffer > 0) {
							// we have bytes left in the readBuffer. Move them into the wrapBuffer
							if (this.wrapBuffer.length - countInWrapBuffer < countInReadBuffer) {
								// reallocate
								byte[] tmp = new byte[countInWrapBuffer + countInReadBuffer];
								System.arraycopy(this.wrapBuffer, 0, tmp, 0, countInWrapBuffer);
								this.wrapBuffer = tmp;
							}

							// copy readBuffer bytes to wrapBuffer
							System.arraycopy(this.readBuffer, 0, this.wrapBuffer, countInWrapBuffer, countInReadBuffer);
							countInWrapBuffer += countInReadBuffer;
						}

						this.offset += countInWrapBuffer;
						setResult(this.wrapBuffer, 0, countInWrapBuffer);
						return true;
					} else {
						return false;
					}
				}
			}

```
- 第一次读到的数据为
```
b c
```

```
protected final boolean readLine() throws IOException {
		if (this.stream == null || this.overLimit) {
			return false;
		}

		int countInWrapBuffer = 0;

		// position of matching positions in the delimiter byte array
		int delimPos = 0;

		while (true) {
			if (this.readPos >= this.limit) {
				// readBuffer is completely consumed. Fill it again but keep partially read delimiter bytes.
				if (!fillBuffer(delimPos)) {
					int countInReadBuffer = delimPos;
					if (countInWrapBuffer + countInReadBuffer > 0) {
						// we have bytes left to emit
						if (countInReadBuffer > 0) {
							// we have bytes left in the readBuffer. Move them into the wrapBuffer
							if (this.wrapBuffer.length - countInWrapBuffer < countInReadBuffer) {
								// reallocate
								byte[] tmp = new byte[countInWrapBuffer + countInReadBuffer];
								System.arraycopy(this.wrapBuffer, 0, tmp, 0, countInWrapBuffer);
								this.wrapBuffer = tmp;
							}

							// copy readBuffer bytes to wrapBuffer
							System.arraycopy(this.readBuffer, 0, this.wrapBuffer, countInWrapBuffer, countInReadBuffer);
							countInWrapBuffer += countInReadBuffer;
						}

						this.offset += countInWrapBuffer;
						setResult(this.wrapBuffer, 0, countInWrapBuffer);
						return true;
					} else {
						return false;
					}
				}
			}

			int startPos = this.readPos - delimPos;
			int count;

			// Search for next occurrence of delimiter in read buffer.
			while (this.readPos < this.limit && delimPos < this.delimiter.length) {
				if ((this.readBuffer[this.readPos]) == this.delimiter[delimPos]) {
					// Found the expected delimiter character. Continue looking for the next character of delimiter.
					delimPos++;
				} else {
					// Delimiter does not match.
					// We have to reset the read position to the character after the first matching character
					//   and search for the whole delimiter again.
					readPos -= delimPos;
					delimPos = 0;
				}
				readPos++;
			}

			// check why we dropped out
			if (delimPos == this.delimiter.length) {
				// we found a delimiter
				int readBufferBytesRead = this.readPos - startPos;
				this.offset += countInWrapBuffer + readBufferBytesRead;
				count = readBufferBytesRead - this.delimiter.length;

				// copy to byte array
				if (countInWrapBuffer > 0) {
					// check wrap buffer size
					if (this.wrapBuffer.length < countInWrapBuffer + count) {
						final byte[] nb = new byte[countInWrapBuffer + count];
						System.arraycopy(this.wrapBuffer, 0, nb, 0, countInWrapBuffer);
						this.wrapBuffer = nb;
					}
					if (count >= 0) {
						System.arraycopy(this.readBuffer, 0, this.wrapBuffer, countInWrapBuffer, count);
					}
					setResult(this.wrapBuffer, 0, countInWrapBuffer + count);
					return true;
				} else {
					setResult(this.readBuffer, startPos, count);
					return true;
				}
			} else {
				// we reached the end of the readBuffer
				count = this.limit - startPos;
				
				// check against the maximum record length
				if (((long) countInWrapBuffer) + count > this.lineLengthLimit) {
					throw new IOException("The record length exceeded the maximum record length (" + 
							this.lineLengthLimit + ").");
				}

				// Compute number of bytes to move to wrapBuffer
				// Chars of partially read delimiter must remain in the readBuffer. We might need to go back.
				int bytesToMove = count - delimPos;
				// ensure wrapBuffer is large enough
				if (this.wrapBuffer.length - countInWrapBuffer < bytesToMove) {
					// reallocate
					byte[] tmp = new byte[Math.max(this.wrapBuffer.length * 2, countInWrapBuffer + bytesToMove)];
					System.arraycopy(this.wrapBuffer, 0, tmp, 0, countInWrapBuffer);
					this.wrapBuffer = tmp;
				}

				// copy readBuffer to wrapBuffer (except delimiter chars)
				System.arraycopy(this.readBuffer, startPos, this.wrapBuffer, countInWrapBuffer, bytesToMove);
				countInWrapBuffer += bytesToMove;
				// move delimiter chars to the beginning of the readBuffer
				System.arraycopy(this.readBuffer, this.readPos - delimPos, this.readBuffer, 0, delimPos);

			}
		}
	}
```


## 源码分析(切分文件实际读取)（切片二）
## DataSourceTask
### DataSourceTask.invoke()

- Source 的操作链(ChainedFlatMapDriver,ChainedMapDriver,SynchronousChainedCombineDriver) 即 FlatMap -> Map -> Combine (SUM(1)),也就是source读到的数据，都需要经过链上的算子操作
 
    ```
// start all chained tasks
			BatchTask.openChainedTasks(this.chainedTasks, this);
    ```

    ```
    this.chainedTasks = {ArrayList@5459}  size = 3
 0 = {ChainedFlatMapDriver@5458} 
 1 = {ChainedMapDriver@5505} 
 2 = {SynchronousChainedCombineDriver@5506} 
    ```

- 随机读到一个切片，给当前DataSourceTask使用,因为在Source读取数据时是不按key分区，也就不分谁处理，有任务来处理，就给一个切片处理就行，每给出一个从总的切片中移除

    ```
    final InputSplit split = splitIterator.next();
    ```
- 当前切片信息
 
    ```
        LOG.debug(getLogString("Opening input split " + split.toString()));
        
        
        
        
       15:12:01,928 DEBUG [CHAIN DataSource (at com.opensourceteams.module.bigdata.flink.example.dataset.worldcount.WordCountRun$.main(WordCountRun.scala:19) (org.apache.flink.api.java.io.TextInp) -> FlatMap (FlatMap at com.opensourceteams.module.bigdata.flink.example.dataset.worldcount.WordCountRun$.main(WordCountRun.scala:23)) -> Map (Map at com.opensourceteams.module.bigdata.flink.example.dataset.worldcount.WordCountRun$.main(WordCountRun.scala:23)) -> Combine (SUM(1)) (2/2)] org.apache.flink.runtime.operators.DataSourceTask.invoke(DataSourceTask.java:172)      - Starting to read input from split [0] file:/opt/n_001_workspaces/bigdata/flink/flink-maven-scala-2/src/main/resources/data/line.txt:0+5:  CHAIN DataSource (at com.opensourceteams.module.bigdata.flink.example.dataset.worldcount.WordCountRun$.main(WordCountRun.scala:19) (org.apache.flink.api.java.io.TextInp) -> FlatMap (FlatMap at com.opensourceteams.module.bigdata.flink.example.dataset.worldcount.WordCountRun$.main(WordCountRun.scala:23)) -> Map (Map at com.opensourceteams.module.bigdata.flink.example.dataset.worldcount.WordCountRun$.main(WordCountRun.scala:23)) -> Combine (SUM(1)) (2/2)

    ```
   
- 对当前切片进行处理  ,调用 DelimitedInputFormat.open()，//open还没开始真正的读数据，只是定位，把第一个换行符，分到前一个分片，自己从第二个换行符开始读取数据
    ```
    format.open(split);
    ```

```
@Override
	public void invoke() throws Exception {
		// --------------------------------------------------------------------
		// Initialize
		// --------------------------------------------------------------------
		initInputFormat();

		LOG.debug(getLogString("Start registering input and output"));

		try {
			initOutputs(getUserCodeClassLoader());
		} catch (Exception ex) {
			throw new RuntimeException("The initialization of the DataSource's outputs caused an error: " +
					ex.getMessage(), ex);
		}

		LOG.debug(getLogString("Finished registering input and output"));

		// --------------------------------------------------------------------
		// Invoke
		// --------------------------------------------------------------------
		LOG.debug(getLogString("Starting data source operator"));

		RuntimeContext ctx = createRuntimeContext();

		final Counter numRecordsOut;
		{
			Counter tmpNumRecordsOut;
			try {
				OperatorIOMetricGroup ioMetricGroup = ((OperatorMetricGroup) ctx.getMetricGroup()).getIOMetricGroup();
				ioMetricGroup.reuseInputMetricsForTask();
				if (this.config.getNumberOfChainedStubs() == 0) {
					ioMetricGroup.reuseOutputMetricsForTask();
				}
				tmpNumRecordsOut = ioMetricGroup.getNumRecordsOutCounter();
			} catch (Exception e) {
				LOG.warn("An exception occurred during the metrics setup.", e);
				tmpNumRecordsOut = new SimpleCounter();
			}
			numRecordsOut = tmpNumRecordsOut;
		}
		
		Counter completedSplitsCounter = ctx.getMetricGroup().counter("numSplitsProcessed");

		if (RichInputFormat.class.isAssignableFrom(this.format.getClass())) {
			((RichInputFormat) this.format).setRuntimeContext(ctx);
			LOG.debug(getLogString("Rich Source detected. Initializing runtime context."));
			((RichInputFormat) this.format).openInputFormat();
			LOG.debug(getLogString("Rich Source detected. Opening the InputFormat."));
		}

		ExecutionConfig executionConfig = getExecutionConfig();

		boolean objectReuseEnabled = executionConfig.isObjectReuseEnabled();

		LOG.debug("DataSourceTask object reuse: " + (objectReuseEnabled ? "ENABLED" : "DISABLED") + ".");
		
		final TypeSerializer<OT> serializer = this.serializerFactory.getSerializer();
		
		try {
			// start all chained tasks
			BatchTask.openChainedTasks(this.chainedTasks, this);
			
			// get input splits to read
			final Iterator<InputSplit> splitIterator = getInputSplits();
			
			// for each assigned input split
			while (!this.taskCanceled && splitIterator.hasNext())
			{
				// get start and end
				final InputSplit split = splitIterator.next();

				LOG.debug(getLogString("Opening input split " + split.toString()));
				
				final InputFormat<OT, InputSplit> format = this.format;
			
				// open input format
				format.open(split);
	
				LOG.debug(getLogString("Starting to read input from split " + split.toString()));
				
				try {
					final Collector<OT> output = new CountingCollector<>(this.output, numRecordsOut);

					if (objectReuseEnabled) {
						OT reuse = serializer.createInstance();

						// as long as there is data to read
						while (!this.taskCanceled && !format.reachedEnd()) {

							OT returned;
							if ((returned = format.nextRecord(reuse)) != null) {
								output.collect(returned);
							}
						}
					} else {
						// as long as there is data to read
						while (!this.taskCanceled && !format.reachedEnd()) {
							OT returned;
							if ((returned = format.nextRecord(serializer.createInstance())) != null) {
								output.collect(returned);
							}
						}
					}

					if (LOG.isDebugEnabled() && !this.taskCanceled) {
						LOG.debug(getLogString("Closing input split " + split.toString()));
					}
				} finally {
					// close. We close here such that a regular close throwing an exception marks a task as failed.
					format.close();
				}
				completedSplitsCounter.inc();
			} // end for all input splits

			// close the collector. if it is a chaining task collector, it will close its chained tasks
			this.output.close();

			// close all chained tasks letting them report failure
			BatchTask.closeChainedTasks(this.chainedTasks, this);

		}
		catch (Exception ex) {
			// close the input, but do not report any exceptions, since we already have another root cause
			try {
				this.format.close();
			} catch (Throwable ignored) {}

			BatchTask.cancelChainedTasks(this.chainedTasks);

			ex = ExceptionInChainedStubException.exceptionUnwrap(ex);

			if (ex instanceof CancelTaskException) {
				// forward canceling exception
				throw ex;
			}
			else if (!this.taskCanceled) {
				// drop exception, if the task was canceled
				BatchTask.logAndThrowException(ex, this);
			}
		} finally {
			BatchTask.clearWriters(eventualOutputs);
			// --------------------------------------------------------------------
			// Closing
			// --------------------------------------------------------------------
			if (this.format != null && RichInputFormat.class.isAssignableFrom(this.format.getClass())) {
				((RichInputFormat) this.format).closeInputFormat();
				LOG.debug(getLogString("Rich Source detected. Closing the InputFormat."));
			}
		}

		if (!this.taskCanceled) {
			LOG.debug(getLogString("Finished data source operator"));
		}
		else {
			LOG.debug(getLogString("Data source operator cancelled"));
		}
	}

```



### DelimitedInputFormat.open()
- 调用FileInputFormat.open(split),设置当前切片信息(切片的开始位置，切片长度)，和定位开始位置
- initBuffers();// 初使化Buffers信息,默认的readBuffer大小为1M,wrapBuffer 为256 byte
- splitStart >0 需要调用 DelimitedInputFormat.readLine()，如果是 splitStart =0 ，直接调用fillBuffer

```
/**
	 * Opens the given input split. This method opens the input stream to the specified file, allocates read buffers
	 * and positions the stream at the correct position, making sure that any partial record at the beginning is skipped.
	 *
	 * @param split The input split to open.
	 *
	 * @see org.apache.flink.api.common.io.FileInputFormat#open(org.apache.flink.core.fs.FileInputSplit)
	 */
	@Override
	public void open(FileInputSplit split) throws IOException {
		super.open(split);
		initBuffers();

		this.offset = splitStart;
		if (this.splitStart != 0) {
			this.stream.seek(offset);
			readLine();
			// if the first partial record already pushes the stream over
			// the limit of our split, then no record starts within this split
			if (this.overLimit) {
				this.end = true;
			}
		} else {
			fillBuffer(0);
		}
	}
```

### FileInputFormat.open(split)
- 调置当前分片 
 
    ```
    this.currentSplit = fileSplit;
    ```
- 调置当前分片的开始位置
 
    ```
    this.splitStart = fileSplit.getStart();
    ```
- 调置当前分片的长度
 
    ```
   this.splitLength = fileSplit.getLength();
    ```

- 流定位到开始位置
     
    ```
    	// get FSDataInputStream
    		if (this.splitStart != 0) {
    			this.stream.seek(this.splitStart);
    		}
    ```

```
/**
	 * Opens an input stream to the file defined in the input format.
	 * The stream is positioned at the beginning of the given split.
	 * <p>
	 * The stream is actually opened in an asynchronous thread to make sure any interruptions to the thread 
	 * working on the input format do not reach the file system.
	 */
	@Override
	public void open(FileInputSplit fileSplit) throws IOException {

		this.currentSplit = fileSplit;
		this.splitStart = fileSplit.getStart();
		this.splitLength = fileSplit.getLength();

		if (LOG.isDebugEnabled()) {
			LOG.debug("Opening input split " + fileSplit.getPath() + " [" + this.splitStart + "," + this.splitLength + "]");
		}

		
		// open the split in an asynchronous thread
		final InputSplitOpenThread isot = new InputSplitOpenThread(fileSplit, this.openTimeout);
		isot.start();
		
		try {
			this.stream = isot.waitForCompletion();
			this.stream = decorateInputStream(this.stream, fileSplit);
		}
		catch (Throwable t) {
			throw new IOException("Error opening the Input Split " + fileSplit.getPath() + 
					" [" + splitStart + "," + splitLength + "]: " + t.getMessage(), t);
		}
		
		// get FSDataInputStream
		if (this.splitStart != 0) {
			this.stream.seek(this.splitStart);
		}
	}

```

### DelimitedInputFormat.initBuffers
- 默认的readBuffer大小为1M,wrapBuffer 为256 byte
- 当前切片默认值设置

    ```
    	this.readPos = 0;
    		this.limit = 0;
    		this.overLimit = false;
    		this.end = false;
    ```

```
private void initBuffers() {
		this.bufferSize = this.bufferSize <= 0 ? DEFAULT_READ_BUFFER_SIZE : this.bufferSize;

		if (this.bufferSize <= this.delimiter.length) {
			throw new IllegalArgumentException("Buffer size must be greater than length of delimiter.");
		}

		if (this.readBuffer == null || this.readBuffer.length != this.bufferSize) {
			this.readBuffer = new byte[this.bufferSize];
		}
		if (this.wrapBuffer == null || this.wrapBuffer.length < 256) {
			this.wrapBuffer = new byte[256];
		}

		this.readPos = 0;
		this.limit = 0;
		this.overLimit = false;
		this.end = false;
	}
```




### DelimitedInputFormat.fillBuffer()
- 读到数据到缓存中，如果数据大于1m就读1m的数据，如果小于1m，就把当前切片的数据全部读完
```
0 = 10
1 = 98
2 = 32
3 = 99
```

```
/**
	 * Fills the read buffer with bytes read from the file starting from an offset.
	 */
	private boolean fillBuffer(int offset) throws IOException {
		int maxReadLength = this.readBuffer.length - offset;
		// special case for reading the whole split.
		if (this.splitLength == FileInputFormat.READ_WHOLE_SPLIT_FLAG) {
			int read = this.stream.read(this.readBuffer, offset, maxReadLength);
			if (read == -1) {
				this.stream.close();
				this.stream = null;
				return false;
			} else {
				this.readPos = offset;
				this.limit = read;
				return true;
			}
		}
		
		// else ..
		int toRead;
		if (this.splitLength > 0) {
			// if we have more data, read that
			toRead = this.splitLength > maxReadLength ? maxReadLength : (int) this.splitLength;
		}
		else {
			// if we have exhausted our split, we need to complete the current record, or read one
			// more across the next split.
			// the reason is that the next split will skip over the beginning until it finds the first
			// delimiter, discarding it as an incomplete chunk of data that belongs to the last record in the
			// previous split.
			toRead = maxReadLength;
			this.overLimit = true;
		}

		int read = this.stream.read(this.readBuffer, offset, toRead);

		if (read == -1) {
			this.stream.close();
			this.stream = null;
			return false;
		} else {
			this.splitLength -= read;
			this.readPos = offset; // position from where to start reading
			this.limit = read + offset; // number of valid bytes in the read buffer
			return true;
		}
	}
```



### DelimitedInputFormat.nextRecord
- 读取一行数据,从当前分片，DelimitedInputFormat.open()已重新计算当前切片的开始位置
- 调用DelimitedInputFormat.readLine() 读取当前切片的一行数据

```
public OT nextRecord(OT record) throws IOException {
		if (readLine()) {
			return readRecord(record, this.currBuffer, this.currOffset, this.currLen);
		} else {
			this.end = true;
			return null;
		}
	}
```

### DelimitedInputFormat.readLine() 
- DelimitedInputFormat.open()已重新计算当前切片的开始位置,但是切片的长度不变，还是读取以前计算的长度
- 读到第一个换行符的数据，即读一行数据，如果没有换行符，当读取到当前切片最大长度
 
```
			// Search for next occurrence of delimiter in read buffer.
			while (this.readPos < this.limit && delimPos < this.delimiter.length) {
				if ((this.readBuffer[this.readPos]) == this.delimiter[delimPos]) {
					// Found the expected delimiter character. Continue looking for the next character of delimiter.
					delimPos++;
				} else {
					// Delimiter does not match.
					// We have to reset the read position to the character after the first matching character
					//   and search for the whole delimiter again.
					readPos -= delimPos;
					delimPos = 0;
				}
				readPos++;
			}
```
- 从缓存区readBuffer复制当前行数据到 wrapBuffer

    ```
    System.arraycopy(this.readBuffer, startPos, this.wrapBuffer, countInWrapBuffer, bytesToMove);
    ```
- 如果有换行符，需要删除换行符，在readBuffer
 
    ```
    // move delimiter chars to the beginning of the readBuffer
    				System.arraycopy(this.readBuffer, this.readPos - delimPos, this.readBuffer, 0, delimPos);
    ```

- 当前切片需要读取的个数this.limit，读取完后,继续读一个1m的数据到缓存中,最后将当前行数据返回setResult
- fillBuffer函数中，如果当前切处数据读完了，会设置overLimit = true,读下一行数据时就不满足条件就不会读了


```
if (this.readPos >= this.limit) {
				// readBuffer is completely consumed. Fill it again but keep partially read delimiter bytes.
				if (!fillBuffer(delimPos)) {
					int countInReadBuffer = delimPos;
					if (countInWrapBuffer + countInReadBuffer > 0) {
						// we have bytes left to emit
						if (countInReadBuffer > 0) {
							// we have bytes left in the readBuffer. Move them into the wrapBuffer
							if (this.wrapBuffer.length - countInWrapBuffer < countInReadBuffer) {
								// reallocate
								byte[] tmp = new byte[countInWrapBuffer + countInReadBuffer];
								System.arraycopy(this.wrapBuffer, 0, tmp, 0, countInWrapBuffer);
								this.wrapBuffer = tmp;
							}

							// copy readBuffer bytes to wrapBuffer
							System.arraycopy(this.readBuffer, 0, this.wrapBuffer, countInWrapBuffer, countInReadBuffer);
							countInWrapBuffer += countInReadBuffer;
						}

						this.offset += countInWrapBuffer;
						setResult(this.wrapBuffer, 0, countInWrapBuffer);
						return true;
					} else {
						return false;
					}
				}
			}

```
- 第一次读到的数据为
```
b c
```

```
protected final boolean readLine() throws IOException {
		if (this.stream == null || this.overLimit) {
			return false;
		}

		int countInWrapBuffer = 0;

		// position of matching positions in the delimiter byte array
		int delimPos = 0;

		while (true) {
			if (this.readPos >= this.limit) {
				// readBuffer is completely consumed. Fill it again but keep partially read delimiter bytes.
				if (!fillBuffer(delimPos)) {
					int countInReadBuffer = delimPos;
					if (countInWrapBuffer + countInReadBuffer > 0) {
						// we have bytes left to emit
						if (countInReadBuffer > 0) {
							// we have bytes left in the readBuffer. Move them into the wrapBuffer
							if (this.wrapBuffer.length - countInWrapBuffer < countInReadBuffer) {
								// reallocate
								byte[] tmp = new byte[countInWrapBuffer + countInReadBuffer];
								System.arraycopy(this.wrapBuffer, 0, tmp, 0, countInWrapBuffer);
								this.wrapBuffer = tmp;
							}

							// copy readBuffer bytes to wrapBuffer
							System.arraycopy(this.readBuffer, 0, this.wrapBuffer, countInWrapBuffer, countInReadBuffer);
							countInWrapBuffer += countInReadBuffer;
						}

						this.offset += countInWrapBuffer;
						setResult(this.wrapBuffer, 0, countInWrapBuffer);
						return true;
					} else {
						return false;
					}
				}
			}

			int startPos = this.readPos - delimPos;
			int count;

			// Search for next occurrence of delimiter in read buffer.
			while (this.readPos < this.limit && delimPos < this.delimiter.length) {
				if ((this.readBuffer[this.readPos]) == this.delimiter[delimPos]) {
					// Found the expected delimiter character. Continue looking for the next character of delimiter.
					delimPos++;
				} else {
					// Delimiter does not match.
					// We have to reset the read position to the character after the first matching character
					//   and search for the whole delimiter again.
					readPos -= delimPos;
					delimPos = 0;
				}
				readPos++;
			}

			// check why we dropped out
			if (delimPos == this.delimiter.length) {
				// we found a delimiter
				int readBufferBytesRead = this.readPos - startPos;
				this.offset += countInWrapBuffer + readBufferBytesRead;
				count = readBufferBytesRead - this.delimiter.length;

				// copy to byte array
				if (countInWrapBuffer > 0) {
					// check wrap buffer size
					if (this.wrapBuffer.length < countInWrapBuffer + count) {
						final byte[] nb = new byte[countInWrapBuffer + count];
						System.arraycopy(this.wrapBuffer, 0, nb, 0, countInWrapBuffer);
						this.wrapBuffer = nb;
					}
					if (count >= 0) {
						System.arraycopy(this.readBuffer, 0, this.wrapBuffer, countInWrapBuffer, count);
					}
					setResult(this.wrapBuffer, 0, countInWrapBuffer + count);
					return true;
				} else {
					setResult(this.readBuffer, startPos, count);
					return true;
				}
			} else {
				// we reached the end of the readBuffer
				count = this.limit - startPos;
				
				// check against the maximum record length
				if (((long) countInWrapBuffer) + count > this.lineLengthLimit) {
					throw new IOException("The record length exceeded the maximum record length (" + 
							this.lineLengthLimit + ").");
				}

				// Compute number of bytes to move to wrapBuffer
				// Chars of partially read delimiter must remain in the readBuffer. We might need to go back.
				int bytesToMove = count - delimPos;
				// ensure wrapBuffer is large enough
				if (this.wrapBuffer.length - countInWrapBuffer < bytesToMove) {
					// reallocate
					byte[] tmp = new byte[Math.max(this.wrapBuffer.length * 2, countInWrapBuffer + bytesToMove)];
					System.arraycopy(this.wrapBuffer, 0, tmp, 0, countInWrapBuffer);
					this.wrapBuffer = tmp;
				}

				// copy readBuffer to wrapBuffer (except delimiter chars)
				System.arraycopy(this.readBuffer, startPos, this.wrapBuffer, countInWrapBuffer, bytesToMove);
				countInWrapBuffer += bytesToMove;
				// move delimiter chars to the beginning of the readBuffer
				System.arraycopy(this.readBuffer, this.readPos - delimPos, this.readBuffer, 0, delimPos);

			}
		}
	}
```
end