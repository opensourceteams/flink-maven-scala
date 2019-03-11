# Flink1.7.2  Dataset local 源码分析


## 源码
- https://github.com/opensourceteams/flink-maven-scala

## 输入数据
```aidl
c a a
b c a

```
## 程序

### WordCount.scala

```aidl
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
    env.setParallelism(1)

    val dataSet = env.readTextFile("file:/opt/n_001_workspaces/bigdata/flink/flink-maven-scala-2/src/main/resources/data/line.txt")


    import org.apache.flink.streaming.api.scala._
    val result = dataSet.flatMap(x => x.split(" ")).map((_,1)).groupBy(0).sum(1)



    result.print()




  }

}

```

## 源码分析

## DataSet
### DataSet.print
- 该函数调用collect()函数

```
/**
	 * Prints the elements in a DataSet to the standard output stream {@link System#out} of the JVM that calls
	 * the print() method. For programs that are executed in a cluster, this method needs
	 * to gather the contents of the DataSet back to the client, to print it there.
	 *
	 * <p>The string written for each element is defined by the {@link Object#toString()} method.
	 *
	 * <p>This method immediately triggers the program execution, similar to the
	 * {@link #collect()} and {@link #count()} methods.
	 *
	 * @see #printToErr()
	 * @see #printOnTaskManager(String)
	 */
	public void print() throws Exception {
		List<T> elements = collect();
		for (T e: elements) {
			System.out.println(e);
		}
	}
```

### DataSet.collect()
- getExecutionEnvironment().execute() 得到作业执行环境,本地模式会开启：Flink Mini Cluster started successfully
- 调用MiniCluster.executeJobBlocking

```
/**
	 * Convenience method to get the elements of a DataSet as a List.
	 * As DataSet can contain a lot of data, this method should be used with caution.
	 *
	 * @return A List containing the elements of the DataSet
	 */
	public List<T> collect() throws Exception {
		final String id = new AbstractID().toString();
		final TypeSerializer<T> serializer = getType().createSerializer(getExecutionEnvironment().getConfig());

		this.output(new Utils.CollectHelper<>(id, serializer)).name("collect()");
		JobExecutionResult res = getExecutionEnvironment().execute();

		ArrayList<byte[]> accResult = res.getAccumulatorResult(id);
		if (accResult != null) {
			try {
				return SerializedListAccumulator.deserializeList(accResult, serializer);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException("Cannot find type class of collected data type.", e);
			} catch (IOException e) {
				throw new RuntimeException("Serialization error while deserializing collected data", e);
			}
		} else {
			throw new RuntimeException("The call to collect() could not retrieve the DataSet.");
		}
	}
```

## MiniCluster
### MiniCluster.executeJobBlocking
- 该函数会调用submitJob(job)函数
-  jobResultFuture.get() //block 函数，等待作业执行完成后，返回作业执行结果信息

```
/**
	 * This method runs a job in blocking mode. The method returns only after the job
	 * completed successfully, or after it failed terminally.
	 *
	 * @param job  The Flink job to execute
	 * @return The result of the job execution
	 *
	 * @throws JobExecutionException Thrown if anything went amiss during initial job launch,
	 *         or if the job terminally failed.
	 */
	@Override
	public JobExecutionResult executeJobBlocking(JobGraph job) throws JobExecutionException, InterruptedException {
		checkNotNull(job, "job is null");

		final CompletableFuture<JobSubmissionResult> submissionFuture = submitJob(job);

		final CompletableFuture<JobResult> jobResultFuture = submissionFuture.thenCompose(
			(JobSubmissionResult ignored) -> requestJobResult(job.getJobID()));

		final JobResult jobResult;

		try {
			jobResult = jobResultFuture.get();
		} catch (ExecutionException e) {
			throw new JobExecutionException(job.getJobID(), "Could not retrieve JobResult.", ExceptionUtils.stripExecutionException(e));
		}

		try {
			return jobResult.toJobExecutionResult(Thread.currentThread().getContextClassLoader());
		} catch (IOException | ClassNotFoundException e) {
			throw new JobExecutionException(job.getJobID(), e);
		}
	}


```

### MiniCluster.submitJob

```
public CompletableFuture<JobSubmissionResult> submitJob(JobGraph jobGraph) {
		final DispatcherGateway dispatcherGateway;
		try {
			dispatcherGateway = getDispatcherGateway();
		} catch (LeaderRetrievalException | InterruptedException e) {
			ExceptionUtils.checkInterrupted(e);
			return FutureUtils.completedExceptionally(e);
		}

		// we have to allow queued scheduling in Flip-6 mode because we need to request slots
		// from the ResourceManager
		jobGraph.setAllowQueuedScheduling(true);

		final CompletableFuture<InetSocketAddress> blobServerAddressFuture = createBlobServerAddress(dispatcherGateway);

		final CompletableFuture<Void> jarUploadFuture = uploadAndSetJobFiles(blobServerAddressFuture, jobGraph);

		final CompletableFuture<Acknowledge> acknowledgeCompletableFuture = jarUploadFuture.thenCompose(
			(Void ack) -> dispatcherGateway.submitJob(jobGraph, rpcTimeout));

		return acknowledgeCompletableFuture.thenApply(
			(Acknowledge ignored) -> new JobSubmissionResult(jobGraph.getJobID()));
	}
```




## Dispatcher
### Dispatcher.submitJob
- 输出提交作日志 INFO

    ```
    Submitting job 889ff625d756a41bb84cdddb441a2778 (Flink Java Job at Mon Mar 11 14:55:38 CST 2019).
    ```
- 调用函数 Dispatcher.persistAndRunJob
    
```
public CompletableFuture<Acknowledge> submitJob(JobGraph jobGraph, Time timeout) {
		final JobID jobId = jobGraph.getJobID();

		log.info("Submitting job {} ({}).", jobId, jobGraph.getName());
		final RunningJobsRegistry.JobSchedulingStatus jobSchedulingStatus;

		try {
			jobSchedulingStatus = runningJobsRegistry.getJobSchedulingStatus(jobId);
		} catch (IOException e) {
			return FutureUtils.completedExceptionally(new FlinkException(String.format("Failed to retrieve job scheduling status for job %s.", jobId), e));
		}

		if (jobSchedulingStatus == RunningJobsRegistry.JobSchedulingStatus.DONE || jobManagerRunnerFutures.containsKey(jobId)) {
			return FutureUtils.completedExceptionally(
				new JobSubmissionException(jobId, String.format("Job has already been submitted and is in state %s.", jobSchedulingStatus)));
		} else {
			final CompletableFuture<Acknowledge> persistAndRunFuture = waitForTerminatingJobManager(jobId, jobGraph, this::persistAndRunJob)
				.thenApply(ignored -> Acknowledge.get());

			return persistAndRunFuture.exceptionally(
				(Throwable throwable) -> {
					final Throwable strippedThrowable = ExceptionUtils.stripCompletionException(throwable);
					log.error("Failed to submit job {}.", jobId, strippedThrowable);
					throw new CompletionException(
						new JobSubmissionException(jobId, "Failed to submit job.", strippedThrowable));
				});
		}
	}
```

### Dispatcher.persistAndRunJob
- 调用Dispatcher.runJob

```
private CompletableFuture<Void> persistAndRunJob(JobGraph jobGraph) throws Exception {
		submittedJobGraphStore.putJobGraph(new SubmittedJobGraph(jobGraph, null));

		final CompletableFuture<Void> runJobFuture = runJob(jobGraph);

		return runJobFuture.whenComplete(BiConsumerWithException.unchecked((Object ignored, Throwable throwable) -> {
			if (throwable != null) {
				submittedJobGraphStore.removeJobGraph(jobGraph.getJobID());
			}
		}));
	}

```

### Dispatcher.runJob
- 调用 Dispatcher.createJobManagerRunner

```
private CompletableFuture<Void> runJob(JobGraph jobGraph) {
		Preconditions.checkState(!jobManagerRunnerFutures.containsKey(jobGraph.getJobID()));

		final CompletableFuture<JobManagerRunner> jobManagerRunnerFuture = createJobManagerRunner(jobGraph);

		jobManagerRunnerFutures.put(jobGraph.getJobID(), jobManagerRunnerFuture);

		return jobManagerRunnerFuture
			.thenApply(FunctionUtils.nullFn())
			.whenCompleteAsync(
				(ignored, throwable) -> {
					if (throwable != null) {
						jobManagerRunnerFutures.remove(jobGraph.getJobID());
					}
				},
				getMainThreadExecutor());
	}
```

### Dispatcher.createJobManagerRunner
- 调用jobManagerRunnerFactory.createJobManagerRunner
- 创建executorGrapth之后，会调用函数Dispatcher.startJobManagerRunner

```
private CompletableFuture<JobManagerRunner> createJobManagerRunner(JobGraph jobGraph) {
		final RpcService rpcService = getRpcService();

		final CompletableFuture<JobManagerRunner> jobManagerRunnerFuture = CompletableFuture.supplyAsync(
			CheckedSupplier.unchecked(() ->
				jobManagerRunnerFactory.createJobManagerRunner(
					ResourceID.generate(),
					jobGraph,
					configuration,
					rpcService,
					highAvailabilityServices,
					heartbeatServices,
					blobServer,
					jobManagerSharedServices,
					new DefaultJobManagerJobMetricGroupFactory(jobManagerMetricGroup),
					fatalErrorHandler)),
			rpcService.getExecutor());

		return jobManagerRunnerFuture.thenApply(FunctionUtils.uncheckedFunction(this::startJobManagerRunner));
	}
```


## JobManagerRunner
### jobManagerRunnerFactory.createJobManagerRunner
- 调用new JobManagerRunner

```
	/**
	 * Singleton default factory for {@link JobManagerRunner}.
	 */
	public enum DefaultJobManagerRunnerFactory implements JobManagerRunnerFactory {
		INSTANCE;

		@Override
		public JobManagerRunner createJobManagerRunner(
				ResourceID resourceId,
				JobGraph jobGraph,
				Configuration configuration,
				RpcService rpcService,
				HighAvailabilityServices highAvailabilityServices,
				HeartbeatServices heartbeatServices,
				BlobServer blobServer,
				JobManagerSharedServices jobManagerServices,
				JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory,
				FatalErrorHandler fatalErrorHandler) throws Exception {
			return new JobManagerRunner(
				resourceId,
				jobGraph,
				configuration,
				rpcService,
				highAvailabilityServices,
				heartbeatServices,
				blobServer,
				jobManagerServices,
				jobManagerJobMetricGroupFactory,
				fatalErrorHandler);
		}
	}

```

### new JobManagerRunner
- 调用 new JobMaster

##  JobMaster
### JobMaster
- 构建 executionGraph

    ```
    this.executionGraph = createAndRestoreExecutionGraph(jobManagerJobMetricGroup);
    ```

```
public JobMaster(
			RpcService rpcService,
			JobMasterConfiguration jobMasterConfiguration,
			ResourceID resourceId,
			JobGraph jobGraph,
			HighAvailabilityServices highAvailabilityService,
			SlotPoolFactory slotPoolFactory,
			JobManagerSharedServices jobManagerSharedServices,
			HeartbeatServices heartbeatServices,
			BlobServer blobServer,
			JobManagerJobMetricGroupFactory jobMetricGroupFactory,
			OnCompletionActions jobCompletionActions,
			FatalErrorHandler fatalErrorHandler,
			ClassLoader userCodeLoader) throws Exception {

		super(rpcService, AkkaRpcServiceUtils.createRandomName(JOB_MANAGER_NAME));

		final JobMasterGateway selfGateway = getSelfGateway(JobMasterGateway.class);

		this.jobMasterConfiguration = checkNotNull(jobMasterConfiguration);
		this.resourceId = checkNotNull(resourceId);
		this.jobGraph = checkNotNull(jobGraph);
		this.rpcTimeout = jobMasterConfiguration.getRpcTimeout();
		this.highAvailabilityServices = checkNotNull(highAvailabilityService);
		this.blobServer = checkNotNull(blobServer);
		this.scheduledExecutorService = jobManagerSharedServices.getScheduledExecutorService();
		this.jobCompletionActions = checkNotNull(jobCompletionActions);
		this.fatalErrorHandler = checkNotNull(fatalErrorHandler);
		this.userCodeLoader = checkNotNull(userCodeLoader);
		this.jobMetricGroupFactory = checkNotNull(jobMetricGroupFactory);

		this.taskManagerHeartbeatManager = heartbeatServices.createHeartbeatManagerSender(
			resourceId,
			new TaskManagerHeartbeatListener(selfGateway),
			rpcService.getScheduledExecutor(),
			log);

		this.resourceManagerHeartbeatManager = heartbeatServices.createHeartbeatManager(
				resourceId,
				new ResourceManagerHeartbeatListener(),
				rpcService.getScheduledExecutor(),
				log);

		final String jobName = jobGraph.getName();
		final JobID jid = jobGraph.getJobID();

		log.info("Initializing job {} ({}).", jobName, jid);

		final RestartStrategies.RestartStrategyConfiguration restartStrategyConfiguration =
				jobGraph.getSerializedExecutionConfig()
						.deserializeValue(userCodeLoader)
						.getRestartStrategy();

		this.restartStrategy = RestartStrategyResolving.resolve(restartStrategyConfiguration,
			jobManagerSharedServices.getRestartStrategyFactory(),
			jobGraph.isCheckpointingEnabled());

		log.info("Using restart strategy {} for {} ({}).", this.restartStrategy, jobName, jid);

		resourceManagerLeaderRetriever = highAvailabilityServices.getResourceManagerLeaderRetriever();

		this.slotPool = checkNotNull(slotPoolFactory).createSlotPool(jobGraph.getJobID());

		this.slotPoolGateway = slotPool.getSelfGateway(SlotPoolGateway.class);

		this.registeredTaskManagers = new HashMap<>(4);

		this.backPressureStatsTracker = checkNotNull(jobManagerSharedServices.getBackPressureStatsTracker());
		this.lastInternalSavepoint = null;

		this.jobManagerJobMetricGroup = jobMetricGroupFactory.create(jobGraph);
		this.executionGraph = createAndRestoreExecutionGraph(jobManagerJobMetricGroup);
		this.jobStatusListener = null;

		this.resourceManagerConnection = null;
		this.establishedResourceManagerConnection = null;
	}
```

## Dispatcher
### Dispatcher.startJobManagerRunner
- 调用JobManagerRunner.start() 
```
private JobManagerRunner startJobManagerRunner(JobManagerRunner jobManagerRunner) throws Exception {
		final JobID jobId = jobManagerRunner.getJobGraph().getJobID();
		jobManagerRunner.getResultFuture().whenCompleteAsync(
			(ArchivedExecutionGraph archivedExecutionGraph, Throwable throwable) -> {
				// check if we are still the active JobManagerRunner by checking the identity
				//noinspection ObjectEquality
				if (jobManagerRunner == jobManagerRunnerFutures.get(jobId).getNow(null)) {
					if (archivedExecutionGraph != null) {
						jobReachedGloballyTerminalState(archivedExecutionGraph);
					} else {
						final Throwable strippedThrowable = ExceptionUtils.stripCompletionException(throwable);

						if (strippedThrowable instanceof JobNotFinishedException) {
							jobNotFinished(jobId);
						} else {
							jobMasterFailed(jobId, strippedThrowable);
						}
					}
				} else {
					log.debug("There is a newer JobManagerRunner for the job {}.", jobId);
				}
			}, getMainThreadExecutor());

		jobManagerRunner.start();

		return jobManagerRunner;
	}
```


## JobManagerRunner
### JobManagerRunner.verifyJobSchedulingStatusAndStartJobManager
- 调用JobMaster.start()
```
private void verifyJobSchedulingStatusAndStartJobManager(UUID leaderSessionId) throws Exception {
		final JobSchedulingStatus jobSchedulingStatus = runningJobsRegistry.getJobSchedulingStatus(jobGraph.getJobID());

		if (jobSchedulingStatus == JobSchedulingStatus.DONE) {
			log.info("Granted leader ship but job {} has been finished. ", jobGraph.getJobID());
			jobFinishedByOther();
		} else {
			log.info("JobManager runner for job {} ({}) was granted leadership with session id {} at {}.",
				jobGraph.getName(), jobGraph.getJobID(), leaderSessionId, getAddress());

			runningJobsRegistry.setJobRunning(jobGraph.getJobID());

			final CompletableFuture<Acknowledge> startFuture = jobMaster.start(new JobMasterId(leaderSessionId), rpcTimeout);
			final CompletableFuture<JobMasterGateway> currentLeaderGatewayFuture = leaderGatewayFuture;

			startFuture.whenCompleteAsync(
				(Acknowledge ack, Throwable throwable) -> {
					if (throwable != null) {
						handleJobManagerRunnerError(new FlinkException("Could not start the job manager.", throwable));
					} else {
						confirmLeaderSessionIdIfStillLeader(leaderSessionId, currentLeaderGatewayFuture);
					}
				},
				jobManagerSharedServices.getScheduledExecutorService());
		}
	}

```

## JobMaster
### JobMaster.start()
- 调用JobMaster.startJobExecution() 调用该函数去启动jobExection
```
	/**
	 * Start the rpc service and begin to run the job.
	 *
	 * @param newJobMasterId The necessary fencing token to run the job
	 * @param timeout for the operation
	 * @return Future acknowledge if the job could be started. Otherwise the future contains an exception
	 */
	public CompletableFuture<Acknowledge> start(final JobMasterId newJobMasterId, final Time timeout) throws Exception {
		// make sure we receive RPC and async calls
		super.start();

		return callAsyncWithoutFencing(() -> startJobExecution(newJobMasterId), timeout);
	}

```

### JobMaster.startJobExecution
- 调用函数resetAndScheduleExecutionGraph

```
//-- job starting and stopping  -----------------------------------------------------------------

	private Acknowledge startJobExecution(JobMasterId newJobMasterId) throws Exception {
		validateRunsInMainThread();

		checkNotNull(newJobMasterId, "The new JobMasterId must not be null.");

		if (Objects.equals(getFencingToken(), newJobMasterId)) {
			log.info("Already started the job execution with JobMasterId {}.", newJobMasterId);

			return Acknowledge.get();
		}

		setNewFencingToken(newJobMasterId);

		startJobMasterServices();

		log.info("Starting execution of job {} ({})", jobGraph.getName(), jobGraph.getJobID());

		resetAndScheduleExecutionGraph();

		return Acknowledge.get();
	}
```

### JobMaster.resetAndScheduleExecutionGraph
- 调用函数 JobMaster.scheduleExecutionGraph
```
private void resetAndScheduleExecutionGraph() throws Exception {
		validateRunsInMainThread();

		final CompletableFuture<Void> executionGraphAssignedFuture;

		if (executionGraph.getState() == JobStatus.CREATED) {
			executionGraphAssignedFuture = CompletableFuture.completedFuture(null);
		} else {
			suspendAndClearExecutionGraphFields(new FlinkException("ExecutionGraph is being reset in order to be rescheduled."));
			final JobManagerJobMetricGroup newJobManagerJobMetricGroup = jobMetricGroupFactory.create(jobGraph);
			final ExecutionGraph newExecutionGraph = createAndRestoreExecutionGraph(newJobManagerJobMetricGroup);

			executionGraphAssignedFuture = executionGraph.getTerminationFuture().handleAsync(
				(JobStatus ignored, Throwable throwable) -> {
					assignExecutionGraph(newExecutionGraph, newJobManagerJobMetricGroup);
					return null;
				},
				getMainThreadExecutor());
		}

		executionGraphAssignedFuture.thenRun(this::scheduleExecutionGraph);
	}
```
### JobMaster.scheduleExecutionGraph
- 调用ExecutionGraph.scheduleForExecution()
```
	private void scheduleExecutionGraph() {
		checkState(jobStatusListener == null);
		// register self as job status change listener
		jobStatusListener = new JobManagerJobStatusListener();
		executionGraph.registerJobStatusListener(jobStatusListener);

		try {
			executionGraph.scheduleForExecution();
		}
		catch (Throwable t) {
			executionGraph.failGlobal(t);
		}
	}
```

### ExecutionGraph.scheduleForExecution()
- DataSet 默认的调度模式为ScheduleMode.LAZY_FROM_SOURCES (可选LAZY_FROM_SOURCES,EAGER)
- 调用ExecutionGraph.scheduleLazy
```
public void scheduleForExecution() throws JobException {

		final long currentGlobalModVersion = globalModVersion;

		if (transitionState(JobStatus.CREATED, JobStatus.RUNNING)) {

			final CompletableFuture<Void> newSchedulingFuture;

			switch (scheduleMode) {

				case LAZY_FROM_SOURCES:
					newSchedulingFuture = scheduleLazy(slotProvider);
					break;

				case EAGER:
					newSchedulingFuture = scheduleEager(slotProvider, allocationTimeout);
					break;

				default:
					throw new JobException("Schedule mode is invalid.");
			}

			if (state == JobStatus.RUNNING && currentGlobalModVersion == globalModVersion) {
				schedulingFuture = newSchedulingFuture;

				newSchedulingFuture.whenCompleteAsync(
					(Void ignored, Throwable throwable) -> {
						if (throwable != null && !(throwable instanceof CancellationException)) {
							// only fail if the scheduling future was not canceled
							failGlobal(ExceptionUtils.stripCompletionException(throwable));
						}
					},
					futureExecutor);
			} else {
				newSchedulingFuture.cancel(false);
			}
		}
		else {
			throw new IllegalStateException("Job may only be scheduled from state " + JobStatus.CREATED);
		}
	}
```


### ScheduleMode
```
/**
 * The ScheduleMode decides how tasks of an execution graph are started.  
 */
public enum ScheduleMode {

	/** Schedule tasks lazily from the sources. Downstream tasks are started once their input data are ready */
	LAZY_FROM_SOURCES,

	/** Schedules all tasks immediately. */
	EAGER;
	
	/**
	 * Returns whether we are allowed to deploy consumers lazily.
	 */
	public boolean allowLazyDeployment() {
		return this == LAZY_FROM_SOURCES;
	}
	
}

```


### ExecutionGraph.scheduleLazy
- verticesInCreationOrder 为所有的ExecutionJobVertex,此案例共有三个()
    - Source读取源数据，进行函数链操作，并且会进行本地合并
    ```
    jobVertex = {InputFormatVertex@7675} "CHAIN DataSource (at com.opensourceteams.module.bigdata.flink.example.dataset.worldcount.WordCountRun$.main(WordCountRun.scala:19) (org.apache.flink.api.java.io.TextInp) -> FlatMap (FlatMap at com.opensourceteams.module.bigdata.flink.example.dataset.worldcount.WordCountRun$.main(WordCountRun.scala:23)) -> Map (Map at com.opensourceteams.module.bigdata.flink.example.dataset.worldcount.WordCountRun$.main(WordCountRun.scala:23)) -> Combine (SUM(1)) (org.apache.flink.runtime.operators.DataSourceTask)"
    ```

    - Batch 批处理，对各任务执行结果进行汇总
    ```
      jobVertex = {JobVertex@7695} "Reduce (SUM(1)) (org.apache.flink.runtime.operators.BatchTask)"
    ```
    - Sink 数据输出
    ```
     jobVertex = {OutputFormatVertex@7716} "DataSink (collect()) (org.apache.flink.runtime.operators.DataSinkTask)"
    ```

    ```
    
    org.apache.flink.runtime.executiongraph.ExecutionGraph.scheduleLazy
    
    verticesInCreationOrder = {ArrayList@7268}  size = 3
     0 = {ExecutionJobVertex@7609} 
     1 = {ExecutionJobVertex@7672} 
     2 = {ExecutionJobVertex@7673} 
     
     
     
     verticesInCreationOrder = {ArrayList@7268}  size = 3
     0 = {ExecutionJobVertex@7609} 
      stateMonitor = {Object@7674} 
      graph = {ExecutionGraph@6569} 
      jobVertex = {InputFormatVertex@7675} "CHAIN DataSource (at com.opensourceteams.module.bigdata.flink.example.dataset.worldcount.WordCountRun$.main(WordCountRun.scala:19) (org.apache.flink.api.java.io.TextInp) -> FlatMap (FlatMap at com.opensourceteams.module.bigdata.flink.example.dataset.worldcount.WordCountRun$.main(WordCountRun.scala:23)) -> Map (Map at com.opensourceteams.module.bigdata.flink.example.dataset.worldcount.WordCountRun$.main(WordCountRun.scala:23)) -> Combine (SUM(1)) (org.apache.flink.runtime.operators.DataSourceTask)"
      operatorIDs = {Collections$UnmodifiableRandomAccessList@7676}  size = 1
      userDefinedOperatorIds = {Collections$UnmodifiableRandomAccessList@7677}  size = 1
      taskVertices = {ExecutionVertex[1]@7678} 
      producedDataSets = {IntermediateResult[1]@7680} 
      inputs = {ArrayList@7682}  size = 0
      parallelism = 1
      slotSharingGroup = {SlotSharingGroup@7683} "SlotSharingGroup [0ee9fb1bd06c876f81051811f01eb46d, 5f8e90970a256ab2a713eff5617101a6, e1cc45ed6109bbdbe1a262f83b152ab6]"
      coLocationGroup = null
      inputSplits = {FileInputSplit[1]@7684} 
      maxParallelismConfigured = true
      maxParallelism = 1
      serializedTaskInformation = null
      taskInformationBlobKey = null
      taskInformationOrBlobKey = null
      splitAssigner = {LocatableInputSplitAssigner@7686} 
     1 = {ExecutionJobVertex@7672} 
      stateMonitor = {Object@7694} 
      graph = {ExecutionGraph@6569} 
      jobVertex = {JobVertex@7695} "Reduce (SUM(1)) (org.apache.flink.runtime.operators.BatchTask)"
      operatorIDs = {Collections$UnmodifiableRandomAccessList@7696}  size = 1
      userDefinedOperatorIds = {Collections$UnmodifiableRandomAccessList@7697}  size = 1
      taskVertices = {ExecutionVertex[1]@7698} 
      producedDataSets = {IntermediateResult[1]@7699} 
      inputs = {ArrayList@7700}  size = 1
      parallelism = 1
      slotSharingGroup = {SlotSharingGroup@7683} "SlotSharingGroup [0ee9fb1bd06c876f81051811f01eb46d, 5f8e90970a256ab2a713eff5617101a6, e1cc45ed6109bbdbe1a262f83b152ab6]"
      coLocationGroup = null
      inputSplits = null
      maxParallelismConfigured = true
      maxParallelism = 1
      serializedTaskInformation = null
      taskInformationBlobKey = null
      taskInformationOrBlobKey = null
      splitAssigner = null
     2 = {ExecutionJobVertex@7673} 
      stateMonitor = {Object@7715} 
      graph = {ExecutionGraph@6569} 
      jobVertex = {OutputFormatVertex@7716} "DataSink (collect()) (org.apache.flink.runtime.operators.DataSinkTask)"
      operatorIDs = {Collections$UnmodifiableRandomAccessList@7717}  size = 1
      userDefinedOperatorIds = {Collections$UnmodifiableRandomAccessList@7718}  size = 1
      taskVertices = {ExecutionVertex[1]@7719} 
      producedDataSets = {IntermediateResult[0]@7720} 
      inputs = {ArrayList@7721}  size = 1
      parallelism = 1
      slotSharingGroup = {SlotSharingGroup@7683} "SlotSharingGroup [0ee9fb1bd06c876f81051811f01eb46d, 5f8e90970a256ab2a713eff5617101a6, e1cc45ed6109bbdbe1a262f83b152ab6]"
      coLocationGroup = null
      inputSplits = null
      maxParallelismConfigured = true
      maxParallelism = 1
      serializedTaskInformation = null
      taskInformationBlobKey = null
      taskInformationOrBlobKey = null
      splitAssigner = null
    numVerticesTotal = 3

    ```
    
- 触发 ExecutionJobVertex.scheduleAll    

```
private CompletableFuture<Void> scheduleLazy(SlotProvider slotProvider) {

		final ArrayList<CompletableFuture<Void>> schedulingFutures = new ArrayList<>(numVerticesTotal);
		// simply take the vertices without inputs.
		for (ExecutionJobVertex ejv : verticesInCreationOrder) {
			if (ejv.getJobVertex().isInputVertex()) {
				final CompletableFuture<Void> schedulingJobVertexFuture = ejv.scheduleAll(
					slotProvider,
					allowQueuedScheduling,
					LocationPreferenceConstraint.ALL, // since it is an input vertex, the input based location preferences should be empty
					Collections.emptySet());

				schedulingFutures.add(schedulingJobVertexFuture);
			}
		}

		return FutureUtils.waitForAll(schedulingFutures);
	}

```

## ExecutionJobVertex
### ExecutionJobVertex.scheduleAll    
- 先处理第一个执行边，因为边与边之间是有一个依赖关系
 ```
 CHAIN DataSource (at main(Run.java:20) (org.apache.flink.api.java.io.TextInputFormat)) -> FlatMap (FlatMap at main(Run.java:23)) -> Combine (SUM(1), at main(Run.java:33) (1/1)
 ```
- 调用FutureUtils.waitForAll(scheduleFutures) 会触发该边的任务调用 , ExecutionVertex.scheduleForExecution
- 以上执行完成后，会触发调用第一个 Execution,转成的任务，即DataSourceTask
```
/**
	 * Schedules all execution vertices of this ExecutionJobVertex.
	 *
	 * @param slotProvider to allocate the slots from
	 * @param queued if the allocations can be queued
	 * @param locationPreferenceConstraint constraint for the location preferences
	 * @param allPreviousExecutionGraphAllocationIds set with all previous allocation ids in the job graph.
	 *                                                 Can be empty if the allocation ids are not required for scheduling.
	 * @return Future which is completed once all {@link Execution} could be deployed
	 */
	public CompletableFuture<Void> scheduleAll(
			SlotProvider slotProvider,
			boolean queued,
			LocationPreferenceConstraint locationPreferenceConstraint,
			@Nonnull Set<AllocationID> allPreviousExecutionGraphAllocationIds) {

		final ExecutionVertex[] vertices = this.taskVertices;

		final ArrayList<CompletableFuture<Void>> scheduleFutures = new ArrayList<>(vertices.length);

		// kick off the tasks
		for (ExecutionVertex ev : vertices) {
			scheduleFutures.add(ev.scheduleForExecution(
				slotProvider,
				queued,
				locationPreferenceConstraint,
				allPreviousExecutionGraphAllocationIds));
		}

		return FutureUtils.waitForAll(scheduleFutures);
	}

```

## ExecutionVertex
### ExecutionVertex.scheduleForExecution
- 调用Execution.scheduleForExecution
```
/**
	 * Schedules the current execution of this ExecutionVertex.
	 *
	 * @param slotProvider to allocate the slots from
	 * @param queued if the allocation can be queued
	 * @param locationPreferenceConstraint constraint for the location preferences
	 * @param allPreviousExecutionGraphAllocationIds set with all previous allocation ids in the job graph.
	 *                                                 Can be empty if the allocation ids are not required for scheduling.
	 * @return Future which is completed once the execution is deployed. The future
	 * can also completed exceptionally.
	 */
	public CompletableFuture<Void> scheduleForExecution(
			SlotProvider slotProvider,
			boolean queued,
			LocationPreferenceConstraint locationPreferenceConstraint,
			@Nonnull Set<AllocationID> allPreviousExecutionGraphAllocationIds) {
		return this.currentExecution.scheduleForExecution(
			slotProvider,
			queued,
			locationPreferenceConstraint,
			allPreviousExecutionGraphAllocationIds);
	}
```



### Execution.scheduleForExecution
- 调用Execution.allocateAndAssignSlotForExecution
- 调用Execution.deploy() 去部署当前Execution

```
/**
	 * NOTE: This method only throws exceptions if it is in an illegal state to be scheduled, or if the tasks needs
	 *       to be scheduled immediately and no resource is available. If the task is accepted by the schedule, any
	 *       error sets the vertex state to failed and triggers the recovery logic.
	 *
	 * @param slotProvider The slot provider to use to allocate slot for this execution attempt.
	 * @param queued Flag to indicate whether the scheduler may queue this task if it cannot
	 *               immediately deploy it.
	 * @param locationPreferenceConstraint constraint for the location preferences
	 * @param allPreviousExecutionGraphAllocationIds set with all previous allocation ids in the job graph.
	 *                                                 Can be empty if the allocation ids are not required for scheduling.
	 * @return Future which is completed once the Execution has been deployed
	 */
	public CompletableFuture<Void> scheduleForExecution(
			SlotProvider slotProvider,
			boolean queued,
			LocationPreferenceConstraint locationPreferenceConstraint,
			@Nonnull Set<AllocationID> allPreviousExecutionGraphAllocationIds) {
		final Time allocationTimeout = vertex.getExecutionGraph().getAllocationTimeout();
		try {
			final CompletableFuture<Execution> allocationFuture = allocateAndAssignSlotForExecution(
				slotProvider,
				queued,
				locationPreferenceConstraint,
				allPreviousExecutionGraphAllocationIds,
				allocationTimeout);

			// IMPORTANT: We have to use the synchronous handle operation (direct executor) here so
			// that we directly deploy the tasks if the slot allocation future is completed. This is
			// necessary for immediate deployment.
			final CompletableFuture<Void> deploymentFuture = allocationFuture.thenAccept(
				(FutureConsumerWithException<Execution, Exception>) value -> deploy());

			deploymentFuture.whenComplete(
				(Void ignored, Throwable failure) -> {
					if (failure != null) {
						final Throwable stripCompletionException = ExceptionUtils.stripCompletionException(failure);
						final Throwable schedulingFailureCause;

						if (stripCompletionException instanceof TimeoutException) {
							schedulingFailureCause = new NoResourceAvailableException(
								"Could not allocate enough slots within timeout of " + allocationTimeout + " to run the job. " +
									"Please make sure that the cluster has enough resources.");
						} else {
							schedulingFailureCause = stripCompletionException;
						}

						markFailed(schedulingFailureCause);
					}
				});

			// if tasks have to scheduled immediately check that the task has been deployed
			if (!queued && !deploymentFuture.isDone()) {
				deploymentFuture.completeExceptionally(new IllegalArgumentException("The slot allocation future has not been completed yet."));
			}

			return deploymentFuture;
		} catch (IllegalExecutionStateException e) {
			return FutureUtils.completedExceptionally(e);
		}
	}

```

### Execution.deploy
- 部署Execution
- 提交任务 TaskExecutor.submitTask()

```
/**
	 * Deploys the execution to the previously assigned resource.
	 *
	 * @throws JobException if the execution cannot be deployed to the assigned resource
	 */
	public void deploy() throws JobException {
		final LogicalSlot slot  = assignedResource;

		checkNotNull(slot, "In order to deploy the execution we first have to assign a resource via tryAssignResource.");

		// Check if the TaskManager died in the meantime
		// This only speeds up the response to TaskManagers failing concurrently to deployments.
		// The more general check is the rpcTimeout of the deployment call
		if (!slot.isAlive()) {
			throw new JobException("Target slot (TaskManager) for deployment is no longer alive.");
		}

		// make sure exactly one deployment call happens from the correct state
		// note: the transition from CREATED to DEPLOYING is for testing purposes only
		ExecutionState previous = this.state;
		if (previous == SCHEDULED || previous == CREATED) {
			if (!transitionState(previous, DEPLOYING)) {
				// race condition, someone else beat us to the deploying call.
				// this should actually not happen and indicates a race somewhere else
				throw new IllegalStateException("Cannot deploy task: Concurrent deployment call race.");
			}
		}
		else {
			// vertex may have been cancelled, or it was already scheduled
			throw new IllegalStateException("The vertex must be in CREATED or SCHEDULED state to be deployed. Found state " + previous);
		}

		if (this != slot.getPayload()) {
			throw new IllegalStateException(
				String.format("The execution %s has not been assigned to the assigned slot.", this));
		}

		try {

			// race double check, did we fail/cancel and do we need to release the slot?
			if (this.state != DEPLOYING) {
				slot.releaseSlot(new FlinkException("Actual state of execution " + this + " (" + state + ") does not match expected state DEPLOYING."));
				return;
			}

			if (LOG.isInfoEnabled()) {
				LOG.info(String.format("Deploying %s (attempt #%d) to %s", vertex.getTaskNameWithSubtaskIndex(),
						attemptNumber, getAssignedResourceLocation().getHostname()));
			}

			final TaskDeploymentDescriptor deployment = vertex.createDeploymentDescriptor(
				attemptId,
				slot,
				taskRestore,
				attemptNumber);

			// null taskRestore to let it be GC'ed
			taskRestore = null;

			final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();

			final CompletableFuture<Acknowledge> submitResultFuture = taskManagerGateway.submitTask(deployment, rpcTimeout);

			submitResultFuture.whenCompleteAsync(
				(ack, failure) -> {
					// only respond to the failure case
					if (failure != null) {
						if (failure instanceof TimeoutException) {
							String taskname = vertex.getTaskNameWithSubtaskIndex() + " (" + attemptId + ')';

							markFailed(new Exception(
								"Cannot deploy task " + taskname + " - TaskManager (" + getAssignedResourceLocation()
									+ ") not responding after a rpcTimeout of " + rpcTimeout, failure));
						} else {
							markFailed(failure);
						}
					}
				},
				executor);
		}
		catch (Throwable t) {
			markFailed(t);
			ExceptionUtils.rethrow(t);
		}
	}

```
### TaskExecutor.submitTask
- 启动当前任务,调用 Task.run

    ```
task.startTaskThread();
    ```

```
// ----------------------------------------------------------------------
	// Task lifecycle RPCs
	// ----------------------------------------------------------------------

	@Override
	public CompletableFuture<Acknowledge> submitTask(
			TaskDeploymentDescriptor tdd,
			JobMasterId jobMasterId,
			Time timeout) {

		try {
			final JobID jobId = tdd.getJobId();
			final JobManagerConnection jobManagerConnection = jobManagerTable.get(jobId);

			if (jobManagerConnection == null) {
				final String message = "Could not submit task because there is no JobManager " +
					"associated for the job " + jobId + '.';

				log.debug(message);
				throw new TaskSubmissionException(message);
			}

			if (!Objects.equals(jobManagerConnection.getJobMasterId(), jobMasterId)) {
				final String message = "Rejecting the task submission because the job manager leader id " +
					jobMasterId + " does not match the expected job manager leader id " +
					jobManagerConnection.getJobMasterId() + '.';

				log.debug(message);
				throw new TaskSubmissionException(message);
			}

			if (!taskSlotTable.tryMarkSlotActive(jobId, tdd.getAllocationId())) {
				final String message = "No task slot allocated for job ID " + jobId +
					" and allocation ID " + tdd.getAllocationId() + '.';
				log.debug(message);
				throw new TaskSubmissionException(message);
			}

			// re-integrate offloaded data:
			try {
				tdd.loadBigData(blobCacheService.getPermanentBlobService());
			} catch (IOException | ClassNotFoundException e) {
				throw new TaskSubmissionException("Could not re-integrate offloaded TaskDeploymentDescriptor data.", e);
			}

			// deserialize the pre-serialized information
			final JobInformation jobInformation;
			final TaskInformation taskInformation;
			try {
				jobInformation = tdd.getSerializedJobInformation().deserializeValue(getClass().getClassLoader());
				taskInformation = tdd.getSerializedTaskInformation().deserializeValue(getClass().getClassLoader());
			} catch (IOException | ClassNotFoundException e) {
				throw new TaskSubmissionException("Could not deserialize the job or task information.", e);
			}

			if (!jobId.equals(jobInformation.getJobId())) {
				throw new TaskSubmissionException(
					"Inconsistent job ID information inside TaskDeploymentDescriptor (" +
						tdd.getJobId() + " vs. " + jobInformation.getJobId() + ")");
			}

			TaskMetricGroup taskMetricGroup = taskManagerMetricGroup.addTaskForJob(
				jobInformation.getJobId(),
				jobInformation.getJobName(),
				taskInformation.getJobVertexId(),
				tdd.getExecutionAttemptId(),
				taskInformation.getTaskName(),
				tdd.getSubtaskIndex(),
				tdd.getAttemptNumber());

			InputSplitProvider inputSplitProvider = new RpcInputSplitProvider(
				jobManagerConnection.getJobManagerGateway(),
				taskInformation.getJobVertexId(),
				tdd.getExecutionAttemptId(),
				taskManagerConfiguration.getTimeout());

			TaskManagerActions taskManagerActions = jobManagerConnection.getTaskManagerActions();
			CheckpointResponder checkpointResponder = jobManagerConnection.getCheckpointResponder();

			LibraryCacheManager libraryCache = jobManagerConnection.getLibraryCacheManager();
			ResultPartitionConsumableNotifier resultPartitionConsumableNotifier = jobManagerConnection.getResultPartitionConsumableNotifier();
			PartitionProducerStateChecker partitionStateChecker = jobManagerConnection.getPartitionStateChecker();

			final TaskLocalStateStore localStateStore = localStateStoresManager.localStateStoreForSubtask(
				jobId,
				tdd.getAllocationId(),
				taskInformation.getJobVertexId(),
				tdd.getSubtaskIndex());

			final JobManagerTaskRestore taskRestore = tdd.getTaskRestore();

			final TaskStateManager taskStateManager = new TaskStateManagerImpl(
				jobId,
				tdd.getExecutionAttemptId(),
				localStateStore,
				taskRestore,
				checkpointResponder);

			Task task = new Task(
				jobInformation,
				taskInformation,
				tdd.getExecutionAttemptId(),
				tdd.getAllocationId(),
				tdd.getSubtaskIndex(),
				tdd.getAttemptNumber(),
				tdd.getProducedPartitions(),
				tdd.getInputGates(),
				tdd.getTargetSlotNumber(),
				taskExecutorServices.getMemoryManager(),
				taskExecutorServices.getIOManager(),
				taskExecutorServices.getNetworkEnvironment(),
				taskExecutorServices.getBroadcastVariableManager(),
				taskStateManager,
				taskManagerActions,
				inputSplitProvider,
				checkpointResponder,
				blobCacheService,
				libraryCache,
				fileCache,
				taskManagerConfiguration,
				taskMetricGroup,
				resultPartitionConsumableNotifier,
				partitionStateChecker,
				getRpcService().getExecutor());

			log.info("Received task {}.", task.getTaskInfo().getTaskNameWithSubtasks());

			boolean taskAdded;

			try {
				taskAdded = taskSlotTable.addTask(task);
			} catch (SlotNotFoundException | SlotNotActiveException e) {
				throw new TaskSubmissionException("Could not submit task.", e);
			}

			if (taskAdded) {
				task.startTaskThread();

				return CompletableFuture.completedFuture(Acknowledge.get());
			} else {
				final String message = "TaskManager already contains a task for id " +
					task.getExecutionId() + '.';

				log.debug(message);
				throw new TaskSubmissionException(message);
			}
		} catch (TaskSubmissionException e) {
			return FutureUtils.completedExceptionally(e);
		}
	}
```

### Task.run

```
/**
	 * The core work method that bootstraps the task and executes its code.
	 */
	@Override
	public void run() {

		// ----------------------------
		//  Initial State transition
		// ----------------------------
		while (true) {
			ExecutionState current = this.executionState;
			if (current == ExecutionState.CREATED) {
				if (transitionState(ExecutionState.CREATED, ExecutionState.DEPLOYING)) {
					// success, we can start our work
					break;
				}
			}
			else if (current == ExecutionState.FAILED) {
				// we were immediately failed. tell the TaskManager that we reached our final state
				notifyFinalState();
				if (metrics != null) {
					metrics.close();
				}
				return;
			}
			else if (current == ExecutionState.CANCELING) {
				if (transitionState(ExecutionState.CANCELING, ExecutionState.CANCELED)) {
					// we were immediately canceled. tell the TaskManager that we reached our final state
					notifyFinalState();
					if (metrics != null) {
						metrics.close();
					}
					return;
				}
			}
			else {
				if (metrics != null) {
					metrics.close();
				}
				throw new IllegalStateException("Invalid state for beginning of operation of task " + this + '.');
			}
		}

		// all resource acquisitions and registrations from here on
		// need to be undone in the end
		Map<String, Future<Path>> distributedCacheEntries = new HashMap<>();
		AbstractInvokable invokable = null;

		try {
			// ----------------------------
			//  Task Bootstrap - We periodically
			//  check for canceling as a shortcut
			// ----------------------------

			// activate safety net for task thread
			LOG.info("Creating FileSystem stream leak safety net for task {}", this);
			FileSystemSafetyNet.initializeSafetyNetForThread();

			blobService.getPermanentBlobService().registerJob(jobId);

			// first of all, get a user-code classloader
			// this may involve downloading the job's JAR files and/or classes
			LOG.info("Loading JAR files for task {}.", this);

			userCodeClassLoader = createUserCodeClassloader();
			final ExecutionConfig executionConfig = serializedExecutionConfig.deserializeValue(userCodeClassLoader);

			if (executionConfig.getTaskCancellationInterval() >= 0) {
				// override task cancellation interval from Flink config if set in ExecutionConfig
				taskCancellationInterval = executionConfig.getTaskCancellationInterval();
			}

			if (executionConfig.getTaskCancellationTimeout() >= 0) {
				// override task cancellation timeout from Flink config if set in ExecutionConfig
				taskCancellationTimeout = executionConfig.getTaskCancellationTimeout();
			}

			if (isCanceledOrFailed()) {
				throw new CancelTaskException();
			}

			// ----------------------------------------------------------------
			// register the task with the network stack
			// this operation may fail if the system does not have enough
			// memory to run the necessary data exchanges
			// the registration must also strictly be undone
			// ----------------------------------------------------------------

			LOG.info("Registering task at network: {}.", this);

			network.registerTask(this);

			// add metrics for buffers
			this.metrics.getIOMetricGroup().initializeBufferMetrics(this);

			// register detailed network metrics, if configured
			if (taskManagerConfig.getConfiguration().getBoolean(TaskManagerOptions.NETWORK_DETAILED_METRICS)) {
				// similar to MetricUtils.instantiateNetworkMetrics() but inside this IOMetricGroup
				MetricGroup networkGroup = this.metrics.getIOMetricGroup().addGroup("Network");
				MetricGroup outputGroup = networkGroup.addGroup("Output");
				MetricGroup inputGroup = networkGroup.addGroup("Input");

				// output metrics
				for (int i = 0; i < producedPartitions.length; i++) {
					ResultPartitionMetrics.registerQueueLengthMetrics(
						outputGroup.addGroup(i), producedPartitions[i]);
				}

				for (int i = 0; i < inputGates.length; i++) {
					InputGateMetrics.registerQueueLengthMetrics(
						inputGroup.addGroup(i), inputGates[i]);
				}
			}

			// next, kick off the background copying of files for the distributed cache
			try {
				for (Map.Entry<String, DistributedCache.DistributedCacheEntry> entry :
						DistributedCache.readFileInfoFromConfig(jobConfiguration)) {
					LOG.info("Obtaining local cache file for '{}'.", entry.getKey());
					Future<Path> cp = fileCache.createTmpFile(entry.getKey(), entry.getValue(), jobId, executionId);
					distributedCacheEntries.put(entry.getKey(), cp);
				}
			}
			catch (Exception e) {
				throw new Exception(
					String.format("Exception while adding files to distributed cache of task %s (%s).", taskNameWithSubtask, executionId), e);
			}

			if (isCanceledOrFailed()) {
				throw new CancelTaskException();
			}

			// ----------------------------------------------------------------
			//  call the user code initialization methods
			// ----------------------------------------------------------------

			TaskKvStateRegistry kvStateRegistry = network.createKvStateTaskRegistry(jobId, getJobVertexId());

			Environment env = new RuntimeEnvironment(
				jobId,
				vertexId,
				executionId,
				executionConfig,
				taskInfo,
				jobConfiguration,
				taskConfiguration,
				userCodeClassLoader,
				memoryManager,
				ioManager,
				broadcastVariableManager,
				taskStateManager,
				accumulatorRegistry,
				kvStateRegistry,
				inputSplitProvider,
				distributedCacheEntries,
				producedPartitions,
				inputGates,
				network.getTaskEventDispatcher(),
				checkpointResponder,
				taskManagerConfig,
				metrics,
				this);

			// now load and instantiate the task's invokable code
			invokable = loadAndInstantiateInvokable(userCodeClassLoader, nameOfInvokableClass, env);

			// ----------------------------------------------------------------
			//  actual task core work
			// ----------------------------------------------------------------

			// we must make strictly sure that the invokable is accessible to the cancel() call
			// by the time we switched to running.
			this.invokable = invokable;

			// switch to the RUNNING state, if that fails, we have been canceled/failed in the meantime
			if (!transitionState(ExecutionState.DEPLOYING, ExecutionState.RUNNING)) {
				throw new CancelTaskException();
			}

			// notify everyone that we switched to running
			taskManagerActions.updateTaskExecutionState(new TaskExecutionState(jobId, executionId, ExecutionState.RUNNING));

			// make sure the user code classloader is accessible thread-locally
			executingThread.setContextClassLoader(userCodeClassLoader);

			// run the invokable
			invokable.invoke();

			// make sure, we enter the catch block if the task leaves the invoke() method due
			// to the fact that it has been canceled
			if (isCanceledOrFailed()) {
				throw new CancelTaskException();
			}

			// ----------------------------------------------------------------
			//  finalization of a successful execution
			// ----------------------------------------------------------------

			// finish the produced partitions. if this fails, we consider the execution failed.
			for (ResultPartition partition : producedPartitions) {
				if (partition != null) {
					partition.finish();
				}
			}

			// try to mark the task as finished
			// if that fails, the task was canceled/failed in the meantime
			if (!transitionState(ExecutionState.RUNNING, ExecutionState.FINISHED)) {
				throw new CancelTaskException();
			}
		}
		catch (Throwable t) {

			// unwrap wrapped exceptions to make stack traces more compact
			if (t instanceof WrappingRuntimeException) {
				t = ((WrappingRuntimeException) t).unwrap();
			}

			// ----------------------------------------------------------------
			// the execution failed. either the invokable code properly failed, or
			// an exception was thrown as a side effect of cancelling
			// ----------------------------------------------------------------

			try {
				// check if the exception is unrecoverable
				if (ExceptionUtils.isJvmFatalError(t) ||
						(t instanceof OutOfMemoryError && taskManagerConfig.shouldExitJvmOnOutOfMemoryError())) {

					// terminate the JVM immediately
					// don't attempt a clean shutdown, because we cannot expect the clean shutdown to complete
					try {
						LOG.error("Encountered fatal error {} - terminating the JVM", t.getClass().getName(), t);
					} finally {
						Runtime.getRuntime().halt(-1);
					}
				}

				// transition into our final state. we should be either in DEPLOYING, RUNNING, CANCELING, or FAILED
				// loop for multiple retries during concurrent state changes via calls to cancel() or
				// to failExternally()
				while (true) {
					ExecutionState current = this.executionState;

					if (current == ExecutionState.RUNNING || current == ExecutionState.DEPLOYING) {
						if (t instanceof CancelTaskException) {
							if (transitionState(current, ExecutionState.CANCELED)) {
								cancelInvokable(invokable);
								break;
							}
						}
						else {
							if (transitionState(current, ExecutionState.FAILED, t)) {
								// proper failure of the task. record the exception as the root cause
								failureCause = t;
								cancelInvokable(invokable);

								break;
							}
						}
					}
					else if (current == ExecutionState.CANCELING) {
						if (transitionState(current, ExecutionState.CANCELED)) {
							break;
						}
					}
					else if (current == ExecutionState.FAILED) {
						// in state failed already, no transition necessary any more
						break;
					}
					// unexpected state, go to failed
					else if (transitionState(current, ExecutionState.FAILED, t)) {
						LOG.error("Unexpected state in task {} ({}) during an exception: {}.", taskNameWithSubtask, executionId, current);
						break;
					}
					// else fall through the loop and
				}
			}
			catch (Throwable tt) {
				String message = String.format("FATAL - exception in exception handler of task %s (%s).", taskNameWithSubtask, executionId);
				LOG.error(message, tt);
				notifyFatalError(message, tt);
			}
		}
		finally {
			try {
				LOG.info("Freeing task resources for {} ({}).", taskNameWithSubtask, executionId);

				// clear the reference to the invokable. this helps guard against holding references
				// to the invokable and its structures in cases where this Task object is still referenced
				this.invokable = null;

				// stop the async dispatcher.
				// copy dispatcher reference to stack, against concurrent release
				ExecutorService dispatcher = this.asyncCallDispatcher;
				if (dispatcher != null && !dispatcher.isShutdown()) {
					dispatcher.shutdownNow();
				}

				// free the network resources
				network.unregisterTask(this);

				// free memory resources
				if (invokable != null) {
					memoryManager.releaseAll(invokable);
				}

				// remove all of the tasks library resources
				libraryCache.unregisterTask(jobId, executionId);
				fileCache.releaseJob(jobId, executionId);
				blobService.getPermanentBlobService().releaseJob(jobId);

				// close and de-activate safety net for task thread
				LOG.info("Ensuring all FileSystem streams are closed for task {}", this);
				FileSystemSafetyNet.closeSafetyNetAndGuardedResourcesForThread();

				notifyFinalState();
			}
			catch (Throwable t) {
				// an error in the resource cleanup is fatal
				String message = String.format("FATAL - exception in resource cleanup of task %s (%s).", taskNameWithSubtask, executionId);
				LOG.error(message, t);
				notifyFatalError(message, t);
			}

			// un-register the metrics at the end so that the task may already be
			// counted as finished when this happens
			// errors here will only be logged
			try {
				metrics.close();
			}
			catch (Throwable t) {
				LOG.error("Error during metrics de-registration of task {} ({}).", taskNameWithSubtask, executionId, t);
			}
		}
	}

```

### Execution.allocateAndAssignSlotForExecution
- 调用 transitionState(CREATED, SCHEDULED)) 把当前Execution状态由已创建更新为已调度
- 计算优选位置
 
    ```
  // calculate the preferred locations
			final CompletableFuture<Collection<TaskManagerLocation>> preferredLocationsFuture =
				calculatePreferredLocations(locationPreferenceConstraint);
    ```

```
/**
	 * Allocates and assigns a slot obtained from the slot provider to the execution.
	 *
	 * @param slotProvider to obtain a new slot from
	 * @param queued if the allocation can be queued
	 * @param locationPreferenceConstraint constraint for the location preferences
	 * @param allPreviousExecutionGraphAllocationIds set with all previous allocation ids in the job graph.
	 *                                                 Can be empty if the allocation ids are not required for scheduling.
	 * @param allocationTimeout rpcTimeout for allocating a new slot
	 * @return Future which is completed with this execution once the slot has been assigned
	 * 			or with an exception if an error occurred.
	 * @throws IllegalExecutionStateException if this method has been called while not being in the CREATED state
	 */
	public CompletableFuture<Execution> allocateAndAssignSlotForExecution(
			SlotProvider slotProvider,
			boolean queued,
			LocationPreferenceConstraint locationPreferenceConstraint,
			@Nonnull Set<AllocationID> allPreviousExecutionGraphAllocationIds,
			Time allocationTimeout) throws IllegalExecutionStateException {

		checkNotNull(slotProvider);

		final SlotSharingGroup sharingGroup = vertex.getJobVertex().getSlotSharingGroup();
		final CoLocationConstraint locationConstraint = vertex.getLocationConstraint();

		// sanity check
		if (locationConstraint != null && sharingGroup == null) {
			throw new IllegalStateException(
					"Trying to schedule with co-location constraint but without slot sharing allowed.");
		}

		// this method only works if the execution is in the state 'CREATED'
		if (transitionState(CREATED, SCHEDULED)) {

			final SlotSharingGroupId slotSharingGroupId = sharingGroup != null ? sharingGroup.getSlotSharingGroupId() : null;

			ScheduledUnit toSchedule = locationConstraint == null ?
					new ScheduledUnit(this, slotSharingGroupId) :
					new ScheduledUnit(this, slotSharingGroupId, locationConstraint);

			// try to extract previous allocation ids, if applicable, so that we can reschedule to the same slot
			ExecutionVertex executionVertex = getVertex();
			AllocationID lastAllocation = executionVertex.getLatestPriorAllocation();

			Collection<AllocationID> previousAllocationIDs =
				lastAllocation != null ? Collections.singletonList(lastAllocation) : Collections.emptyList();

			// calculate the preferred locations
			final CompletableFuture<Collection<TaskManagerLocation>> preferredLocationsFuture =
				calculatePreferredLocations(locationPreferenceConstraint);

			final SlotRequestId slotRequestId = new SlotRequestId();

			final CompletableFuture<LogicalSlot> logicalSlotFuture = preferredLocationsFuture
				.thenCompose(
					(Collection<TaskManagerLocation> preferredLocations) ->
						slotProvider.allocateSlot(
							slotRequestId,
							toSchedule,
							queued,
							new SlotProfile(
								ResourceProfile.UNKNOWN,
								preferredLocations,
								previousAllocationIDs,
								allPreviousExecutionGraphAllocationIds),
							allocationTimeout));

			// register call back to cancel slot request in case that the execution gets canceled
			releaseFuture.whenComplete(
				(Object ignored, Throwable throwable) -> {
					if (logicalSlotFuture.cancel(false)) {
						slotProvider.cancelSlotRequest(
							slotRequestId,
							slotSharingGroupId,
							new FlinkException("Execution " + this + " was released."));
					}
				});

			return logicalSlotFuture.thenApply(
				(LogicalSlot logicalSlot) -> {
					if (tryAssignResource(logicalSlot)) {
						return this;
					} else {
						// release the slot
						logicalSlot.releaseSlot(new FlinkException("Could not assign logical slot to execution " + this + '.'));

						throw new CompletionException(new FlinkException("Could not assign slot " + logicalSlot + " to execution " + this + " because it has already been assigned "));
					}
				});
		}
		else {
			// call race, already deployed, or already done
			throw new IllegalExecutionStateException(this, CREATED, state);
		}
	}
```

### ExecutionState
- Execution状态，和流程
- CREATED（已创建）  -> SCHEDULED(已调度) -> DEPLOYING(部署中) -> RUNNING(运行中) -> FINISHED(已完成) 等

```
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.execution;

/**
 * An enumeration of all states that a task can be in during its execution.
 * Tasks usually start in the state {@code CREATED} and switch states according to
 * this diagram:
 * <pre>{@code
 *
 *     CREATED  -> SCHEDULED -> DEPLOYING -> RUNNING -> FINISHED
 *        |            |            |          |
 *        |            |            |   +------+
 *        |            |            V   V
 *        |            |         CANCELLING -----+----> CANCELED
 *        |            |                         |
 *        |            +-------------------------+
 *        |
 *        |                                   ... -> FAILED
 *        V
 *    RECONCILING  -> RUNNING | FINISHED | CANCELED | FAILED
 *
 * }</pre>
 *
 * <p>It is possible to enter the {@code RECONCILING} state from {@code CREATED}
 * state if job manager fail over, and the {@code RECONCILING} state can switch into
 * any existing task state.
 *
 * <p>It is possible to enter the {@code FAILED} state from any other state.
 *
 * <p>The states {@code FINISHED}, {@code CANCELED}, and {@code FAILED} are
 * considered terminal states.
 */
public enum ExecutionState {

	CREATED,
	
	SCHEDULED,
	
	DEPLOYING,
	
	RUNNING,

	/**
	 * This state marks "successfully completed". It can only be reached when a
	 * program reaches the "end of its input". The "end of input" can be reached
	 * when consuming a bounded input (fix set of files, bounded query, etc) or
	 * when stopping a program (not cancelling!) which make the input look like
	 * it reached its end at a specific point.
	 */
	FINISHED,
	
	CANCELING,
	
	CANCELED,
	
	FAILED,

	RECONCILING;

	public boolean isTerminal() {
		return this == FINISHED || this == CANCELED || this == FAILED;
	}
}

```


## Task
### SourceTask
- 更新当前Execution任务状态从已创建为到已调度
 
    ```
transitionState(ExecutionState.CREATED, ExecutionState.DEPLOYING)
    ```
- 更新Execution状态从已部署到正在运行中
 
    ```
// notify everyone that we switched to running
			taskManagerActions.updateTaskExecutionState(new TaskExecutionState(jobId, executionId, ExecutionState.RUNNING));
    ```

- 调用 DataSourceTask.invokable
 
    ```
// run the invokable
			invokable.invoke();
    ```

```
/**
	 * The core work method that bootstraps the task and executes its code.
	 */
	@Override
	public void run() {

		// ----------------------------
		//  Initial State transition
		// ----------------------------
		while (true) {
			ExecutionState current = this.executionState;
			if (current == ExecutionState.CREATED) {
				if (transitionState(ExecutionState.CREATED, ExecutionState.DEPLOYING)) {
					// success, we can start our work
					break;
				}
			}
			else if (current == ExecutionState.FAILED) {
				// we were immediately failed. tell the TaskManager that we reached our final state
				notifyFinalState();
				if (metrics != null) {
					metrics.close();
				}
				return;
			}
			else if (current == ExecutionState.CANCELING) {
				if (transitionState(ExecutionState.CANCELING, ExecutionState.CANCELED)) {
					// we were immediately canceled. tell the TaskManager that we reached our final state
					notifyFinalState();
					if (metrics != null) {
						metrics.close();
					}
					return;
				}
			}
			else {
				if (metrics != null) {
					metrics.close();
				}
				throw new IllegalStateException("Invalid state for beginning of operation of task " + this + '.');
			}
		}

		// all resource acquisitions and registrations from here on
		// need to be undone in the end
		Map<String, Future<Path>> distributedCacheEntries = new HashMap<>();
		AbstractInvokable invokable = null;

		try {
			// ----------------------------
			//  Task Bootstrap - We periodically
			//  check for canceling as a shortcut
			// ----------------------------

			// activate safety net for task thread
			LOG.info("Creating FileSystem stream leak safety net for task {}", this);
			FileSystemSafetyNet.initializeSafetyNetForThread();

			blobService.getPermanentBlobService().registerJob(jobId);

			// first of all, get a user-code classloader
			// this may involve downloading the job's JAR files and/or classes
			LOG.info("Loading JAR files for task {}.", this);

			userCodeClassLoader = createUserCodeClassloader();
			final ExecutionConfig executionConfig = serializedExecutionConfig.deserializeValue(userCodeClassLoader);

			if (executionConfig.getTaskCancellationInterval() >= 0) {
				// override task cancellation interval from Flink config if set in ExecutionConfig
				taskCancellationInterval = executionConfig.getTaskCancellationInterval();
			}

			if (executionConfig.getTaskCancellationTimeout() >= 0) {
				// override task cancellation timeout from Flink config if set in ExecutionConfig
				taskCancellationTimeout = executionConfig.getTaskCancellationTimeout();
			}

			if (isCanceledOrFailed()) {
				throw new CancelTaskException();
			}

			// ----------------------------------------------------------------
			// register the task with the network stack
			// this operation may fail if the system does not have enough
			// memory to run the necessary data exchanges
			// the registration must also strictly be undone
			// ----------------------------------------------------------------

			LOG.info("Registering task at network: {}.", this);

			network.registerTask(this);

			// add metrics for buffers
			this.metrics.getIOMetricGroup().initializeBufferMetrics(this);

			// register detailed network metrics, if configured
			if (taskManagerConfig.getConfiguration().getBoolean(TaskManagerOptions.NETWORK_DETAILED_METRICS)) {
				// similar to MetricUtils.instantiateNetworkMetrics() but inside this IOMetricGroup
				MetricGroup networkGroup = this.metrics.getIOMetricGroup().addGroup("Network");
				MetricGroup outputGroup = networkGroup.addGroup("Output");
				MetricGroup inputGroup = networkGroup.addGroup("Input");

				// output metrics
				for (int i = 0; i < producedPartitions.length; i++) {
					ResultPartitionMetrics.registerQueueLengthMetrics(
						outputGroup.addGroup(i), producedPartitions[i]);
				}

				for (int i = 0; i < inputGates.length; i++) {
					InputGateMetrics.registerQueueLengthMetrics(
						inputGroup.addGroup(i), inputGates[i]);
				}
			}

			// next, kick off the background copying of files for the distributed cache
			try {
				for (Map.Entry<String, DistributedCache.DistributedCacheEntry> entry :
						DistributedCache.readFileInfoFromConfig(jobConfiguration)) {
					LOG.info("Obtaining local cache file for '{}'.", entry.getKey());
					Future<Path> cp = fileCache.createTmpFile(entry.getKey(), entry.getValue(), jobId, executionId);
					distributedCacheEntries.put(entry.getKey(), cp);
				}
			}
			catch (Exception e) {
				throw new Exception(
					String.format("Exception while adding files to distributed cache of task %s (%s).", taskNameWithSubtask, executionId), e);
			}

			if (isCanceledOrFailed()) {
				throw new CancelTaskException();
			}

			// ----------------------------------------------------------------
			//  call the user code initialization methods
			// ----------------------------------------------------------------

			TaskKvStateRegistry kvStateRegistry = network.createKvStateTaskRegistry(jobId, getJobVertexId());

			Environment env = new RuntimeEnvironment(
				jobId,
				vertexId,
				executionId,
				executionConfig,
				taskInfo,
				jobConfiguration,
				taskConfiguration,
				userCodeClassLoader,
				memoryManager,
				ioManager,
				broadcastVariableManager,
				taskStateManager,
				accumulatorRegistry,
				kvStateRegistry,
				inputSplitProvider,
				distributedCacheEntries,
				producedPartitions,
				inputGates,
				network.getTaskEventDispatcher(),
				checkpointResponder,
				taskManagerConfig,
				metrics,
				this);

			// now load and instantiate the task's invokable code
			invokable = loadAndInstantiateInvokable(userCodeClassLoader, nameOfInvokableClass, env);

			// ----------------------------------------------------------------
			//  actual task core work
			// ----------------------------------------------------------------

			// we must make strictly sure that the invokable is accessible to the cancel() call
			// by the time we switched to running.
			this.invokable = invokable;

			// switch to the RUNNING state, if that fails, we have been canceled/failed in the meantime
			if (!transitionState(ExecutionState.DEPLOYING, ExecutionState.RUNNING)) {
				throw new CancelTaskException();
			}

			// notify everyone that we switched to running
			taskManagerActions.updateTaskExecutionState(new TaskExecutionState(jobId, executionId, ExecutionState.RUNNING));

			// make sure the user code classloader is accessible thread-locally
			executingThread.setContextClassLoader(userCodeClassLoader);

			// run the invokable
			invokable.invoke();

			// make sure, we enter the catch block if the task leaves the invoke() method due
			// to the fact that it has been canceled
			if (isCanceledOrFailed()) {
				throw new CancelTaskException();
			}

			// ----------------------------------------------------------------
			//  finalization of a successful execution
			// ----------------------------------------------------------------

			// finish the produced partitions. if this fails, we consider the execution failed.
			for (ResultPartition partition : producedPartitions) {
				if (partition != null) {
					partition.finish();
				}
			}

			// try to mark the task as finished
			// if that fails, the task was canceled/failed in the meantime
			if (!transitionState(ExecutionState.RUNNING, ExecutionState.FINISHED)) {
				throw new CancelTaskException();
			}
		}
		catch (Throwable t) {

			// unwrap wrapped exceptions to make stack traces more compact
			if (t instanceof WrappingRuntimeException) {
				t = ((WrappingRuntimeException) t).unwrap();
			}

			// ----------------------------------------------------------------
			// the execution failed. either the invokable code properly failed, or
			// an exception was thrown as a side effect of cancelling
			// ----------------------------------------------------------------

			try {
				// check if the exception is unrecoverable
				if (ExceptionUtils.isJvmFatalError(t) ||
						(t instanceof OutOfMemoryError && taskManagerConfig.shouldExitJvmOnOutOfMemoryError())) {

					// terminate the JVM immediately
					// don't attempt a clean shutdown, because we cannot expect the clean shutdown to complete
					try {
						LOG.error("Encountered fatal error {} - terminating the JVM", t.getClass().getName(), t);
					} finally {
						Runtime.getRuntime().halt(-1);
					}
				}

				// transition into our final state. we should be either in DEPLOYING, RUNNING, CANCELING, or FAILED
				// loop for multiple retries during concurrent state changes via calls to cancel() or
				// to failExternally()
				while (true) {
					ExecutionState current = this.executionState;

					if (current == ExecutionState.RUNNING || current == ExecutionState.DEPLOYING) {
						if (t instanceof CancelTaskException) {
							if (transitionState(current, ExecutionState.CANCELED)) {
								cancelInvokable(invokable);
								break;
							}
						}
						else {
							if (transitionState(current, ExecutionState.FAILED, t)) {
								// proper failure of the task. record the exception as the root cause
								failureCause = t;
								cancelInvokable(invokable);

								break;
							}
						}
					}
					else if (current == ExecutionState.CANCELING) {
						if (transitionState(current, ExecutionState.CANCELED)) {
							break;
						}
					}
					else if (current == ExecutionState.FAILED) {
						// in state failed already, no transition necessary any more
						break;
					}
					// unexpected state, go to failed
					else if (transitionState(current, ExecutionState.FAILED, t)) {
						LOG.error("Unexpected state in task {} ({}) during an exception: {}.", taskNameWithSubtask, executionId, current);
						break;
					}
					// else fall through the loop and
				}
			}
			catch (Throwable tt) {
				String message = String.format("FATAL - exception in exception handler of task %s (%s).", taskNameWithSubtask, executionId);
				LOG.error(message, tt);
				notifyFatalError(message, tt);
			}
		}
		finally {
			try {
				LOG.info("Freeing task resources for {} ({}).", taskNameWithSubtask, executionId);

				// clear the reference to the invokable. this helps guard against holding references
				// to the invokable and its structures in cases where this Task object is still referenced
				this.invokable = null;

				// stop the async dispatcher.
				// copy dispatcher reference to stack, against concurrent release
				ExecutorService dispatcher = this.asyncCallDispatcher;
				if (dispatcher != null && !dispatcher.isShutdown()) {
					dispatcher.shutdownNow();
				}

				// free the network resources
				network.unregisterTask(this);

				// free memory resources
				if (invokable != null) {
					memoryManager.releaseAll(invokable);
				}

				// remove all of the tasks library resources
				libraryCache.unregisterTask(jobId, executionId);
				fileCache.releaseJob(jobId, executionId);
				blobService.getPermanentBlobService().releaseJob(jobId);

				// close and de-activate safety net for task thread
				LOG.info("Ensuring all FileSystem streams are closed for task {}", this);
				FileSystemSafetyNet.closeSafetyNetAndGuardedResourcesForThread();

				notifyFinalState();
			}
			catch (Throwable t) {
				// an error in the resource cleanup is fatal
				String message = String.format("FATAL - exception in resource cleanup of task %s (%s).", taskNameWithSubtask, executionId);
				LOG.error(message, t);
				notifyFatalError(message, t);
			}

			// un-register the metrics at the end so that the task may already be
			// counted as finished when this happens
			// errors here will only be logged
			try {
				metrics.close();
			}
			catch (Throwable t) {
				LOG.error("Error during metrics de-registration of task {} ({}).", taskNameWithSubtask, executionId, t);
			}
		}
	}

```
## DataSourceTask
### DataSourceTask.invokable
- 这里是真正开始处理数据源Source的地方
- chainedTasks(FlatMap操作链条，还有Commibine链条)
 
        ```
        this.chainedTasks = {ArrayList@14404}  size = 2
         0 = {ChainedFlatMapDriver@14403} 
         1 = {SynchronousChainedCombineDriver@14459} 

        ```
- 迭代文件中的每一行数据,调用TextInputFormat.readRecord读取一行数据(读取文件放到缓存里，再从缓存一行一行读取)，调用CoutingCollector.collect()处理这一行数据

    ```
    	// as long as there is data to read
						while (!this.taskCanceled && !format.reachedEnd()) {
							OT returned;
							if ((returned = format.nextRecord(serializer.createInstance())) != null) {
								output.collect(returned);
							}
						}
    ```
- 注意，这里有一ChainedFlatMapDriver.close()当source文件读取完成后，会调用close()进行合并了，当然，由于是Mini在Source都读取完成后调用，集群环境，应该是按分片去读取了，就相当于一个任务读一部分数据，多个任务处理(集群环境，下次专门在集群环境考考)
 
    ```
	// close the collector. if it is a chaining task collector, it will close its chained tasks
			this.output.close();

    ```
 
```
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


## CountingCollector
### CountingCollector.collect
- 调用 ChainedFlatMapDriver.collect
```

	public void collect(OUT record) {
		this.numRecordsOut.inc();
		this.collector.collect(record);
	}
```
### chaining.ChainedFlatMapDriver.collect
- 调用用户自定义flatMap函数处理这一行数据，每一条数据(a,1),(b,1),(a,1) 调用一次，CountingCollector.collect
```
	public void collect(IT record) {
		try {
			this.numRecordsIn.inc();
			this.mapper.flatMap(record, this.outputCollector);
		} catch (Exception ex) {
			throw new ExceptionInChainedStubException(this.taskName, ex);
		}
	}
```

### CountingCollector.collect
- 调用 SynchronousChainedCombineDriver.collect
```
	public void collect(OUT record) {
		this.numRecordsOut.inc();
		this.collector.collect(record);
	}
```
## SynchronousChainedCombineDriver
### SynchronousChainedCombineDriver.collect
- 把当前这一条记录放到序列化缓存中

    ```
    this.sorter.write(record)
    ```
- 调用 sort.NormalizedKeySorter.write进行数据写入
```
public void collect(IN record) {
		this.numRecordsIn.inc();
		// try writing to the sorter first
		try {
			if (this.sorter.write(record)) {
				return;
			}
		} catch (IOException e) {
			throw new ExceptionInChainedStubException(this.taskName, e);
		}

		// do the actual sorting
		try {
			sortAndCombine();
		} catch (Exception e) {
			throw new ExceptionInChainedStubException(this.taskName, e);
		}
		this.sorter.reset();

		try {
			if (!this.sorter.write(record)) {
				throw new IOException("Cannot write record to fresh sort buffer. Record too large.");
			}
		} catch (IOException e) {
			throw new ExceptionInChainedStubException(this.taskName, e);
		}
	}

```


### NormalizedKeySorter.write
- 序列化当前这条记录到缓存中
    ```
    this.serializer.serialize(record, this.recordCollector);
    ```
- 序列化当前这条记录的key到缓存中,key是可以去重的并且按一定规则，遍历key来进行按key进行合并(聚合)
    ```
    this.comparator.putNormalizedKey(record, this.currentSortIndexSegment, this.currentSortIndexOffset + OFFSET_LEN, this.numKeyBytes);
    ```    
```
	/**
	 * Writes a given record to this sort buffer. The written record will be appended and take
	 * the last logical position.
	 * 
	 * @param record The record to be written.
	 * @return True, if the record was successfully written, false, if the sort buffer was full.
	 * @throws IOException Thrown, if an error occurred while serializing the record into the buffers.
	 */
	@Override
	public boolean write(T record) throws IOException {
		//check whether we need a new memory segment for the sort index
		if (this.currentSortIndexOffset > this.lastIndexEntryOffset) {
			if (memoryAvailable()) {
				this.currentSortIndexSegment = nextMemorySegment();
				this.sortIndex.add(this.currentSortIndexSegment);
				this.currentSortIndexOffset = 0;
				this.sortIndexBytes += this.segmentSize;
			} else {
				return false;
			}
		}
		
		// serialize the record into the data buffers
		try {
			this.serializer.serialize(record, this.recordCollector);
		}
		catch (EOFException e) {
			return false;
		}
		
		final long newOffset = this.recordCollector.getCurrentOffset();
		final boolean shortRecord = newOffset - this.currentDataBufferOffset < LARGE_RECORD_THRESHOLD;
		
		if (!shortRecord && LOG.isDebugEnabled()) {
			LOG.debug("Put a large record ( >" + LARGE_RECORD_THRESHOLD + " into the sort buffer");
		}
		
		// add the pointer and the normalized key
		this.currentSortIndexSegment.putLong(this.currentSortIndexOffset, shortRecord ?
				this.currentDataBufferOffset : (this.currentDataBufferOffset | LARGE_RECORD_TAG));

		if (this.numKeyBytes != 0) {
			this.comparator.putNormalizedKey(record, this.currentSortIndexSegment, this.currentSortIndexOffset + OFFSET_LEN, this.numKeyBytes);
		}
		
		this.currentSortIndexOffset += this.indexEntrySize;
		this.currentDataBufferOffset = newOffset;
		this.numRecords++;
		return true;
	}
```

## ChainedFlatMapDriver
### ChainedFlatMapDriver.close
- 调用 chaining.ChainedDriver.close()

```
	public void close() {
		this.outputCollector.close();
	}
```
### ChainedDriver.close
- 调用CountingCollector.close()

```
	public void close() {
		this.collector.close();
	}
```

### CountingCollector.close()
- 调用 CountingCollector.sortAndCombine()
```
	public void close() {
		try {
			sortAndCombine();
		} catch (Exception e) {
			throw new ExceptionInChainedStubException(this.taskName, e);
		}

		this.outputCollector.close();
	}
```

### CountingCollector.sortAndCombine()
- 调用快速排序 this.sortAlgo.sort(sorter)
- key迭代器，key是去重的,用来对key进行本地合并操作

    ```
    final NonReusingKeyGroupedIterator<IN> keyIter = new NonReusingKeyGroupedIterator<IN>(sorter.getIterator(), this.groupingComparator);
    ```
- 迭代每一个key,进行合并操作,调用AggregatingUdf.combine函数

    ```
    			// run stub implementation
				while (this.running && keyIter.nextKey()) {
					stub.combine(keyIter.getValues(), output);
				}
    ```
    
```
private void sortAndCombine() throws Exception {
		final InMemorySorter<IN> sorter = this.sorter;

		if (objectReuseEnabled) {
			if (!sorter.isEmpty()) {
				this.sortAlgo.sort(sorter);
				// run the combiner
				final ReusingKeyGroupedIterator<IN> keyIter = new ReusingKeyGroupedIterator<IN>(sorter.getIterator(), this.serializer, this.groupingComparator);


				// cache references on the stack
				final GroupCombineFunction<IN, OUT> stub = this.combiner;
				final Collector<OUT> output = this.outputCollector;

				// run stub implementation
				while (this.running && keyIter.nextKey()) {
					stub.combine(keyIter.getValues(), output);
				}
			}
		} else {
			if (!sorter.isEmpty()) {
				this.sortAlgo.sort(sorter);
				// run the combiner
				final NonReusingKeyGroupedIterator<IN> keyIter = new NonReusingKeyGroupedIterator<IN>(sorter.getIterator(), this.groupingComparator);


				// cache references on the stack
				final GroupCombineFunction<IN, OUT> stub = this.combiner;
				final Collector<OUT> output = this.outputCollector;

				// run stub implementation
				while (this.running && keyIter.nextKey()) {
					stub.combine(keyIter.getValues(), output);
				}
			}
		}
	}

```


### AggregatingUdf.combine
- 调用reduce()函数
```
	public void combine(Iterable<T> records, Collector<T> out) {
			reduce(records, out);
		}
```

### AggregatingUdf.reduce
- 计算相同key的值合并,【这里是本地合并的地方】

    ```
    for (int i = 0; i < fieldPositions.length; i++) {
					Object val = record.getFieldNotNull(fieldPositions[i]);
					aggFunctions[i].aggregate(val);
				}
    ```
- 调用 CountingCollector.collect() 发送给下游
```
public void reduce(Iterable<T> records, Collector<T> out) {
			final AggregationFunction<Object>[] aggFunctions = this.aggFunctions;
			final int[] fieldPositions = this.fieldPositions;

			// aggregators are initialized from before

			T outT = null;
			for (T record : records) {
				outT = record;

				for (int i = 0; i < fieldPositions.length; i++) {
					Object val = record.getFieldNotNull(fieldPositions[i]);
					aggFunctions[i].aggregate(val);
				}
			}

			for (int i = 0; i < fieldPositions.length; i++) {
				Object aggVal = aggFunctions[i].getAggregate();
				outT.setField(aggVal, fieldPositions[i]);
				aggFunctions[i].initializeAggregate();
			}

			out.collect(outT);
		}


```


### CountingCollector.collect()
- 调用 OutputCollector.collect()

```
	public void collect(OUT record) {
		this.numRecordsOut.inc();
		this.collector.collect(record);
	}
```

### OutputCollector.collect()
- 发送消息 writer.emit(this.delegate);

```
/**
	 * Collects a record and emits it to all writers.
	 */
	@Override
	public void collect(T record)  {
		if (record != null) {
			this.delegate.setInstance(record);
			try {
				for (RecordWriter<SerializationDelegate<T>> writer : writers) {
					writer.emit(this.delegate);
				}
			}
			catch (IOException e) {
				throw new RuntimeException("Emitting the record caused an I/O exception: " + e.getMessage(), e);
			}
			catch (InterruptedException e) {
				throw new RuntimeException("Emitting the record was interrupted: " + e.getMessage(), e);
			}
		}
		else {
			throw new NullPointerException("The system does not support records that are null."
								+ "Null values are only supported as fields inside other objects.");
		}
	}
```

### RecordWriter.emit
- 通过hash按key分区 channelSelector.selectChannels(record, numChannels)

```
	public void emit(T record) throws IOException, InterruptedException {
		emit(record, channelSelector.selectChannels(record, numChannels));
	}
```


### RecordWriter.emit
- 将消息发给指定的下游copyFromSerializerToTargetChannel(channel),即对应的分区

```
	private void emit(T record, int[] targetChannels) throws IOException, InterruptedException {
		serializer.serializeRecord(record);

		boolean pruneAfterCopying = false;
		for (int channel : targetChannels) {
			if (copyFromSerializerToTargetChannel(channel)) {
				pruneAfterCopying = true;
			}
		}

		// Make sure we don't hold onto the large intermediate serialization buffer for too long
		if (pruneAfterCopying) {
			serializer.prune();
		}
	}
```


##  接下来处理Shuffle,即对各Source任务进行Reduce聚合，由于本案例只有一个并行度，所以运行的结果是一样的，重新聚合后


## BatchTask
### BatchTask.invoke

```
/**
	 * The main work method.
	 */
	@Override
	public void invoke() throws Exception {
		// --------------------------------------------------------------------
		// Initialize
		// --------------------------------------------------------------------
		if (LOG.isDebugEnabled()) {
			LOG.debug(formatLogString("Start registering input and output."));
		}

		// obtain task configuration (including stub parameters)
		Configuration taskConf = getTaskConfiguration();
		this.config = new TaskConfig(taskConf);

		// now get the operator class which drives the operation
		final Class<? extends Driver<S, OT>> driverClass = this.config.getDriver();
		this.driver = InstantiationUtil.instantiate(driverClass, Driver.class);

		String headName =  getEnvironment().getTaskInfo().getTaskName().split("->")[0].trim();
		this.metrics = getEnvironment().getMetricGroup()
			.getOrAddOperator(headName.startsWith("CHAIN") ? headName.substring(6) : headName);
		this.metrics.getIOMetricGroup().reuseInputMetricsForTask();
		if (config.getNumberOfChainedStubs() == 0) {
			this.metrics.getIOMetricGroup().reuseOutputMetricsForTask();
		}

		// initialize the readers.
		// this does not yet trigger any stream consuming or processing.
		initInputReaders();
		initBroadcastInputReaders();

		// initialize the writers.
		initOutputs();

		if (LOG.isDebugEnabled()) {
			LOG.debug(formatLogString("Finished registering input and output."));
		}

		// --------------------------------------------------------------------
		// Invoke
		// --------------------------------------------------------------------
		if (LOG.isDebugEnabled()) {
			LOG.debug(formatLogString("Start task code."));
		}

		this.runtimeUdfContext = createRuntimeContext(metrics);

		// whatever happens in this scope, make sure that the local strategies are cleaned up!
		// note that the initialization of the local strategies is in the try-finally block as well,
		// so that the thread that creates them catches its own errors that may happen in that process.
		// this is especially important, since there may be asynchronous closes (such as through canceling).
		try {
			// initialize the remaining data structures on the input and trigger the local processing
			// the local processing includes building the dams / caches
			try {
				int numInputs = driver.getNumberOfInputs();
				int numComparators = driver.getNumberOfDriverComparators();
				int numBroadcastInputs = this.config.getNumBroadcastInputs();
				
				initInputsSerializersAndComparators(numInputs, numComparators);
				initBroadcastInputsSerializers(numBroadcastInputs);
				
				// set the iterative status for inputs and broadcast inputs
				{
					List<Integer> iterativeInputs = new ArrayList<Integer>();
					
					for (int i = 0; i < numInputs; i++) {
						final int numberOfEventsUntilInterrupt = getTaskConfig().getNumberOfEventsUntilInterruptInIterativeGate(i);
			
						if (numberOfEventsUntilInterrupt < 0) {
							throw new IllegalArgumentException();
						}
						else if (numberOfEventsUntilInterrupt > 0) {
							this.inputReaders[i].setIterativeReader();
							iterativeInputs.add(i);
				
							if (LOG.isDebugEnabled()) {
								LOG.debug(formatLogString("Input [" + i + "] reads in supersteps with [" +
										+ numberOfEventsUntilInterrupt + "] event(s) till next superstep."));
							}
						}
					}
					this.iterativeInputs = asArray(iterativeInputs);
				}
				
				{
					List<Integer> iterativeBcInputs = new ArrayList<Integer>();
					
					for (int i = 0; i < numBroadcastInputs; i++) {
						final int numberOfEventsUntilInterrupt = getTaskConfig().getNumberOfEventsUntilInterruptInIterativeBroadcastGate(i);
						
						if (numberOfEventsUntilInterrupt < 0) {
							throw new IllegalArgumentException();
						}
						else if (numberOfEventsUntilInterrupt > 0) {
							this.broadcastInputReaders[i].setIterativeReader();
							iterativeBcInputs.add(i);
				
							if (LOG.isDebugEnabled()) {
								LOG.debug(formatLogString("Broadcast input [" + i + "] reads in supersteps with [" +
										+ numberOfEventsUntilInterrupt + "] event(s) till next superstep."));
							}
						}
					}
					this.iterativeBroadcastInputs = asArray(iterativeBcInputs);
				}
				
				initLocalStrategies(numInputs);
			}
			catch (Exception e) {
				throw new RuntimeException("Initializing the input processing failed" +
						(e.getMessage() == null ? "." : ": " + e.getMessage()), e);
			}

			if (!this.running) {
				if (LOG.isDebugEnabled()) {
					LOG.debug(formatLogString("Task cancelled before task code was started."));
				}
				return;
			}

			// pre main-function initialization
			initialize();

			// read the broadcast variables. they will be released in the finally clause 
			for (int i = 0; i < this.config.getNumBroadcastInputs(); i++) {
				final String name = this.config.getBroadcastInputName(i);
				readAndSetBroadcastInput(i, name, this.runtimeUdfContext, 1 /* superstep one for the start */);
			}

			// the work goes here
			run();
		}
		finally {
			// clean up in any case!
			closeLocalStrategiesAndCaches();

			clearReaders(inputReaders);
			clearWriters(eventualOutputs);

		}

		if (this.running) {
			if (LOG.isDebugEnabled()) {
				LOG.debug(formatLogString("Finished task code."));
			}
		} else {
			if (LOG.isDebugEnabled()) {
				LOG.debug(formatLogString("Task code cancelled."));
			}
		}
	}

```


### BatchTask.run
- 调用用户代码进行reduce, this.driver.run();
- 调用AggregatingUdf.reduce()进行绝后
- 调用 CountingCollector.collect(outT);发送给下游

```
protected void run() throws Exception {
		// ---------------------------- Now, the actual processing starts ------------------------
		// check for asynchronous canceling
		if (!this.running) {
			return;
		}

		boolean stubOpen = false;

		try {
			// run the data preparation
			try {
				this.driver.prepare();
			}
			catch (Throwable t) {
				// if the preparation caused an error, clean up
				// errors during clean-up are swallowed, because we have already a root exception
				throw new Exception("The data preparation for task '" + this.getEnvironment().getTaskInfo().getTaskName() +
					"' , caused an error: " + t.getMessage(), t);
			}

			// check for canceling
			if (!this.running) {
				return;
			}

			// start all chained tasks
			BatchTask.openChainedTasks(this.chainedTasks, this);

			// open stub implementation
			if (this.stub != null) {
				try {
					Configuration stubConfig = this.config.getStubParameters();
					FunctionUtils.openFunction(this.stub, stubConfig);
					stubOpen = true;
				}
				catch (Throwable t) {
					throw new Exception("The user defined 'open()' method caused an exception: " + t.getMessage(), t);
				}
			}

			// run the user code
			this.driver.run();

			// close. We close here such that a regular close throwing an exception marks a task as failed.
			if (this.running && this.stub != null) {
				FunctionUtils.closeFunction(this.stub);
				stubOpen = false;
			}

			this.output.close();

			// close all chained tasks letting them report failure
			BatchTask.closeChainedTasks(this.chainedTasks, this);
		}
		catch (Exception ex) {
			// close the input, but do not report any exceptions, since we already have another root cause
			if (stubOpen) {
				try {
					FunctionUtils.closeFunction(this.stub);
				}
				catch (Throwable t) {
					// do nothing
				}
			}
			
			// if resettable driver invoke teardown
			if (this.driver instanceof ResettableDriver) {
				final ResettableDriver<?, ?> resDriver = (ResettableDriver<?, ?>) this.driver;
				try {
					resDriver.teardown();
				} catch (Throwable t) {
					throw new Exception("Error while shutting down an iterative operator: " + t.getMessage(), t);
				}
			}

			BatchTask.cancelChainedTasks(this.chainedTasks);

			ex = ExceptionInChainedStubException.exceptionUnwrap(ex);

			if (ex instanceof CancelTaskException) {
				// forward canceling exception
				throw ex;
			}
			else if (this.running) {
				// throw only if task was not cancelled. in the case of canceling, exceptions are expected 
				BatchTask.logAndThrowException(ex, this);
			}
		}
		finally {
			this.driver.cleanup();
		}
	}

```

###  CountingCollector.collect

```
	public void collect(OUT record) {
		this.numRecordsOut.inc();
		this.collector.collect(record);
	}
```

### OutputCollector.collect
- 调用 writer.emit(this.delegate);

```

/**
	 * Collects a record and emits it to all writers.
	 */
	@Override
	public void collect(T record)  {
		if (record != null) {
			this.delegate.setInstance(record);
			try {
				for (RecordWriter<SerializationDelegate<T>> writer : writers) {
					writer.emit(this.delegate);
				}
			}
			catch (IOException e) {
				throw new RuntimeException("Emitting the record caused an I/O exception: " + e.getMessage(), e);
			}
			catch (InterruptedException e) {
				throw new RuntimeException("Emitting the record was interrupted: " + e.getMessage(), e);
			}
		}
		else {
			throw new NullPointerException("The system does not support records that are null."
								+ "Null values are only supported as fields inside other objects.");
		}
	}
```

### RecordWriter.emit

```
	public void emit(T record) throws IOException, InterruptedException {
		emit(record, channelSelector.selectChannels(record, numChannels));
	}
```


### RecordWriter.emit
- copyFromSerializerToTargetChannel(channel) 发送给下游
```
	private void emit(T record, int[] targetChannels) throws IOException, InterruptedException {
		serializer.serializeRecord(record);

		boolean pruneAfterCopying = false;
		for (int channel : targetChannels) {
			if (copyFromSerializerToTargetChannel(channel)) {
				pruneAfterCopying = true;
			}
		}

		// Make sure we don't hold onto the large intermediate serialization buffer for too long
		if (pruneAfterCopying) {
			serializer.prune();
		}
	}

```

## 处理DataSink
- 调用DataSinkTask.invoke()


end