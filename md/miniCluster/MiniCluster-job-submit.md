# Flink MiniCluster 作业提交

## 程序源码
- https://github.com/opensourceteams/fink-maven-scala

## 程序main函数WordCount
- SocketWindowWordCount.scala 
- env.execute("默认作业") 调用.LocalStreamEnvironment.execute函数

```

package com.opensourceteams.module.bigdata.flink.example.stream.worldcount.nc

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * nc -lk 1234  输入数据
  */
object SocketWindowWordCount {

  def main(args: Array[String]): Unit = {


    val port = 1234
    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // get input data by connecting to the socket
    val dataStream = env.socketTextStream("localhost", port, '\n')


    import org.apache.flink.streaming.api.scala._
    val textResult = dataStream.flatMap( w => w.split("\\s") ).map( w => WordWithCount(w,1))
      .keyBy("word")
      /**
        * 每5秒刷新一次，相当于重新开始计数，
        * 好处，不需要一直拿所有的数据统计
        * 只需要在指定时间间隔内的增量数据，减少了数据规模
        */
      .timeWindow(Time.seconds(5))
      .sum("count" )

    textResult.print().setParallelism(1)

    if(args == null || args.size ==0){
      env.execute("默认作业")
    }else{
      env.execute(args(0))
    }

    println("结束")

  }


  // Data type for words with count
  case class WordWithCount(word: String, count: Long)

}

```

### LocalStreamEnvironment.execute
- 用指定的作业名称，在Flink miniCluster上执行JobGraph
- getStreamGraph()构建图流
- streamGraph.getJobGraph()得到作业图
- 通过MiniClusterConfiguration构建MiniCluster
- 启动MiniCluster
- 调用函数miniCluster.executeJobBlocking(jobGraph)提交作业图
```
/**
	 * Executes the JobGraph of the on a mini cluster of CLusterUtil with a user
	 * specified name.
	 *
	 * @param jobName
	 *            name of the job
	 * @return The result of the job execution, containing elapsed time and accumulators.
	 */
	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		// transform the streaming program into a JobGraph
		StreamGraph streamGraph = getStreamGraph();
		streamGraph.setJobName(jobName);

		JobGraph jobGraph = streamGraph.getJobGraph();
		jobGraph.setAllowQueuedScheduling(true);

		Configuration configuration = new Configuration();
		configuration.addAll(jobGraph.getJobConfiguration());
		configuration.setString(TaskManagerOptions.MANAGED_MEMORY_SIZE, "0");

		// add (and override) the settings with what the user defined
		configuration.addAll(this.configuration);

		if (!configuration.contains(RestOptions.PORT)) {
			configuration.setInteger(RestOptions.PORT, 0);
		}

		int numSlotsPerTaskManager = configuration.getInteger(TaskManagerOptions.NUM_TASK_SLOTS, jobGraph.getMaximumParallelism());

		MiniClusterConfiguration cfg = new MiniClusterConfiguration.Builder()
			.setConfiguration(configuration)
			.setNumSlotsPerTaskManager(numSlotsPerTaskManager)
			.build();

		if (LOG.isInfoEnabled()) {
			LOG.info("Running job on local embedded Flink mini cluster");
		}

		MiniCluster miniCluster = new MiniCluster(cfg);

		try {
			miniCluster.start();
			configuration.setInteger(RestOptions.PORT, miniCluster.getRestAddress().getPort());

			return miniCluster.executeJobBlocking(jobGraph);
		}
		finally {
			transformations.clear();
			miniCluster.close();
		}
	}

```

### miniCluster.executeJobBlocking
- 此方法是阻塞模式，在运行成功后或失败后返回结果
- 调用函数submitJob(job)提交作业
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

### submitJob(job)
- 调用函数  dispatcherGateway.submitJob(jobGraph, rpcTimeout)
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

###  Dispatcher.submitJob
- INFO日志输出提交作业名称
- 调用函数waitForTerminatingJobManager

```
@Override
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


### Dispatcher.waitForTerminatingJobManager
- 调用函数 action.apply(jobGraph)

```
private CompletableFuture<Void> waitForTerminatingJobManager(JobID jobId, JobGraph jobGraph, FunctionWithException<JobGraph, CompletableFuture<Void>, ?> action) {
		final CompletableFuture<Void> jobManagerTerminationFuture = getJobTerminationFuture(jobId)
			.exceptionally((Throwable throwable) -> {
				throw new CompletionException(
					new DispatcherException(
						String.format("Termination of previous JobManager for job %s failed. Cannot submit job under the same job id.", jobId),
						throwable)); });

		return jobManagerTerminationFuture.thenComposeAsync(
			FunctionUtils.uncheckedFunction((ignored) -> {
				jobManagerTerminationFutures.remove(jobId);
				return action.apply(jobGraph);
			}),
			getMainThreadExecutor());
	}


```

### dispatcher.DefaultJobManagerRunnerFactory.createJobManagerRunner
- new JobManagerRunner

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

### JobManagerRunner
- new JobMaster
```
/**
	 * Exceptions that occur while creating the JobManager or JobManagerRunner are directly
	 * thrown and not reported to the given {@code FatalErrorHandler}.
	 *
	 * @throws Exception Thrown if the runner cannot be set up, because either one of the
	 *                   required services could not be started, ot the Job could not be initialized.
	 */
	public JobManagerRunner(
			final ResourceID resourceId,
			final JobGraph jobGraph,
			final Configuration configuration,
			final RpcService rpcService,
			final HighAvailabilityServices haServices,
			final HeartbeatServices heartbeatServices,
			final BlobServer blobServer,
			final JobManagerSharedServices jobManagerSharedServices,
			final JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory,
			final FatalErrorHandler fatalErrorHandler) throws Exception {

		this.resultFuture = new CompletableFuture<>();
		this.terminationFuture = new CompletableFuture<>();

		// make sure we cleanly shut down out JobManager services if initialization fails
		try {
			this.jobGraph = checkNotNull(jobGraph);
			this.jobManagerSharedServices = checkNotNull(jobManagerSharedServices);
			this.fatalErrorHandler = checkNotNull(fatalErrorHandler);

			checkArgument(jobGraph.getNumberOfVertices() > 0, "The given job is empty");

			// libraries and class loader first
			final LibraryCacheManager libraryCacheManager = jobManagerSharedServices.getLibraryCacheManager();
			try {
				libraryCacheManager.registerJob(
						jobGraph.getJobID(), jobGraph.getUserJarBlobKeys(), jobGraph.getClasspaths());
			} catch (IOException e) {
				throw new Exception("Cannot set up the user code libraries: " + e.getMessage(), e);
			}

			final ClassLoader userCodeLoader = libraryCacheManager.getClassLoader(jobGraph.getJobID());
			if (userCodeLoader == null) {
				throw new Exception("The user code class loader could not be initialized.");
			}

			// high availability services next
			this.runningJobsRegistry = haServices.getRunningJobsRegistry();
			this.leaderElectionService = haServices.getJobManagerLeaderElectionService(jobGraph.getJobID());

			final JobMasterConfiguration jobMasterConfiguration = JobMasterConfiguration.fromConfiguration(configuration);

			this.rpcTimeout = jobMasterConfiguration.getRpcTimeout();

			this.leaderGatewayFuture = new CompletableFuture<>();

			final SlotPoolFactory slotPoolFactory = DefaultSlotPoolFactory.fromConfiguration(
				configuration,
				rpcService);

			// now start the JobManager
			this.jobMaster = new JobMaster(
				rpcService,
				jobMasterConfiguration,
				resourceId,
				jobGraph,
				haServices,
				slotPoolFactory,
				jobManagerSharedServices,
				heartbeatServices,
				blobServer,
				jobManagerJobMetricGroupFactory,
				this,
				fatalErrorHandler,
				userCodeLoader);
		}
		catch (Throwable t) {
			terminationFuture.completeExceptionally(t);
			resultFuture.completeExceptionally(t);

			throw new JobExecutionException(jobGraph.getJobID(), "Could not set up JobManager", t);
		}
	}


```

### JobMaster
- Initializing job
- 调用函数createAndRestoreExecutionGraph
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

### JobMaster.createAndRestoreExecutionGraph
- 调用函数createExecutionGraph

```
private ExecutionGraph createAndRestoreExecutionGraph(JobManagerJobMetricGroup currentJobManagerJobMetricGroup) throws Exception {

		ExecutionGraph newExecutionGraph = createExecutionGraph(currentJobManagerJobMetricGroup);

		final CheckpointCoordinator checkpointCoordinator = newExecutionGraph.getCheckpointCoordinator();

		if (checkpointCoordinator != null) {
			// check whether we find a valid checkpoint
			if (!checkpointCoordinator.restoreLatestCheckpointedState(
				newExecutionGraph.getAllVertices(),
				false,
				false)) {

				// check whether we can restore from a savepoint
				tryRestoreExecutionGraphFromSavepoint(newExecutionGraph, jobGraph.getSavepointRestoreSettings());
			}
		}

		return newExecutionGraph;
	}

```

### JobMaster.createExecutionGraph
- 调用ExecutionGraphBuilder.buildGraph

```
private ExecutionGraph createExecutionGraph(JobManagerJobMetricGroup currentJobManagerJobMetricGroup) throws JobExecutionException, JobException {
		return ExecutionGraphBuilder.buildGraph(
			null,
			jobGraph,
			jobMasterConfiguration.getConfiguration(),
			scheduledExecutorService,
			scheduledExecutorService,
			slotPool.getSlotProvider(),
			userCodeLoader,
			highAvailabilityServices.getCheckpointRecoveryFactory(),
			rpcTimeout,
			restartStrategy,
			currentJobManagerJobMetricGroup,
			blobServer,
			jobMasterConfiguration.getSlotRequestTimeout(),
			log);
	}


```


### ExecutionGraphBuilder.buildGraph
- 调用函数buildGraph

```
/**
	 * Builds the ExecutionGraph from the JobGraph.
	 * If a prior execution graph exists, the JobGraph will be attached. If no prior execution
	 * graph exists, then the JobGraph will become attach to a new empty execution graph.
	 */
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
			BlobWriter blobWriter,
			Time allocationTimeout,
			Logger log)
		throws JobExecutionException, JobException {

		return buildGraph(
			prior,
			jobGraph,
			jobManagerConfig,
			futureExecutor,
			ioExecutor,
			slotProvider,
			classLoader,
			recoveryFactory,
			rpcTimeout,
			restartStrategy,
			metrics,
			-1,
			blobWriter,
			allocationTimeout,
			log);
	}


```



###  ExecutionGraphBuilder.buildGraph
- 从JobGraph构建ExecutionGraph,如果存在先前的执行图，则将附加JobGraph。 如果没有事先执行图存在，然后JobGraph将附加到一个新的空执行图。
- INFO日志:Running initialization on master for job 默认作业 (239b80a2409163ab70426660441a9416).
- INFO日志输出:Successfully ran initialization on master in 23307 ms.
- 函数返回executionGraph
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


### JobManagerRunner.grantLeadership
- 调用函数verifyJobSchedulingStatusAndStartJobManager(leaderSessionID);

```
	//----------------------------------------------------------------------------------------------
	// Leadership methods
	//----------------------------------------------------------------------------------------------

	@Override
	public void grantLeadership(final UUID leaderSessionID) {
		synchronized (lock) {
			if (shutdown) {
				log.info("JobManagerRunner already shutdown.");
				return;
			}

			try {
				verifyJobSchedulingStatusAndStartJobManager(leaderSessionID);
			} catch (Exception e) {
				handleJobManagerRunnerError(e);
			}
		}
	}

```

### JobManagerRunner.verifyJobSchedulingStatusAndStartJobManager
- INFO日志输出:JobManager runner for job 默认作业 (8f3dc40e802309e580e7d685b3a40e78) was granted leadership with session id 8e88e148-a755-4f1b-b731-8744fb4c9483 at akka://flink/user/jobmanager_1.
- 调用函数:jobMaster.start
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
### JobMaster.start
- 调用函数startJobExecution()启动JobExecution()
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
- INFO日志输出:Starting execution of job 默认作业 (8f3dc40e802309e580e7d685b3a40e78)

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


### ExecutionGraph.scheduleForExecution
- 调用函数transitionState

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

### ExecutionGraph.transitionState
- 调用函数 transitionState

```
	// ------------------------------------------------------------------------
	//  State Transitions
	// ------------------------------------------------------------------------

	private boolean transitionState(JobStatus current, JobStatus newState) {
		return transitionState(current, newState, null);
	}
```


### ExecutionGraph.transitionState
- INFO日志输出: Job 默认作业 (8f3dc40e802309e580e7d685b3a40e78) switched from state CREATED to RUNNING.

```
private boolean transitionState(JobStatus current, JobStatus newState, Throwable error) {
		// consistency check
		if (current.isTerminalState()) {
			String message = "Job is trying to leave terminal state " + current;
			LOG.error(message);
			throw new IllegalStateException(message);
		}

		// now do the actual state transition
		if (STATE_UPDATER.compareAndSet(this, current, newState)) {
			LOG.info("Job {} ({}) switched from state {} to {}.", getJobName(), getJobID(), current, newState, error);

			stateTimestamps[newState.ordinal()] = System.currentTimeMillis();
			notifyJobStatusChange(newState, error);
			return true;
		}
		else {
			return false;
		}
	}
```
end