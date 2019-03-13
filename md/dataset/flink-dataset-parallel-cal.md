# Flink1.7.2  Dataset 并行计算源码分析

## 概述
- 了解Flink处理流程(用户程序 -> JobGrapth -> ExecutionGraph -> JobVertex -> ExecutionVertex -> 并行度 -> Task(DataSourceTask,BatchTask,DataSinkTask)
- 了解ExecutionVetex的构建，Task的构建，执行，任务之间的调用关系

## 原理分析
- 程序会转成JobGrapth提交,JobGraph最终转为ExecutionGraph进行处理
- ExecutionGraph会拆分成ExecutionJobVertex执行，按(DataSourceTask,BatchTask,DataSinkTask) 进行拆分
 
    ```
 0 = jobVertex = {InputFormatVertex@7675} "CHAIN DataSource (at com.opensourceteams.module.bigdata.flink.example.dataset.worldcount.WordCountRun$.main(WordCountRun.scala:19) (org.apache.flink.api.java.io.TextInp) -> FlatMap (FlatMap at com.opensourceteams.module.bigdata.flink.example.dataset.worldcount.WordCountRun$.main(WordCountRun.scala:23)) -> Map (Map at com.opensourceteams.module.bigdata.flink.example.dataset.worldcount.WordCountRun$.main(WordCountRun.scala:23)) -> Combine (SUM(1)) (org.apache.flink.runtime.operators.DataSourceTask)"

 1 = jobVertex = {JobVertex@7695} "Reduce (SUM(1)) (org.apache.flink.runtime.operators.BatchTask)"
 
 2 = jobVertex = {OutputFormatVertex@6665} "DataSink (collect()) (org.apache.flink.runtime.operators.DataSinkTask)"
    ```
- ExecutionJobVertex 执行流程CREATED  -> DEPLOYING ,转成对应的Task(CREATED -->DEPLOYING --> RUNNING)
- 默认作业调度模式为：LAZY_FROM_SOURCES，只启动Source任务，下游任务是当上游任务开始给他发送数据时才开始
   
    - 刚开始，只有DataSourceTask对应的ExecutionJobVertex的 jobVertex.inputs 为空(元素个数0个)，所以只对DataSourceTask进行调度，部署，任务运行
    - 随着DataSourceTask开始处理，就会产生中间数据，这时候通过输出数据，按key进行分区，分到对应的BatchTask分区数据，这个时候BatchTask就开始调度，部署，任务运行
    - 随着BatchTask开始处理，就会产生中间数据，这时候通过输出数据，按key进行分区，分到对应的DataSinkTask分区数据，这个时候DataSinkTask就开始调度，部署，任务运行
    - 由于后面的任务依赖前边的任务，就不会一开始就运行所有的任务，串行到，只有该任务有上游的数据发送过来，该任务才会启动，运行，换句话说，就是下游的任务是不启动的，只有上游的任务发送数据过来时，才开始启动，运行，这样节省了计算资源
    

- 有几个并行度，ExecutionJobVertex 会转成对应的几个ExecutionVertex,ExecutionVertex 是会转化成Task来运行，ExecutionVertex中并行度通过subTaskIndex来区分，第一个subTaskIndex=0 ,第二个subTaskIndex = 1



## 输入数据

```
c a a
b c a
```

## 程序 
- WordCount.scala进行单词统计

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

## 源码分析



## JobMaster
### JobMaster
- new JobMaster()
- 把JobGraph 转换为ExecutionGrapth
    
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



## ExecutionGraph
### ExecutionGraph.scheduleForExecution()

- 负责Execution的调度，也就是负责把ExecutionGrapth转成ExecutionJobVertex,ExecutionJobVertex转成ExecutionVertex，再转成任务，这是真正的开始逻辑的地方
- 更新当前Job的状态，即更新ExecutionGraph的状态,从CREATED更新到RUNNING
    ```
transitionState(JobStatus.CREATED, JobStatus.RUNNING)
    ```
    - INFO级别日志
    ```
Job Flink Java Job at Mon Mar 11 18:57:37 CST 2019 (f24b82ed1ec3e1c90455c342a9dfc21e) switched from state CREATED to RUNNING.
    ```

- 默认的作业调度模式 LAZY_FROM_SOURCES,
    - LAZY_FROM_SOURCES:从sources开始安排任务。 一旦输入数据准备就绪，就开始下游任务，(刚开始只有Sources任务，下游任务都是未开始的) ;
    - EAGER ： 立即安排所有任务
 
    ```
private ScheduleMode scheduleMode = ScheduleMode.LAZY_FROM_SOURCES;
    ```
- 调用 ExecutionGraph.scheduleLazy()  //延迟调度
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


###  JobStatus
- 作业的状态 CREATED(已创建) -> RUNNING(运行中)  -> FINISHED(已完成) 等

```

/**
 * Possible states of a job once it has been accepted by the job manager.
 */
public enum JobStatus {

	/** Job is newly created, no task has started to run. */
	CREATED(TerminalState.NON_TERMINAL),

	/** Some tasks are scheduled or running, some may be pending, some may be finished. */
	RUNNING(TerminalState.NON_TERMINAL),

	/** The job has failed and is currently waiting for the cleanup to complete */
	FAILING(TerminalState.NON_TERMINAL),
	
	/** The job has failed with a non-recoverable task failure */
	FAILED(TerminalState.GLOBALLY),

	/** Job is being cancelled */
	CANCELLING(TerminalState.NON_TERMINAL),
	
	/** Job has been cancelled */
	CANCELED(TerminalState.GLOBALLY),

	/** All of the job's tasks have successfully finished. */
	FINISHED(TerminalState.GLOBALLY),
	
	/** The job is currently undergoing a reset and total restart */
	RESTARTING(TerminalState.NON_TERMINAL),

	/** The job has been suspended and is currently waiting for the cleanup to complete */
	SUSPENDING(TerminalState.NON_TERMINAL),

	/**
	 * The job has been suspended which means that it has been stopped but not been removed from a
	 * potential HA job store.
	 */
	SUSPENDED(TerminalState.LOCALLY),

	/** The job is currently reconciling and waits for task execution report to recover state. */
	RECONCILING(TerminalState.NON_TERMINAL);
	
	// ----------------------------

```

###  ScheduleMode

- 作业调度模式，即ExecutionGraph调度模式(LAZY_FROM_SOURCES,EAGER)
- LAZY_FROM_SOURCES:从sources开始安排任务。 一旦输入数据准备就绪，就开始下游任务，(刚开始只有Sources任务，下游任务都是未开始的)
- EAGER ： 立即安排所有任务
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

package org.apache.flink.runtime.jobgraph;

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

- 程序会转成JobGrapth提交,JobGraph最终转为ExecutionGraph进行处理
- ExecutionGraph会拆分成ExecutionJobVertex执行，按(DataSourceTask,BatchTask,DataSinkTask) 进行拆分
 
    ```
 0 = jobVertex = {InputFormatVertex@7675} "CHAIN DataSource (at com.opensourceteams.module.bigdata.flink.example.dataset.worldcount.WordCountRun$.main(WordCountRun.scala:19) (org.apache.flink.api.java.io.TextInp) -> FlatMap (FlatMap at com.opensourceteams.module.bigdata.flink.example.dataset.worldcount.WordCountRun$.main(WordCountRun.scala:23)) -> Map (Map at com.opensourceteams.module.bigdata.flink.example.dataset.worldcount.WordCountRun$.main(WordCountRun.scala:23)) -> Combine (SUM(1)) (org.apache.flink.runtime.operators.DataSourceTask)"

 1 = jobVertex = {JobVertex@7695} "Reduce (SUM(1)) (org.apache.flink.runtime.operators.BatchTask)"
 
 2 = jobVertex = {OutputFormatVertex@6665} "DataSink (collect()) (org.apache.flink.runtime.operators.DataSinkTask)"
    ```
- ExecutionJobVertex （执行流程：CREATED  -> DEPLOYING ）,转成对应的Task(执行流程：CREATED -->DEPLOYING --> RUNNING)

    ```
    verticesInCreationOrder = {ArrayList@6145}  size = 3
 0 = {ExecutionJobVertex@6484} 
  stateMonitor = {Object@6608} 
  graph = {ExecutionGraph@5602} 
  jobVertex = {InputFormatVertex@6609} "CHAIN DataSource (at com.opensourceteams.module.bigdata.flink.example.dataset.worldcount.WordCountRun$.main(WordCountRun.scala:19) (org.apache.flink.api.java.io.TextInp) -> FlatMap (FlatMap at com.opensourceteams.module.bigdata.flink.example.dataset.worldcount.WordCountRun$.main(WordCountRun.scala:23)) -> Map (Map at com.opensourceteams.module.bigdata.flink.example.dataset.worldcount.WordCountRun$.main(WordCountRun.scala:23)) -> Combine (SUM(1)) (org.apache.flink.runtime.operators.DataSourceTask)"
  operatorIDs = {Collections$UnmodifiableRandomAccessList@6610}  size = 1
  userDefinedOperatorIds = {Collections$UnmodifiableRandomAccessList@6611}  size = 1
  taskVertices = {ExecutionVertex[2]@6612} 
  producedDataSets = {IntermediateResult[1]@6614} 
  inputs = {ArrayList@6616}  size = 0
  parallelism = 2
  slotSharingGroup = {SlotSharingGroup@6617} "SlotSharingGroup [9eb9f93248a641c71df925ec6245124a, b2e911c37d2ae462430e12812fb033ad, a2139d4cdf059b384d883251604a5f2e]"
  coLocationGroup = null
  inputSplits = {FileInputSplit[2]@6618} 
  maxParallelismConfigured = true
  maxParallelism = 2
  serializedTaskInformation = null
  taskInformationBlobKey = null
  taskInformationOrBlobKey = null
  splitAssigner = {LocatableInputSplitAssigner@6620} 
 1 = {ExecutionJobVertex@6606} 
  stateMonitor = {Object@6653} 
  graph = {ExecutionGraph@5602} 
  jobVertex = {JobVertex@6654} "Reduce (SUM(1)) (org.apache.flink.runtime.operators.BatchTask)"
  operatorIDs = {Collections$UnmodifiableRandomAccessList@6655}  size = 1
  userDefinedOperatorIds = {Collections$UnmodifiableRandomAccessList@6656}  size = 1
  taskVertices = {ExecutionVertex[2]@6658} 
  producedDataSets = {IntermediateResult[1]@6659} 
  inputs = {ArrayList@6660}  size = 1
  parallelism = 2
  slotSharingGroup = {SlotSharingGroup@6617} "SlotSharingGroup [9eb9f93248a641c71df925ec6245124a, b2e911c37d2ae462430e12812fb033ad, a2139d4cdf059b384d883251604a5f2e]"
  coLocationGroup = null
  inputSplits = null
  maxParallelismConfigured = true
  maxParallelism = 2
  serializedTaskInformation = null
  taskInformationBlobKey = null
  taskInformationOrBlobKey = null
  splitAssigner = null
 2 = {ExecutionJobVertex@6607} 
  stateMonitor = {Object@6664} 
  graph = {ExecutionGraph@5602} 
  jobVertex = {OutputFormatVertex@6665} "DataSink (collect()) (org.apache.flink.runtime.operators.DataSinkTask)"
  operatorIDs = {Collections$UnmodifiableRandomAccessList@6666}  size = 1
  userDefinedOperatorIds = {Collections$UnmodifiableRandomAccessList@6667}  size = 1
  taskVertices = {ExecutionVertex[2]@6668} 
  producedDataSets = {IntermediateResult[0]@6669} 
  inputs = {ArrayList@6670}  size = 1
  parallelism = 2
  slotSharingGroup = {SlotSharingGroup@6617} "SlotSharingGroup [9eb9f93248a641c71df925ec6245124a, b2e911c37d2ae462430e12812fb033ad, a2139d4cdf059b384d883251604a5f2e]"
  coLocationGroup = null
  inputSplits = null
  maxParallelismConfigured = true
  maxParallelism = 2
  serializedTaskInformation = null
  taskInformationBlobKey = null
  taskInformationOrBlobKey = null
  splitAssigner = null
    ```
    


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


### ExecutionJobVertex.scheduleAll
- 有几个并行度把ExecutionJobVertex转成对应个数的 ExecutionVertex
- 调用ExecutionVertex.scheduleForExecution() 处理
- Execution 状态为 **CREATED**

```
	//---------------------------------------------------------------------------------------------
	//  Actions
	//---------------------------------------------------------------------------------------------

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

### ExecutionVertex.scheduleForExecution() 
- 调用 Execution.scheduleForExecution

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

- 分配Slot给Execution
 
    ```
    final CompletableFuture<Execution> allocationFuture = allocateAndAssignSlotForExecution(
    				slotProvider,
    				queued,
    				locationPreferenceConstraint,
    				allPreviousExecutionGraphAllocationIds,
    				allocationTimeout);
    ```
- 调用Execution.deploy()函数，部署Execution到分给的slot中


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


### Execution.deploy()
- Execution 状态从**SCHDULED**到**DEPLOYING**
- 构建部署对象
 
    ```
    	final TaskDeploymentDescriptor deployment = vertex.createDeploymentDescriptor(
				attemptId,
				slot,
				taskRestore,
				attemptNumber);
    ```
- 调用TaskExecutor.submitTask

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


### ExecutionState
- Execution状态  CREATED(已创建) -> SCHEDULED(已调度) -> DEPLOYING(已部署) -> RUNNING(运行中) -> FINISHED(已完成)  等

```


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


### TaskExecutor.submitTask
- 构建Task，Task 默认的状态为**CREATED**
 
    ```
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
    ```

- 调用task.startTaskThread();  //调用task线程的run()函数

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
- 这才开始处理Task,任务的状态用的是ExecutionState中的状态值
- 更新Task状态从**CREATED** 到 **DEPLOYING**
- 加载这个Task的jar文件
 
    ```
	// first of all, get a user-code classloader
			// this may involve downloading the job's JAR files and/or classes
			LOG.info("Loading JAR files for task {}.", this);

			userCodeClassLoader = createUserCodeClassloader();
			final ExecutionConfig executionConfig = serializedExecutionConfig.deserializeValue(userCodeClassLoader);
    ```
- 构建任务运行环境

    ```
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

    ```
- 更新当前任务状态从 **DEPLOYING** 到 **RUNNING**    

    ```
    transitionState(ExecutionState.DEPLOYING, ExecutionState.RUNNING)
    ```
- 调用DataSourceTask.invoke(),会根据具体的任务，调用具体任务的函数
    
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

### DataSourceTask.invoke()
- Transformation chain

    ```
    // start all chained tasks
			BatchTask.openChainedTasks(this.chainedTasks, this);
    ```
    
    ```
    this.chainedTasks = {ArrayList@7851}  size = 3
 0 = {ChainedFlatMapDriver@7850} 
 1 = {ChainedMapDriver@7988} 
 2 = {SynchronousChainedCombineDriver@7989} 
    ```

- 得到输入分片,读取文件的块位置信息
```
// get input splits to read
			final Iterator<InputSplit> splitIterator = getInputSplits();
```

- 得到文件位置信息
    ```
    file:/opt/n_001_workspaces/bigdata/flink/flink-maven-scala-2/src/main/resources/data/line.txt:0+6
    
    ```

- 循环读取分片信息,读到的数据是按行的

    ```
    						while (!this.taskCanceled && !format.reachedEnd()) {
    							OT returned;
    							if ((returned = format.nextRecord(serializer.createInstance())) != null) {
    								output.collect(returned);
    							}
    						}
    ```

```
/**
	 * Create an Invokable task and set its environment.
	 *
	 * @param environment The environment assigned to this invokable.
	 */
	public DataSourceTask(Environment environment) {
		super(environment);
	}

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

## DelimitedInputFormat
### DelimitedInputFormat.nextRecord
- 调用 DelimitedInputFormat.readLine()

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
- 具体读取文件数据的方法，怎么读文件数据的逻辑，在这里
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




