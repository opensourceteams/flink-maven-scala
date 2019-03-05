# Flink1.7.2  并行计算源码分析

## 源码
- 源码:https://github.com/opensourceteams/fink-maven-scala-2
- Flink1.7.2 Source、Window数据交互源码分析: https://github.com/opensourceteams/fink-maven-scala-2/blob/master/md/miniCluster/flink-source-window-data-exchange.md

## 概述
- Flink Window如何进行并行计算
- Flink source如何按key,hash分区，并发射到对应分区的下游Window


## WordCount 程序

```
package com.opensourceteams.module.bigdata.flink.example.stream.worldcount.nc.parallelism

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * nc -lk 1234  输入数据
  */
object SocketWindowWordCountLocal {



  def main(args: Array[String]): Unit = {


    val port = 1234
    // get the execution environment
   // val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    val configuration : Configuration = getConfiguration(true)

    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment(1,configuration)





    // get input data by connecting to the socket
    val dataStream = env.socketTextStream("localhost", port, '\n')



    import org.apache.flink.streaming.api.scala._
    val textResult = dataStream.flatMap( w => w.split("\\s") ).map( w => WordWithCount(w,1))
      .keyBy("word")
      /**
        * 每20秒刷新一次，相当于重新开始计数，
        * 好处，不需要一直拿所有的数据统计
        * 只需要在指定时间间隔内的增量数据，减少了数据规模
        */
      .timeWindow(Time.seconds(5))
      //.countWindow(3)
      //.countWindow(3,1)
      //.countWindowAll(3)


      .sum("count" )

    textResult
      .setParallelism(3)
      .print()




    if(args == null || args.size ==0){


      println("==================================以下为执行计划==================================")
      println("执行地址(firefox效果更好):https://flink.apache.org/visualizer")
      //执行计划
      //println(env.getExecutionPlan)
     // println("==================================以上为执行计划 JSON串==================================\n")
      //StreamGraph
     println(env.getStreamGraph.getStreamingPlanAsJSON)



      //JsonPlanGenerator.generatePlan(jobGraph)

      env.execute("默认作业")

    }else{
      env.execute(args(0))
    }

    println("结束")

  }


  // Data type for words with count
  case class WordWithCount(word: String, count: Long){
    //override def toString: String = Thread.currentThread().getName + word + " : " + count
  }


  def getConfiguration(isDebug:Boolean = false):Configuration = {

    val configuration : Configuration = new Configuration()

    if(isDebug){
      val timeout = "100000 s"
      val timeoutHeartbeatPause = "1000000 s"
      configuration.setString("akka.ask.timeout",timeout)
      configuration.setString("akka.lookup.timeout",timeout)
      configuration.setString("akka.tcp.timeout",timeout)
      configuration.setString("akka.transport.heartbeat.interval",timeout)
      configuration.setString("akka.transport.heartbeat.pause",timeoutHeartbeatPause)
      configuration.setString("akka.watch.heartbeat.pause",timeout)
      configuration.setInteger("heartbeat.interval",10000000)
      configuration.setInteger("heartbeat.timeout",50000000)
    }


    configuration
  }


}


```

## 输入数据
```
1 2 3 4 5 6 7 8 9 10
```
## 源码分析


### ExecutionGraph.scheduleEager

- ExecutionGraph 调度
- executionsToDeploy包括所有的(Source,Window,Sink),在这里设置的setParallelism()并行度为多少，就有多少个Window,本案例设置的为3，所以executionsToDeploy对象的数据如下
 
    - (Source: Socket Stream -> Flat Map -> Map (1/1))
    -  (Window(TumblingProcessingTimeWindows(5000), ProcessingTimeTrigger, SumAggregator, PassThroughWindowFunction) (3/3))
    - (Window(TumblingProcessingTimeWindows(5000), ProcessingTimeTrigger, SumAggregator, PassThroughWindowFunction) (2/3))
    - (Window(TumblingProcessingTimeWindows(5000), ProcessingTimeTrigger, SumAggregator, PassThroughWindowFunction) (1/3))
    -  (Sink: Print to Std. Out (1/1))

    - 详细executionsToDeploy对象
    ```
    executionsToDeploy = {Arrays$ArrayList@5323}  size = 5
 0 = {Execution@5324} "Attempt #0 (Source: Socket Stream -> Flat Map -> Map (1/1)) @ org.apache.flink.runtime.jobmaster.slotpool.SingleLogicalSlot@22dc33b2 - [SCHEDULED]"
 1 = {Execution@5506} "Attempt #0 (Window(TumblingProcessingTimeWindows(5000), ProcessingTimeTrigger, SumAggregator, PassThroughWindowFunction) (3/3)) @ org.apache.flink.runtime.jobmaster.slotpool.SingleLogicalSlot@8f216e4 - [SCHEDULED]"
 2 = {Execution@5507} "Attempt #0 (Window(TumblingProcessingTimeWindows(5000), ProcessingTimeTrigger, SumAggregator, PassThroughWindowFunction) (2/3)) @ org.apache.flink.runtime.jobmaster.slotpool.SingleLogicalSlot@50ccca83 - [SCHEDULED]"
 3 = {Execution@5508} "Attempt #0 (Window(TumblingProcessingTimeWindows(5000), ProcessingTimeTrigger, SumAggregator, PassThroughWindowFunction) (1/3)) @ org.apache.flink.runtime.jobmaster.slotpool.SingleLogicalSlot@243b4f41 - [SCHEDULED]"
 4 = {Execution@5509} "Attempt #0 (Sink: Print to Std. Out (1/1)) @ org.apache.flink.runtime.jobmaster.slotpool.SingleLogicalSlot@67b9a9d7 - [SCHEDULED]"
    ```
- 源码
- 调用Execution.deploy()进行部署
  
 ```
/**
	 *
	 *
	 * @param slotProvider  The resource provider from which the slots are allocated
	 * @param timeout       The maximum time that the deployment may take, before a
	 *                      TimeoutException is thrown.
	 * @returns Future which is completed once the {@link ExecutionGraph} has been scheduled.
	 * The future can also be completed exceptionally if an error happened.
	 */
	private CompletableFuture<Void> scheduleEager(SlotProvider slotProvider, final Time timeout) {
		checkState(state == JobStatus.RUNNING, "job is not running currently");

		// Important: reserve all the space we need up front.
		// that way we do not have any operation that can fail between allocating the slots
		// and adding them to the list. If we had a failure in between there, that would
		// cause the slots to get lost
		final boolean queued = allowQueuedScheduling;

		// collecting all the slots may resize and fail in that operation without slots getting lost
		final ArrayList<CompletableFuture<Execution>> allAllocationFutures = new ArrayList<>(getNumberOfExecutionJobVertices());

		final Set<AllocationID> allPreviousAllocationIds =
			Collections.unmodifiableSet(computeAllPriorAllocationIdsIfRequiredByScheduling());

		// allocate the slots (obtain all their futures
		for (ExecutionJobVertex ejv : getVerticesTopologically()) {
			// these calls are not blocking, they only return futures
			Collection<CompletableFuture<Execution>> allocationFutures = ejv.allocateResourcesForAll(
				slotProvider,
				queued,
				LocationPreferenceConstraint.ALL,
				allPreviousAllocationIds,
				timeout);

			allAllocationFutures.addAll(allocationFutures);
		}

		// this future is complete once all slot futures are complete.
		// the future fails once one slot future fails.
		final ConjunctFuture<Collection<Execution>> allAllocationsFuture = FutureUtils.combineAll(allAllocationFutures);

		final CompletableFuture<Void> currentSchedulingFuture = allAllocationsFuture
			.thenAccept(
				(Collection<Execution> executionsToDeploy) -> {
					for (Execution execution : executionsToDeploy) {
						try {
							execution.deploy();
						} catch (Throwable t) {
							throw new CompletionException(
								new FlinkException(
									String.format("Could not deploy execution %s.", execution),
									t));
						}
					}
				})
			// Generate a more specific failure message for the eager scheduling
			.exceptionally(
				(Throwable throwable) -> {
					final Throwable strippedThrowable = ExceptionUtils.stripCompletionException(throwable);
					final Throwable resultThrowable;

					if (strippedThrowable instanceof TimeoutException) {
						int numTotal = allAllocationsFuture.getNumFuturesTotal();
						int numComplete = allAllocationsFuture.getNumFuturesCompleted();
						String message = "Could not allocate all requires slots within timeout of " +
							timeout + ". Slots required: " + numTotal + ", slots allocated: " + numComplete;

						resultThrowable = new NoResourceAvailableException(message);
					} else {
						resultThrowable = strippedThrowable;
					}

					throw new CompletionException(resultThrowable);
				});

		return currentSchedulingFuture;
	}

 ```
 
### ExecutionState   
 - 由于(Source、Window、Sink)都是做为Execution对象来运行，先来了解一下Execution有哪些状态，即状态的流转，方便理解流程 
 - Execution状态的流转为: CREATED(已创建)  -> SCHEDULED(已调度) -> DEPLOYING(部署中) -> RUNNING(运行中) -> FINISHED(已完成) 等，以下ExecutionState中有详细说明
 
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
### Execution.deploy()
- 对Execution进行部署
- 更新Execution状态,将当前Execution的状态由SCHEDULED更新为DEPLOYING，即由已调度状态更新为部署中

    ```
    transitionState(previous, DEPLOYING)
    ```
- INFO日志输出：部署哪一个Execution到哪一台机器上
    ```
    LOG.info(String.format("Deploying %s (attempt #%d) to %s", 
    ```
    
    ```
    13:11:55,910 INFO  [flink-akka.actor.default-dispatcher-3] org.apache.flink.runtime.executiongraph.Execution.deploy(Execution.java:599)      - Deploying Source: Socket Stream -> Flat Map -> Map (1/1) (attempt #0) to localhost
    ```

- 构建TaskDeploymentDescriptor对象，该对象引用Task实例Execution的id,slot(槽位),就可以确定Execution在哪个slot上运行

    ```
    final TaskDeploymentDescriptor deployment = vertex.createDeploymentDescriptor(
				attemptId,
				slot,
				taskRestore,
				attemptNumber);
    ```
- slot得到TaskManager

    ```
    final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();
    ```
- TaskManager.submitTask 提交任务，参数为TaskDeploymentDescriptor

    ```
    final CompletableFuture<Acknowledge> submitResultFuture = taskManagerGateway.submitTask(deployment, rpcTimeout);
    ```
- 接下来就交给TaskManager去处理了    
- 源码
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
- TaskManager中是由TaskExecutor来提交任务
- 将外部化数据从BLOB存储加载回对象

    ```
    // re-integrate offloaded data:
			try {
				tdd.loadBigData(blobCacheService.getPermanentBlobService());
			} catch (IOException | ClassNotFoundException e) {
				throw new TaskSubmissionException("Could not re-integrate offloaded TaskDeploymentDescriptor data.", e);
			}
    ```
- 从序列化的对象中反序列化(通过类加载)，JobInformation，TaskInformation，用来构建TaskInformation,Task

    ```
    	// deserialize the pre-serialized information
			final JobInformation jobInformation;
			final TaskInformation taskInformation;
			try {
				jobInformation = tdd.getSerializedJobInformation().deserializeValue(getClass().getClassLoader());
				taskInformation = tdd.getSerializedTaskInformation().deserializeValue(getClass().getClassLoader());
			} catch (IOException | ClassNotFoundException e) {
				throw new TaskSubmissionException("Could not deserialize the job or task information.", e);
			}
    ```

- 指定Source中的拆分器，就是将不断产生数据的Source拆分给不同的Window做并行任务(RpcInputSplitProvider是其中的一种分配方式)

    ```
    InputSplitProvider inputSplitProvider = new RpcInputSplitProvider(
				jobManagerConnection.getJobManagerGateway(),
				taskInformation.getJobVertexId(),
				tdd.getExecutionAttemptId(),
				taskManagerConfiguration.getTimeout());
    ```

- 构建任务状态管理器TaskStateManager

    ```
    final TaskStateManager taskStateManager = new TaskStateManagerImpl(
				jobId,
				tdd.getExecutionAttemptId(),
				localStateStore,
				taskRestore,
				checkpointResponder);
    ```

- 构建任务Task

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

- 将任务增加到任务槽位中

    ```
    			try {
				taskAdded = taskSlotTable.addTask(task);
			} catch (SlotNotFoundException | SlotNotActiveException e) {
				throw new TaskSubmissionException("Could not submit task.", e);
			}
    ```

- 调用任务的启动线程，该方法会触发调用Task.run()函数，

    ```
    		if (taskAdded) {
				task.startTaskThread();

				return CompletableFuture.completedFuture(Acknowledge.get());
			} else {
				final String message = "TaskManager already contains a task for id " +
					task.getExecutionId() + '.';

				log.debug(message);
				throw new TaskSubmissionException(message);
			}
    ```
    
- 源码

```
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

### Task.run()
- 先来了解一下任务的概念,Task表示在TaskManager上执行并行子任务。 Task包装Flink操作符（可以是用户函数）并运行它，提供所有必需的服务，例如使用输入数据，生成结果（中间结果分区）并与JobManager通信。
Flink运算符（作为AbstractInvokable的子类实现，只有数据读取器，写入程序和某些事件回调。该任务将这些操作连接到网络堆栈和actor消息，并跟踪执行状态并处理异常。
任务不知道它们与其他任务的关系，或者它们是第一次执行任务还是重复尝试。 所有这些只有JobManager知道。 所有任务都知道它自己的可运行代码，任务的配置以及要使用和生成的中间结果的ID（如果有的话）。
每个任务由一个专用线程运行。
- run()是引导任务并执行其代码的核心工作方法

- 这里是Task的执行状态，前面是Executition的执行状态，需要区分开来,更新任务状态，由CREATED(已创建)到DEPLOYING(部署中)

    ```
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
			
    ```

- 创建文件系统流为这个任务

    ```
    // activate safety net for task thread
			LOG.info("Creating FileSystem stream leak safety net for task {}", this);
			FileSystemSafetyNet.initializeSafetyNetForThread();
    ```

- 加载用户程序jar文件，给当前Task使用

    ```
    // first of all, get a user-code classloader
			// this may involve downloading the job's JAR files and/or classes
			LOG.info("Loading JAR files for task {}.", this);

			userCodeClassLoader = createUserCodeClassloader();
			final ExecutionConfig executionConfig = serializedExecutionConfig.deserializeValue(userCodeClassLoader);
    ```
    
- 注册网络追踪给这当前任务

    ```
    // ----------------------------------------------------------------
			// register the task with the network stack
			// this operation may fail if the system does not have enough
			// memory to run the necessary data exchanges
			// the registration must also strictly be undone
			// ----------------------------------------------------------------

			LOG.info("Registering task at network: {}.", this);

			network.registerTask(this);

    ```
    
- 给当前任务构建运行环境

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
- 加载并实例化任务的可调用代码(用户代码)

    ```
    // now load and instantiate the task's invokable code
			invokable = loadAndInstantiateInvokable(userCodeClassLoader, nameOfInvokableClass, env);
    ```

- 更新当前任务状态,从DEPLOYING(部署中)更新为RUNNING(运行中)

    ```
    			// switch to the RUNNING state, if that fails, we have been canceled/failed in the meantime
			if (!transitionState(ExecutionState.DEPLOYING, ExecutionState.RUNNING)) {
				throw new CancelTaskException();
			}

    ```
    
- StreamTask.invoke()

    ```
    	// run the invokable
			invokable.invoke();
    ```
        

- 源码

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

### StreamTask.invoke()

- 创建一个后端状态，stateBackend,此时为MemoryStateBackend

    ```
    stateBackend = createStateBackend();
    ```
    
- 如果没有调置时间服务，就创建SystemProcessingTimeService,它将当前处理时间指定为调用的结果(时间)

    ```
    			// if the clock is not already set, then assign a default TimeServiceProvider
			if (timerService == null) {
				ThreadFactory timerThreadFactory = new DispatcherThreadFactory(TRIGGER_THREAD_GROUP,
					"Time Trigger for " + getName(), getUserCodeClassLoader());

				timerService = new SystemProcessingTimeService(this, getCheckpointLock(), timerThreadFactory);
			}
    ```
    
- 当前流任务对应的操作链条，此处不同的流任务对应的操作链条不一样，像source流中，用户自定义的函数链不一样，这个operatorChain也不一样，这里以WordCount为例说明

    ```
    operatorChain = new OperatorChain<>(this, streamRecordWriters);
    ```
- Source流中的操作链条 operatorChain.allOperators
- headOperator = operatorChain.getHeadOperator()为StreamSource

    ```
    allOperators = {StreamOperator[3]@5784} 
 0 = {StreamMap@5793} 
 1 = {StreamFlatMap@5794} 
 2 = {StreamSource@5789} 
    ```

- 任务初使化

    ```
    // task specific initialization
			init();
    ```
    
- 在所有的operators是opened之前所有的触发器调度不能被执行，就是需要先把operator.open

    ```
    			// we need to make sure that any triggers scheduled in open() cannot be
			// executed before all operators are opened
			synchronized (lock) {

				// both the following operations are protected by the lock
				// so that we avoid race conditions in the case that initializeState()
				// registers a timer, that fires before the open() is called.

				initializeState();
				openAllOperators();
			}
    ```

- 调用具体任务的run()函数去处理，这里分不同的类型
    - Source 调的是SourceStreamTask.run()函数
    - Window 调的是OneInputStreamTask.run()函数

    ```
    	// let the task do its work
			isRunning = true;
			run();
    ```
    
- 源码

```
public final void invoke() throws Exception {

		boolean disposed = false;
		try {
			// -------- Initialize ---------
			LOG.debug("Initializing {}.", getName());

			asyncOperationsThreadPool = Executors.newCachedThreadPool();

			CheckpointExceptionHandlerFactory cpExceptionHandlerFactory = createCheckpointExceptionHandlerFactory();

			synchronousCheckpointExceptionHandler = cpExceptionHandlerFactory.createCheckpointExceptionHandler(
				getExecutionConfig().isFailTaskOnCheckpointError(),
				getEnvironment());

			asynchronousCheckpointExceptionHandler = new AsyncCheckpointExceptionHandler(this);

			stateBackend = createStateBackend();
			checkpointStorage = stateBackend.createCheckpointStorage(getEnvironment().getJobID());

			// if the clock is not already set, then assign a default TimeServiceProvider
			if (timerService == null) {
				ThreadFactory timerThreadFactory = new DispatcherThreadFactory(TRIGGER_THREAD_GROUP,
					"Time Trigger for " + getName(), getUserCodeClassLoader());

				timerService = new SystemProcessingTimeService(this, getCheckpointLock(), timerThreadFactory);
			}

			operatorChain = new OperatorChain<>(this, streamRecordWriters);
			headOperator = operatorChain.getHeadOperator();

			// task specific initialization
			init();

			// save the work of reloading state, etc, if the task is already canceled
			if (canceled) {
				throw new CancelTaskException();
			}

			// -------- Invoke --------
			LOG.debug("Invoking {}", getName());

			// we need to make sure that any triggers scheduled in open() cannot be
			// executed before all operators are opened
			synchronized (lock) {

				// both the following operations are protected by the lock
				// so that we avoid race conditions in the case that initializeState()
				// registers a timer, that fires before the open() is called.

				initializeState();
				openAllOperators();
			}

			// final check to exit early before starting to run
			if (canceled) {
				throw new CancelTaskException();
			}

			// let the task do its work
			isRunning = true;
			run();

			// if this left the run() method cleanly despite the fact that this was canceled,
			// make sure the "clean shutdown" is not attempted
			if (canceled) {
				throw new CancelTaskException();
			}

			LOG.debug("Finished task {}", getName());

			// make sure no further checkpoint and notification actions happen.
			// we make sure that no other thread is currently in the locked scope before
			// we close the operators by trying to acquire the checkpoint scope lock
			// we also need to make sure that no triggers fire concurrently with the close logic
			// at the same time, this makes sure that during any "regular" exit where still
			synchronized (lock) {
				// this is part of the main logic, so if this fails, the task is considered failed
				closeAllOperators();

				// make sure no new timers can come
				timerService.quiesce();

				// only set the StreamTask to not running after all operators have been closed!
				// See FLINK-7430
				isRunning = false;
			}

			// make sure all timers finish
			timerService.awaitPendingAfterQuiesce();

			LOG.debug("Closed operators for task {}", getName());

			// make sure all buffered data is flushed
			operatorChain.flushOutputs();

			// make an attempt to dispose the operators such that failures in the dispose call
			// still let the computation fail
			tryDisposeAllOperators();
			disposed = true;
		}
		finally {
			// clean up everything we initialized
			isRunning = false;

			// Now that we are outside the user code, we do not want to be interrupted further
			// upon cancellation. The shutdown logic below needs to make sure it does not issue calls
			// that block and stall shutdown.
			// Additionally, the cancellation watch dog will issue a hard-cancel (kill the TaskManager
			// process) as a backup in case some shutdown procedure blocks outside our control.
			setShouldInterruptOnCancel(false);

			// clear any previously issued interrupt for a more graceful shutdown
			Thread.interrupted();

			// stop all timers and threads
			tryShutdownTimerService();

			// stop all asynchronous checkpoint threads
			try {
				cancelables.close();
				shutdownAsyncThreads();
			}
			catch (Throwable t) {
				// catch and log the exception to not replace the original exception
				LOG.error("Could not shut down async checkpoint threads", t);
			}

			// we must! perform this cleanup
			try {
				cleanup();
			}
			catch (Throwable t) {
				// catch and log the exception to not replace the original exception
				LOG.error("Error during cleanup of stream task", t);
			}

			// if the operators were not disposed before, do a hard dispose
			if (!disposed) {
				disposeAllOperators();
			}

			// release the output resources. this method should never fail.
			if (operatorChain != null) {
				// beware: without synchronization, #performCheckpoint() may run in
				//         parallel and this call is not thread-safe
				synchronized (lock) {
					operatorChain.releaseOutputs();
				}
			}
		}
	}
```


### SourceStreamTask.run()
- headOperator,会依次从StreamSource.operatorChain中调用(StreamSource,StreamFlatMap,StreamMap),这个就是链式调用，把这一个类型的任务，可以依次调用执行对应的operator，不需要每次一次operator输出中间结果
- StreamSource操作会调用SocketTextStreamFunction.run()函数来处理
- 源码

```
	protected void run() throws Exception {
		headOperator.run(getCheckpointLock(), getStreamStatusMaintainer());
	}

```

### SocketTextStreamFunction.run()
- 建立Source的Sorcket连接，读取流中的数据，每次读取8K的数据放到缓存中，再按行进行解析
- 把一行数据放到ctx.collect(record);进行后续的处理
- 此处调用的是NonTimestampContext.collect(record)

```
public void run(SourceContext<String> ctx) throws Exception {
		final StringBuilder buffer = new StringBuilder();
		long attempt = 0;

		while (isRunning) {

			try (Socket socket = new Socket()) {
				currentSocket = socket;

				LOG.info("Connecting to server socket " + hostname + ':' + port);
				socket.connect(new InetSocketAddress(hostname, port), CONNECTION_TIMEOUT_TIME);
				try (BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

					char[] cbuf = new char[8192];
					int bytesRead;
					while (isRunning && (bytesRead = reader.read(cbuf)) != -1) {
						buffer.append(cbuf, 0, bytesRead);
						int delimPos;
						while (buffer.length() >= delimiter.length() && (delimPos = buffer.indexOf(delimiter)) != -1) {
							String record = buffer.substring(0, delimPos);
							// truncate trailing carriage return
							if (delimiter.equals("\n") && record.endsWith("\r")) {
								record = record.substring(0, record.length() - 1);
							}
							ctx.collect(record);
							buffer.delete(0, delimPos + delimiter.length());
						}
					}
				}
			}

			// if we dropped out of this loop due to an EOF, sleep and retry
			if (isRunning) {
				attempt++;
				if (maxNumRetries == -1 || attempt < maxNumRetries) {
					LOG.warn("Lost connection to server socket. Retrying in " + delayBetweenRetries + " msecs...");
					Thread.sleep(delayBetweenRetries);
				}
				else {
					// this should probably be here, but some examples expect simple exists of the stream source
					// throw new EOFException("Reached end of stream and reconnects are not enabled.");
					break;
				}
			}
		}

		// collect trailing data
		if (buffer.length() > 0) {
			ctx.collect(buffer.toString());
		}
	}
```
### RecordWriter.emit
- numChannels 为并行度，即为DataStrea.setParallelism(2) 设置的并行度
- channelSelector.selectChannels(record, numChannels),分区算法，给当前数据分区(分区是为了给下游并行计算使用，在这里是发给不同的Window，并行计算)
- 调用KeyGroupStreamPartitioner.selectChannels具体的分区算法
- 源码

```
	public void emit(T record) throws IOException, InterruptedException {
		emit(record, channelSelector.selectChannels(record, numChannels));
	}
```

### KeyGroupStreamPartitioner.selectChannels
- 分区实现KeyGroupRangeAssignment.assignKeyToParallelOperator(key, maxParallelism, numberOfOutputChannels);	

    ```
    	分区代码
    numberOfOutputChannels: 一共分为多少个分区,即并行度为多少
    maxParallelism:最大并行度，默认为128
    key:处理的数据，对应的key的值
    
    KeyGroupRangeAssignment.assignKeyToParallelOperator(key, maxParallelism, numberOfOutputChannels);

    ```
- 源码

```
	@Override
	public int[] selectChannels(
		SerializationDelegate<StreamRecord<T>> record,
		int numberOfOutputChannels) {

		K key;
		try {
			key = keySelector.getKey(record.getInstance().getValue());
		} catch (Exception e) {
			throw new RuntimeException("Could not extract key from " + record.getInstance().getValue(), e);
		}
		returnArray[0] = KeyGroupRangeAssignment.assignKeyToParallelOperator(key, maxParallelism, numberOfOutputChannels);
		return returnArray;
	}

```

### OneInputStreamTask.run()
- StreamTask.run().run()函数调用，当为Window时调用OneInputStreamTask.run()
- 调用StreamInputProcessor.processInput()函数
- 源码

```
    protected void run() throws Exception {
		// cache processor reference on the stack, to make the code more JIT friendly
		final StreamInputProcessor<IN> inputProcessor = this.inputProcessor;

		while (running && inputProcessor.processInput()) {
			// all the work happens in the "processInput" method
		}
	}
```

### StreamInputProcessor.processInput()
- 调用BarrierTracker.getNextNonBlocked()得到一个元素(key,value)得值，也就是source进行flatMap,map 函数之后的数据，此时，还没有进行聚合操作,注意这里会得到
- 此时的数据还没有进行分配给不同的Window,当Source有数据发送过来后，就一条一条调用streamOperator.processElement(record),即WindowOperator.processElement进行处理

```
public boolean processInput() throws Exception {
		if (isFinished) {
			return false;
		}
		if (numRecordsIn == null) {
			try {
				numRecordsIn = ((OperatorMetricGroup) streamOperator.getMetricGroup()).getIOMetricGroup().getNumRecordsInCounter();
			} catch (Exception e) {
				LOG.warn("An exception occurred during the metrics setup.", e);
				numRecordsIn = new SimpleCounter();
			}
		}

		while (true) {
			if (currentRecordDeserializer != null) {
				DeserializationResult result = currentRecordDeserializer.getNextRecord(deserializationDelegate);

				if (result.isBufferConsumed()) {
					currentRecordDeserializer.getCurrentBuffer().recycleBuffer();
					currentRecordDeserializer = null;
				}

				if (result.isFullRecord()) {
					StreamElement recordOrMark = deserializationDelegate.getInstance();

					if (recordOrMark.isWatermark()) {
						// handle watermark
						statusWatermarkValve.inputWatermark(recordOrMark.asWatermark(), currentChannel);
						continue;
					} else if (recordOrMark.isStreamStatus()) {
						// handle stream status
						statusWatermarkValve.inputStreamStatus(recordOrMark.asStreamStatus(), currentChannel);
						continue;
					} else if (recordOrMark.isLatencyMarker()) {
						// handle latency marker
						synchronized (lock) {
							streamOperator.processLatencyMarker(recordOrMark.asLatencyMarker());
						}
						continue;
					} else {
						// now we can do the actual processing
						StreamRecord<IN> record = recordOrMark.asRecord();
						synchronized (lock) {
							numRecordsIn.inc();
							streamOperator.setKeyContextElement1(record);
							streamOperator.processElement(record);
						}
						return true;
					}
				}
			}

			final BufferOrEvent bufferOrEvent = barrierHandler.getNextNonBlocked();
			if (bufferOrEvent != null) {
				if (bufferOrEvent.isBuffer()) {
					currentChannel = bufferOrEvent.getChannelIndex();
					currentRecordDeserializer = recordDeserializers[currentChannel];
					currentRecordDeserializer.setNextBuffer(bufferOrEvent.getBuffer());
				}
				else {
					// Event received
					final AbstractEvent event = bufferOrEvent.getEvent();
					if (event.getClass() != EndOfPartitionEvent.class) {
						throw new IOException("Unexpected event: " + event);
					}
				}
			}
			else {
				isFinished = true;
				if (!barrierHandler.isEmpty()) {
					throw new IllegalStateException("Trailing data in checkpoint barrier handler.");
				}
				return false;
			}
		}
	}
```


### WindowOperator.processElement(StreamRecord<IN> element)
- WindowOperator.processElement,给每一个WordWithCount(1,1) 这样的元素分配window,也就是确认每一个元素属于哪一个窗口，因为需要对同一个窗口的相同key进行聚合操作  
    ```aidl
    final Collection<W> elementWindows = windowAssigner.assignWindows(
                element.getValue(), element.getTimestamp(), windowAssignerContext);
    ```
- 把当前元素增加到state中保存,add函数中会对相同key进行聚合操作(reduce),对同一个window中相同key进行求和就是在这个方法中进行的
    ```aidl
    windowState.add(element.getValue());
    ```
- triggerContext.onElement(element),对当前元素设置trigger,也就是当前元素的window在哪个时间点触发(结束的时间点),
  把当前元素的key,增加到InternalTimerServiceImpl.processingTimeTimersQueue中，每一条数据会加一次，加完后会去重，相当于Set,对相同Key的处理，
  后面发送给Sink的数据，就是遍历这个processingTimeTimersQueue中的数据，当然，每次发送第一个元素，发送后，会把最后一个元素放到第一个元素

    ```aidl
    TriggerResult triggerResult = triggerContext.onElement(element);
    ```

```aidl

public void processElement(StreamRecord<IN> element) throws Exception {
		final Collection<W> elementWindows = windowAssigner.assignWindows(
			element.getValue(), element.getTimestamp(), windowAssignerContext);

		//if element is handled by none of assigned elementWindows
		boolean isSkippedElement = true;

		final K key = this.<K>getKeyedStateBackend().getCurrentKey();

		if (windowAssigner instanceof MergingWindowAssigner) {
			MergingWindowSet<W> mergingWindows = getMergingWindowSet();

			for (W window: elementWindows) {

				// adding the new window might result in a merge, in that case the actualWindow
				// is the merged window and we work with that. If we don't merge then
				// actualWindow == window
				W actualWindow = mergingWindows.addWindow(window, new MergingWindowSet.MergeFunction<W>() {
					@Override
					public void merge(W mergeResult,
							Collection<W> mergedWindows, W stateWindowResult,
							Collection<W> mergedStateWindows) throws Exception {

						if ((windowAssigner.isEventTime() && mergeResult.maxTimestamp() + allowedLateness <= internalTimerService.currentWatermark())) {
							throw new UnsupportedOperationException("The end timestamp of an " +
									"event-time window cannot become earlier than the current watermark " +
									"by merging. Current watermark: " + internalTimerService.currentWatermark() +
									" window: " + mergeResult);
						} else if (!windowAssigner.isEventTime() && mergeResult.maxTimestamp() <= internalTimerService.currentProcessingTime()) {
							throw new UnsupportedOperationException("The end timestamp of a " +
									"processing-time window cannot become earlier than the current processing time " +
									"by merging. Current processing time: " + internalTimerService.currentProcessingTime() +
									" window: " + mergeResult);
						}

						triggerContext.key = key;
						triggerContext.window = mergeResult;

						triggerContext.onMerge(mergedWindows);

						for (W m: mergedWindows) {
							triggerContext.window = m;
							triggerContext.clear();
							deleteCleanupTimer(m);
						}

						// merge the merged state windows into the newly resulting state window
						windowMergingState.mergeNamespaces(stateWindowResult, mergedStateWindows);
					}
				});

				// drop if the window is already late
				if (isWindowLate(actualWindow)) {
					mergingWindows.retireWindow(actualWindow);
					continue;
				}
				isSkippedElement = false;

				W stateWindow = mergingWindows.getStateWindow(actualWindow);
				if (stateWindow == null) {
					throw new IllegalStateException("Window " + window + " is not in in-flight window set.");
				}

				windowState.setCurrentNamespace(stateWindow);
				windowState.add(element.getValue());

				triggerContext.key = key;
				triggerContext.window = actualWindow;

				TriggerResult triggerResult = triggerContext.onElement(element);

				if (triggerResult.isFire()) {
					ACC contents = windowState.get();
					if (contents == null) {
						continue;
					}
					emitWindowContents(actualWindow, contents);
				}

				if (triggerResult.isPurge()) {
					windowState.clear();
				}
				registerCleanupTimer(actualWindow);
			}

			// need to make sure to update the merging state in state
			mergingWindows.persist();
		} else {
			for (W window: elementWindows) {

				// drop if the window is already late
				if (isWindowLate(window)) {
					continue;
				}
				isSkippedElement = false;

				windowState.setCurrentNamespace(window);
				windowState.add(element.getValue());

				triggerContext.key = key;
				triggerContext.window = window;

				TriggerResult triggerResult = triggerContext.onElement(element);

				if (triggerResult.isFire()) {
					ACC contents = windowState.get();
					if (contents == null) {
						continue;
					}
					emitWindowContents(window, contents);
				}

				if (triggerResult.isPurge()) {
					windowState.clear();
				}
				registerCleanupTimer(window);
			}
		}

		// side output input event if
		// element not handled by any window
		// late arriving tag has been set
		// windowAssigner is event time and current timestamp + allowed lateness no less than element timestamp
		if (isSkippedElement && isElementLate(element)) {
			if (lateDataOutputTag != null){
				sideOutput(element);
			} else {
				this.numLateRecordsDropped.inc();
			}
		}
	}


```
### InternalTimerServiceImpl.onProcessingTime 
- processingTimeTimersQueue(HeapPriorityQueueSet) 该对象中存储了所有的key,这些key是去重后，按处理顺序排序
- processingTimeTimersQueue.peek() 取出第一条数据进行处理
- processingTimeTimersQueue.poll();会移除第一条数据，并且，拿最后一条数据，放第1一个元素，导致，所有元素的处理顺序是，先处理第一个元素，然后，把最后一个元素放第一个，
   最后一个就置为空，再循环处理所有数据，相当于处理完第一个元素，处后从最后一个元素开始处理，一直处理到完成,举例
   
   ```aidl
   1 2 1 3 2 5 4
   存为 1 2 3 5 4 
   顺序就变为
    1
    4
    5
    3
    2
   ```
- keyContext.setCurrentKey(timer.getKey());//设置当前的key,当前需要处理的
- triggerTarget.onProcessingTime(timer);// 调用 WindowOperator.onProcessingTime(timer)处理  
      
```aidl
queue = {HeapPriorityQueueElement[129]@8184} 
 1 = {TimerHeapInternalTimer@12441} "Timer{timestamp=1551505439999, key=(1), namespace=TimeWindow{start=1551505380000, end=1551505440000}}"
 2 = {TimerHeapInternalTimer@12442} "Timer{timestamp=1551505439999, key=(2), namespace=TimeWindow{start=1551505380000, end=1551505440000}}"
 3 = {TimerHeapInternalTimer@12443} "Timer{timestamp=1551505439999, key=(3), namespace=TimeWindow{start=1551505380000, end=1551505440000}}"
 5 = {TimerHeapInternalTimer@12443} "Timer{timestamp=1551505439999, key=(3), namespace=TimeWindow{start=1551505380000, end=1551505440000}}"
 4 = {TimerHeapInternalTimer@12443} "Timer{timestamp=1551505439999, key=(3), namespace=TimeWindow{start=1551505380000, end=1551505440000}}"

```  
- 调用 WindowOperator.onProcessingTime(timer)处理当前key;



```aidl

public void onProcessingTime(long time) throws Exception {
		// null out the timer in case the Triggerable calls registerProcessingTimeTimer()
		// inside the callback.
		nextTimer = null;

		InternalTimer<K, N> timer;

		while ((timer = processingTimeTimersQueue.peek()) != null && timer.getTimestamp() <= time) {
			processingTimeTimersQueue.poll();
			keyContext.setCurrentKey(timer.getKey());
			triggerTarget.onProcessingTime(timer);
		}

		if (timer != null && nextTimer == null) {
			nextTimer = processingTimeService.registerTimer(timer.getTimestamp(), this);
		}
	}
```


### WindowOperator.onProcessingTime
- triggerResult.isFire()// 当前元素对应的window已经可以发射了，即过了结束时间
- windowState.get() //取出当前key对应的(key,value)此时已经是相同key聚合后的值
- emitWindowContents(triggerContext.window, contents);//发送给Sink进行处理

```aidl
public void onProcessingTime(InternalTimer<K, W> timer) throws Exception {
		triggerContext.key = timer.getKey();
		triggerContext.window = timer.getNamespace();

		MergingWindowSet<W> mergingWindows;

		if (windowAssigner instanceof MergingWindowAssigner) {
			mergingWindows = getMergingWindowSet();
			W stateWindow = mergingWindows.getStateWindow(triggerContext.window);
			if (stateWindow == null) {
				// Timer firing for non-existent window, this can only happen if a
				// trigger did not clean up timers. We have already cleared the merging
				// window and therefore the Trigger state, however, so nothing to do.
				return;
			} else {
				windowState.setCurrentNamespace(stateWindow);
			}
		} else {
			windowState.setCurrentNamespace(triggerContext.window);
			mergingWindows = null;
		}

		TriggerResult triggerResult = triggerContext.onProcessingTime(timer.getTimestamp());

		if (triggerResult.isFire()) {
			ACC contents = windowState.get();
			if (contents != null) {
				emitWindowContents(triggerContext.window, contents);
			}
		}

		if (triggerResult.isPurge()) {
			windowState.clear();
		}

		if (!windowAssigner.isEventTime() && isCleanupTime(triggerContext.window, timer.getTimestamp())) {
			clearAllState(triggerContext.window, windowState, mergingWindows);
		}

		if (mergingWindows != null) {
			// need to make sure to update the merging state in state
			mergingWindows.persist();
		}
	}
```


### SingleInputGate  
- 中间数据处理流程(数据交互)

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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.deployment.InputChannelDeploymentDescriptor;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionLocation;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.taskmanager.TaskActions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Timer;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An input gate consumes one or more partitions of a single produced intermediate result.
 *
 * <p>Each intermediate result is partitioned over its producing parallel subtasks; each of these
 * partitions is furthermore partitioned into one or more subpartitions.
 *
 * <p>As an example, consider a map-reduce program, where the map operator produces data and the
 * reduce operator consumes the produced data.
 *
 * <pre>{@code
 * +-----+              +---------------------+              +--------+
 * | Map | = produce => | Intermediate Result | <= consume = | Reduce |
 * +-----+              +---------------------+              +--------+
 * }</pre>
 *
 * <p>When deploying such a program in parallel, the intermediate result will be partitioned over its
 * producing parallel subtasks; each of these partitions is furthermore partitioned into one or more
 * subpartitions.
 *
 * <pre>{@code
 *                            Intermediate result
 *               +-----------------------------------------+
 *               |                      +----------------+ |              +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 1 | | <=======+=== | Input Gate | Reduce 1 |
 * | Map 1 | ==> | | Partition 1 | =|   +----------------+ |         |    +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 2 | | <==+    |
 *               |                      +----------------+ |    |    | Subpartition request
 *               |                                         |    |    |
 *               |                      +----------------+ |    |    |
 * +-------+     | +-------------+  +=> | Subpartition 1 | | <==+====+
 * | Map 2 | ==> | | Partition 2 | =|   +----------------+ |    |         +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 2 | | <==+======== | Input Gate | Reduce 2 |
 *               |                      +----------------+ |              +-----------------------+
 *               +-----------------------------------------+
 * }</pre>
 *
 * <p>In the above example, two map subtasks produce the intermediate result in parallel, resulting
 * in two partitions (Partition 1 and 2). Each of these partitions is further partitioned into two
 * subpartitions -- one for each parallel reduce subtask.
 */
public class SingleInputGate implements InputGate {
```
  


end    
    