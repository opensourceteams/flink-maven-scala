# Flink1.7.2 local WordCount源码分析

## 概述
- Flink 环境 local,版本 Flink.1.7.2
- 用官网示例WordCount Scala程序分析源码
- 本文从source、operator、sink三个方面详细分析源码实现

### 时序图
- https://github.com/opensourceteams/fink-maven-scala-2/blob/master/md/images/005-source-operation-sink%E6%BA%90%E7%A0%81%E5%88%86%E6%9E%90.png
![https://github.com/opensourceteams/fink-maven-scala-2/blob/master/md/images/005-source-operation-sink%E6%BA%90%E7%A0%81%E5%88%86%E6%9E%90.png]
### 输入数据
- nc -lk 1234
```
a b a b a
```
## 客户端程序
### SocketWindowWordCountLocal.scala
```
package com.opensourceteams.module.bigdata.flink.example.stream.worldcount.nc

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


    val configuration : Configuration = new Configuration()
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
      .timeWindow(Time.seconds(20))
      //.countWindow(3)
      //.countWindow(3,1)
      //.countWindowAll(3)


      .sum("count" )

    textResult.print().setParallelism(1)



    if(args == null || args.size ==0){
      env.execute("默认作业")

      //执行计划
      //println(env.getExecutionPlan)
      //StreamGraph
     //println(env.getStreamGraph.getStreamingPlanAsJSON)



      //JsonPlanGenerator.generatePlan(jobGraph)

    }else{
      env.execute(args(0))
    }

    println("结束")

  }


  // Data type for words with count
  case class WordWithCount(word: String, count: Long)

}


```
## Flink源码分析

## Source(读取数据)

### SocketTextStreamFunction
- SocketTextStreamFunction.run函数，只要task在运行，就一直通过Socket连接流，BufferedReader.read进行读取，每次读8kb，然后对缓存中的数据进行按行处理
- NonTimestampContext.collect函数进行处理

```
@Override
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
### NonTimestampContext
- collect
- element参数为读取到source中的一行数据
- 调用AbstractStreamOperator.CountingOutput.collect

```
		public void collect(T element) {
			synchronized (lock) {
				output.collect(reuse.replace(element));
			}
		}

```

### AbstractStreamOperator.CountingOutput
- collect
- 调用CopyingChainingOutput.collect
```
		@Override
		public void collect(StreamRecord<OUT> record) {
			numRecordsOut.inc();
			output.collect(record);
		}
```

### CopyingChainingOutput.collect
- collect
- 调用pushToOperator()
```
		public void collect(StreamRecord<T> record) {
			if (this.outputTag != null) {
				// we are only responsible for emitting to the main input
				return;
			}

			pushToOperator(record);
		}
```


### pushToOperator
- 调用StreamFlatMap.processElement
```
protected <X> void pushToOperator(StreamRecord<X> record) {
			try {
				// we know that the given outputTag matches our OutputTag so the record
				// must be of the type that our operator (and Serializer) expects.
				@SuppressWarnings("unchecked")
				StreamRecord<T> castRecord = (StreamRecord<T>) record;

				numRecordsIn.inc();
				StreamRecord<T> copy = castRecord.copy(serializer.copy(castRecord.getValue()));
				operator.setKeyContextElement1(copy);
				operator.processElement(copy);
			} catch (ClassCastException e) {
				if (outputTag != null) {
					// Enrich error message
					ClassCastException replace = new ClassCastException(
						String.format(
							"%s. Failed to push OutputTag with id '%s' to operator. " +
								"This can occur when multiple OutputTags with different types " +
								"but identical names are being used.",
							e.getMessage(),
							outputTag.getId()));

					throw new ExceptionInChainedOperatorException(replace);
				} else {
					throw new ExceptionInChainedOperatorException(e);
				}
			} catch (Exception e) {
				throw new ExceptionInChainedOperatorException(e);
			}

		}
	}
```



## Operator(FlatMap)
### StreamFlatMap
- processElement
- userFunction为自定义函数，即flatMap( w => w.split("\\s") ),括号中的表达式
- element.getValue()为source中的一行数据
- 调用DataStream.flatMap
```
	public void processElement(StreamRecord<IN> element) throws Exception {
		collector.setTimestamp(element);
		userFunction.flatMap(element.getValue(), collector);
	}
```

### DataStream
- flatMap
-  cleanFun(in) 相当于是，source中的一行数据，执行完flatMap函数后返回的结果数据，然后进行foreach遍历，即取出集合中的一个元素，调用out.collect函数，即调用TimestampedCollector.collect
```
  /**
   * Creates a new DataStream by applying the given function to every element and flattening
   * the results.
   */
  def flatMap[R: TypeInformation](fun: T => TraversableOnce[R]): DataStream[R] = {
    if (fun == null) {
      throw new NullPointerException("FlatMap function must not be null.")
    }
    val cleanFun = clean(fun)
    val flatMapper = new FlatMapFunction[T, R] {
      def flatMap(in: T, out: Collector[R]) { cleanFun(in) foreach out.collect }
    }
    flatMap(flatMapper)
  }

```


## Operator(Map)
### TimestampedCollector
- collect
- 调用CountingOutput.collect()
```
	public void collect(T record) {
		output.collect(reuse.replace(record));
	}

```
### CountingOutput
- 调用CopyingChainingOutput.collect
```
	public void collect(StreamRecord<OUT> record) {
			numRecordsOut.inc();
			output.collect(record);
		}
```

### CopyingChainingOutput
- 调用函数pushToOperator()
```
		public void collect(StreamRecord<T> record) {
			if (this.outputTag != null) {
				// we are only responsible for emitting to the main input
				return;
			}

			pushToOperator(record);
		}
```

- 调用operator.processElement(copy);即StreamMap.processElement
```
protected <X> void pushToOperator(StreamRecord<X> record) {
			try {
				// we know that the given outputTag matches our OutputTag so the record
				// must be of the type that our operator (and Serializer) expects.
				@SuppressWarnings("unchecked")
				StreamRecord<T> castRecord = (StreamRecord<T>) record;

				numRecordsIn.inc();
				StreamRecord<T> copy = castRecord.copy(serializer.copy(castRecord.getValue()));
				operator.setKeyContextElement1(copy);
				operator.processElement(copy);
			} catch (ClassCastException e) {
				if (outputTag != null) {
					// Enrich error message
					ClassCastException replace = new ClassCastException(
						String.format(
							"%s. Failed to push OutputTag with id '%s' to operator. " +
								"This can occur when multiple OutputTags with different types " +
								"but identical names are being used.",
							e.getMessage(),
							outputTag.getId()));

					throw new ExceptionInChainedOperatorException(replace);
				} else {
					throw new ExceptionInChainedOperatorException(e);
				}
			} catch (Exception e) {
				throw new ExceptionInChainedOperatorException(e);
			}

		}
	}
```


### StreamMap
- userFunction 相当于map( w => WordWithCount(w,1)) 括号中的表达式
- userFunction.map(element.getValue()) 相当于，拿到Source中一行数据，进行FlatMap操作后，取集合中的一个元素，再进行flatMap操作，得到的值:(a,1)
- 再调用output.collect,即 CountingOutput.collect
```
	public void processElement(StreamRecord<IN> element) throws Exception {
		output.collect(element.replace(userFunction.map(element.getValue())));
	}
```

### CountingOutput
- 调用RecordWriterOutput.collect
```
	public void collect(StreamRecord<OUT> record) {
			numRecordsOut.inc();
			output.collect(record);
		}
```
### RecordWriterOutput
- 调用函数pushToRecordWriter
```
	public void collect(StreamRecord<OUT> record) {
		if (this.outputTag != null) {
			// we are only responsible for emitting to the main input
			return;
		}

		pushToRecordWriter(record);
	}
```

- pushToRecordWriter
- 调用StreamRecordWriter.emit
```
	private <X> void pushToRecordWriter(StreamRecord<X> record) {
		serializationDelegate.setInstance(record);

		try {
			recordWriter.emit(serializationDelegate);
		}
		catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}
```

### StreamRecordWriter
- 调用RecordWriter.emit
```
	public void emit(T record) throws IOException, InterruptedException {
		checkErroneous();
		super.emit(record);
	}
```
### RecordWriter
- 调用emit
```
	public void emit(T record) throws IOException, InterruptedException {
		emit(record, channelSelector.selectChannels(record, numChannels));
	}
```

- emit
- 调用copyFromSerializerToTargetChannel(),该函数会往Channel中写数据，会触发WindowOperator
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

- copyFromSerializerToTargetChannel
```
/**
	 * @param targetChannel
	 * @return <tt>true</tt> if the intermediate serialization buffer should be pruned
	 */
	private boolean copyFromSerializerToTargetChannel(int targetChannel) throws IOException, InterruptedException {
		// We should reset the initial position of the intermediate serialization buffer before
		// copying, so the serialization results can be copied to multiple target buffers.
		serializer.reset();

		boolean pruneTriggered = false;
		BufferBuilder bufferBuilder = getBufferBuilder(targetChannel);
		SerializationResult result = serializer.copyToBufferBuilder(bufferBuilder);
		while (result.isFullBuffer()) {
			numBytesOut.inc(bufferBuilder.finish());
			numBuffersOut.inc();

			// If this was a full record, we are done. Not breaking out of the loop at this point
			// will lead to another buffer request before breaking out (that would not be a
			// problem per se, but it can lead to stalls in the pipeline).
			if (result.isFullRecord()) {
				pruneTriggered = true;
				bufferBuilders[targetChannel] = Optional.empty();
				break;
			}

			bufferBuilder = requestNewBufferBuilder(targetChannel);
			result = serializer.copyToBufferBuilder(bufferBuilder);
		}
		checkState(!serializer.hasSerializedData(), "All data should be written at once");

		if (flushAlways) {
			targetPartition.flush(targetChannel);
		}
		return pruneTriggered;
	}
```


## window operator(reduce)
### WindowOperator
- processElement,该函数，每次source进行flatMap,map,之后，即(a,1) 这样的元素调用emit之后，就会触发该函数调用，每一个元素进行emit之后，都会调用该函数
- windowAssigner.assignWindows,把每一个元素分配给对应的window
- 把该元素存到HeapReducingState.add()中， 这个state值在WindowOperator.windowState.stateTable.primaryTable.state 这个里边存着
- add()调用transform，最终调用ReduceTransformation.apply,该函数会调用reduce函数，在同一次window中，每来一个相同key，就更新一次,实现累加,

    ```
    public V apply(V previousState, V value) throws Exception { return previousState != null ? reduceFunction.reduce(previousState, value) : value; }
    ```

- 每一个元素都关联trigger,TriggerResult triggerResult = triggerContext.onElement(element)
- triggerResult.isFire(),只有当前window完成才为true


```
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

- onProcessingTime
- 调window完成会调用onProcessingTime()函数
- WindowOperator.processElement()中triggerContext.onElement(element)，中的trigger最终当完成window时，会调用WindowOperator.onProcessingTime()
- 取state中的数据，调用emitWindowContents()函数
```
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

### emitWindowContents
```
	private void emitWindowContents(W window, ACC contents) throws Exception {
		timestampedCollector.setAbsoluteTimestamp(window.maxTimestamp());
		processContext.window = window;
		userFunction.process(triggerContext.key, window, processContext, contents, timestampedCollector);
	}
```

## SinkStream(PrintSinkFunction)

### InternalSingleValueWindowFunction
- PassThroughWindowFunction.apply
```
	public void process(KEY key, W window, InternalWindowContext context, IN input, Collector<OUT> out) throws Exception {
		wrappedFunction.apply(key, window, Collections.singletonList(input), out);
	}
```

### PassThroughWindowFunction
- TimestampedCollector.collect
```
	public void apply(K k, W window, Iterable<T> input, Collector<T> out) throws Exception {
		for (T in: input) {
			out.collect(in);
		}
	}
```

### TimestampedCollector
- AbstractStreamOperator.CountingOutput.collect
```
	public void collect(T record) {
		output.collect(reuse.replace(record));
	}

```

### AbstractStreamOperator.CountingOutput
- OperatorChain.CopyingChainingOutput.collect
```
		public void collect(StreamRecord<OUT> record) {
			numRecordsOut.inc();
			output.collect(record);
		}
```

### OperatorChain.CopyingChainingOutput
- pushToOperator
```
		public void collect(StreamRecord<T> record) {
			if (this.outputTag != null) {
				// we are only responsible for emitting to the main input
				return;
			}

			pushToOperator(record);
		}

```


- pushToOperator
- StreamSink.processElement
```

protected <X> void pushToOperator(StreamRecord<X> record) {
			try {
				// we know that the given outputTag matches our OutputTag so the record
				// must be of the type that our operator (and Serializer) expects.
				@SuppressWarnings("unchecked")
				StreamRecord<T> castRecord = (StreamRecord<T>) record;

				numRecordsIn.inc();
				StreamRecord<T> copy = castRecord.copy(serializer.copy(castRecord.getValue()));
				operator.setKeyContextElement1(copy);
				operator.processElement(copy);
			} catch (ClassCastException e) {
				if (outputTag != null) {
					// Enrich error message
					ClassCastException replace = new ClassCastException(
						String.format(
							"%s. Failed to push OutputTag with id '%s' to operator. " +
								"This can occur when multiple OutputTags with different types " +
								"but identical names are being used.",
							e.getMessage(),
							outputTag.getId()));

					throw new ExceptionInChainedOperatorException(replace);
				} else {
					throw new ExceptionInChainedOperatorException(e);
				}
			} catch (Exception e) {
				throw new ExceptionInChainedOperatorException(e);
			}

		}
	}
```



### StreamSink
- PrintSinkFunction.invoke 打印输出
```
public void processElement(StreamRecord<IN> element) throws Exception {
		sinkContext.element = element;
		userFunction.invoke(element.getValue(), sinkContext);
	}
```












end