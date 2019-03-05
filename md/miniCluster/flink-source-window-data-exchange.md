# Flink1.7.2  Source、Window数据交互源码分析

## 源码
- https://github.com/opensourceteams/fink-maven-scala-2

## 概述

## StreamGraph 图
- https://github.com/opensourceteams/fink-maven-scala-2/blob/master/md/images/wordCount/flink-streamGraph-3.png
!()[https://github.com/opensourceteams/fink-maven-scala-2/blob/master/md/images/wordCount/flink-streamGraph-3.png]

## 输入数据

```
1 2 3 4 5 6 7 8 9 10
```

## WordCount程序

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
      println(env.getExecutionPlan)
      println("==================================以上为执行计划 JSON串==================================\n")
      //StreamGraph
     //println(env.getStreamGraph.getStreamingPlanAsJSON)



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


## 源码分析(Source)

### RecordWriter.emit
- record 数据为: WordWithCount(1,1)
- 也就是Source.通过Socket.connect 得到流数据后，按行拆分，进行flatMap,map函数之后，得到的数据
- numChannels为Source partitions，这个并行度是在DataStream.setParallelism(3)设置的
- channelSelector.selectChannels(record, numChannels)，这个是按key,进行hash,算得当前元素所分的partition,即为change
- 续续调用 RecordWriter.emit()

```

	public void emit(T record) throws IOException, InterruptedException {
		emit(record, channelSelector.selectChannels(record, numChannels));
	}

```


### RecordWriter.emit
- 经调试，按key,hash % 并行度,分配的数据如下

    ```
    key:2     partition:0
    key:3     partition:0
    key:4     partition:0
    key:6     partition:0
    
    key:1     partition:1
    key:5     partition:1
    key:8     partition:1
    key:10    partition:1
    
    key:7     partition:2
    key:9     partition:2
    ```
- record进行序列化，数据长度写进ByteBuffer lengthBuffer,数据写进ByteBuffer dataBuffer;
 
    ```
	serializer.serializeRecord(record);
    ```
- 调用RecordWriter.copyFromSerializerToTargetChannel(channel)往通道中写数据

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

### RecordWriter.copyFromSerializerToTargetChannel
- getBufferBuilder(targetChannel)通过channel,得到BufferBuilder,就是得到当前的partition写入数据对象BufferBuilder,其实就是操作ResultPartition.subPartitions
 
    ```
    BufferBuilder bufferBuilder = getBufferBuilder(targetChannel);
    ```
- 每个partition写入的数据，对应到各自的window,这样就实现了，在source端将数据分区，对应的window处理source对应的分区数据
    ```
    subpartitions = {ResultSubpartition[3]@5749} 
 0 = {PipelinedSubpartition@5771} "PipelinedSubpartition#0 [number of buffers: 0 (0 bytes), number of buffers in backlog: 0, finished? false, read view? true]"
 1 = {PipelinedSubpartition@5772} "PipelinedSubpartition#1 [number of buffers: 0 (0 bytes), number of buffers in backlog: 0, finished? false, read view? true]"
 2 = {PipelinedSubpartition@5773} "PipelinedSubpartition#2 [number of buffers: 0 (0 bytes), number of buffers in backlog: 0, finished? false, read view? true]"
    ```

- BufferConsumer是消费BufferBuilder中的数据，BufferBuilder.append 是产生数据


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

### rdWrecoiter.flushAll
- 把bufferBuilders存储着BufferBuilder 数组，数组个数，对应着并行度的个数，每个BufferBuilder对应着ResultSubpartition.subpartitions的partition的PipelinedSubpartition,会对这些PipelinedSubpartition进行Flush,并给对应的Window进行处理
- 调用PipelinedSubpartition.flushAll()

```
	public void flushAll() {
		targetPartition.flushAll();
	}
```

### PipelinedSubpartition.flushAll()
- 调用PipelinedSubpartition.notifyDataAvailable
- 调用PipelinedSubpartitionView.notifyDataAvailable

```
public void flush() {
		final boolean notifyDataAvailable;
		synchronized (buffers) {
			if (buffers.isEmpty()) {
				return;
			}
			// if there is more then 1 buffer, we already notified the reader
			// (at the latest when adding the second buffer)
			notifyDataAvailable = !flushRequested && buffers.size() == 1;
			flushRequested = true;
		}
		if (notifyDataAvailable) {
			notifyDataAvailable();
		}
	}
```

### PipelinedSubpartitionView.notifyDataAvailable
- 调用LocalInputChannel.notifyDataAvailable

```
	public void notifyDataAvailable() {
		availabilityListener.notifyDataAvailable();
	}
```

### LocalInputChannel.notifyDataAvailable
- 调用InputChannel.notifyChannelNonEmpty()
- 调用SingleInputGate.notifyChannelNonEmpty
```
	public void notifyDataAvailable() {
		notifyChannelNonEmpty();
	}
```
### SingleInputGate.queueChannel
- 通知Window，有数据产生了，可以开始消费了(处理数据)
    ```
      inputChannelsWithData.notifyAll();
    ```

```
private void queueChannel(InputChannel channel) {
		int availableChannels;

		synchronized (inputChannelsWithData) {
			if (enqueuedInputChannelsWithData.get(channel.getChannelIndex())) {
				return;
			}
			availableChannels = inputChannelsWithData.size();

			inputChannelsWithData.add(channel);
			enqueuedInputChannelsWithData.set(channel.getChannelIndex());

			if (availableChannels == 0) {
				inputChannelsWithData.notifyAll();
			}
		}

		if (availableChannels == 0) {
			InputGateListener listener = inputGateListener;
			if (listener != null) {
				listener.notifyInputGateNonEmpty(this);
			}
		}
	}
```



## 源码分析(Window)

### OneInputStreamTask.run()
- 调用StreamInputProcessor.processInput()进行处理，这个函数，会进行阻塞，如果没有读到数据的话
- 注意，这是Window的处理线程，有多少个并行度，就会开多少个Window,就有多少个这个方法在一直处理Source不断发过来的数据

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
- 这是一个阻塞的方法，读取Source中对应的partition中的数据,调用BarrierTracker.getNextNonBlocked()
 
    ```
    final BufferOrEvent bufferOrEvent = barrierHandler.getNextNonBlocked();
    ```

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

### BarrierTracker.getNextNonBlocked()
- 调用inputGate.getNextBufferOrEvent();调到的是SingleInputGate.getNextBufferOrEvent()去读取Source发过来到当前Window中的数据
 
```
	public BufferOrEvent getNextNonBlocked() throws Exception {
		while (true) {
			Optional<BufferOrEvent> next = inputGate.getNextBufferOrEvent();
			if (!next.isPresent()) {
				// buffer or input exhausted
				return null;
			}

			BufferOrEvent bufferOrEvent = next.get();
			if (bufferOrEvent.isBuffer()) {
				return bufferOrEvent;
			}
			else if (bufferOrEvent.getEvent().getClass() == CheckpointBarrier.class) {
				processBarrier((CheckpointBarrier) bufferOrEvent.getEvent(), bufferOrEvent.getChannelIndex());
			}
			else if (bufferOrEvent.getEvent().getClass() == CancelCheckpointMarker.class) {
				processCheckpointAbortBarrier((CancelCheckpointMarker) bufferOrEvent.getEvent(), bufferOrEvent.getChannelIndex());
			}
			else {
				// some other event
				return bufferOrEvent;
			}
		}
	}
```

### SingleInputGate.getNextBufferOrEvent
- 这个才真正读Source发过来对应的partition,对应当前的Window中的数据，currentChannel.getNextBuffer();
- inputChannelsWithData 这个对象进行线程阻塞，通知，就是一个开关，Source发过来的数据处理完了就关了，Source有新的数据发过来，就打开了，开关打开时，就可以进行Window数据处理了

```
private Optional<BufferOrEvent> getNextBufferOrEvent(boolean blocking) throws IOException, InterruptedException {
		if (hasReceivedAllEndOfPartitionEvents) {
			return Optional.empty();
		}

		if (isReleased) {
			throw new IllegalStateException("Released");
		}

		requestPartitions();

		InputChannel currentChannel;
		boolean moreAvailable;
		Optional<BufferAndAvailability> result = Optional.empty();

		do {
			synchronized (inputChannelsWithData) {
				while (inputChannelsWithData.size() == 0) {
					if (isReleased) {
						throw new IllegalStateException("Released");
					}

					if (blocking) {
						inputChannelsWithData.wait();
					}
					else {
						return Optional.empty();
					}
				}

				currentChannel = inputChannelsWithData.remove();
				enqueuedInputChannelsWithData.clear(currentChannel.getChannelIndex());
				moreAvailable = !inputChannelsWithData.isEmpty();
			}

			result = currentChannel.getNextBuffer();
		} while (!result.isPresent());

		// this channel was now removed from the non-empty channels queue
		// we re-add it in case it has more data, because in that case no "non-empty" notification
		// will come for that channel
		if (result.get().moreAvailable()) {
			queueChannel(currentChannel);
			moreAvailable = true;
		}

		final Buffer buffer = result.get().buffer();
		if (buffer.isBuffer()) {
			return Optional.of(new BufferOrEvent(buffer, currentChannel.getChannelIndex(), moreAvailable));
		}
		else {
			final AbstractEvent event = EventSerializer.fromBuffer(buffer, getClass().getClassLoader());

			if (event.getClass() == EndOfPartitionEvent.class) {
				channelsWithEndOfPartitionEvents.set(currentChannel.getChannelIndex());

				if (channelsWithEndOfPartitionEvents.cardinality() == numberOfInputChannels) {
					// Because of race condition between:
					// 1. releasing inputChannelsWithData lock in this method and reaching this place
					// 2. empty data notification that re-enqueues a channel
					// we can end up with moreAvailable flag set to true, while we expect no more data.
					checkState(!moreAvailable || !pollNextBufferOrEvent().isPresent());
					moreAvailable = false;
					hasReceivedAllEndOfPartitionEvents = true;
				}

				currentChannel.notifySubpartitionConsumed();

				currentChannel.releaseAllResources();
			}

			return Optional.of(new BufferOrEvent(event, currentChannel.getChannelIndex(), moreAvailable));
		}
	}
```

## 输出数据
- 注意，打印输出的数据，是有规则的，按partition打印的，而且，parition打印的顺序，是在window时按key去重，此时没有排序，然后发送给sink进行打印输出
```
--------------------------
partition:0
WordWithCount(2,1)
WordWithCount(6,1)
WordWithCount(4,1)
WordWithCount(3,1)

partition:1
WordWithCount(1,1)
WordWithCount(10,1)
WordWithCount(8,1)
WordWithCount(5,1)

partition:2
WordWithCount(7,1)
WordWithCount(9,1)


``


end