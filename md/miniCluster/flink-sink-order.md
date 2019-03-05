# Flink Sink 接收数据的顺序(Window发送数据顺序)

## 概述
- InternalTimerServiceImpl.processingTimeTimersQueue存储着同一个Window中所有Key,取第一个key，调用WindowOperator.onProcessingTime进行处理,并发送给Sink
- InternalTimerServiceImpl.processingTimeTimersQueue key处理的顺序是，先处理第一个，然后依次把最后一个元素放到第一个元素进行处理
- Key,处理的顺序，如 1 2 3 5 4,就会变成

   ```aidl
    1
    4
    5
    3
    2
   ```
- 源码: https://github.com/opensourceteams/flink-maven-scala




## 输入数据
```aidl
1 2 1 3 2 5 4
```




## 源码分析

### RecordWriter.emit
- 当WordCount中的数据经过Operator(Source,FlatMap,Map) 处理后，通过RecordWriter.emit()函数发射数据
- 此时发这样的数据格式发送

    ```aidlf
    WordWithCount(1,1)
    WordWithCount(2,1)
    WordWithCount(1,1)
    WordWithCount(3,1)
    WordWithCount(2,1)
    WordWithCount(5,1)
    WordWithCount(4,1)   
    
    ```
- WindowOperator.processElement会接收并处理    

```aidl

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
  
  