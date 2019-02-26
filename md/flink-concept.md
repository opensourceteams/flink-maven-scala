# Flink 名词述语


###  JobSchedulingStatus(作业调度状态)
- PENDING: 作业还没有被调度
- RUNNING: 作业已经被调度，但是还没有完成
- DONE:    作业已经完成(成功或不成功)


```aidl
/**
	 * The scheduling status of a job, as maintained by the {@code RunningJobsRegistry}.
	 */
	enum JobSchedulingStatus {

		/** Job has not been scheduled, yet. */
		PENDING,

		/** Job has been scheduled and is not yet finished. */
		RUNNING,

		/** Job has been finished, successfully or unsuccessfully. */
		DONE;
	}

	// ----------
```