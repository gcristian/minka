package io.tilt.minka.api.config;

public class SchedulerSettings {

	public static String PNAME = "MK";
	public static String THREAD_NAME_SCHEDULER = PNAME + "Sch";
	public static String THREAD_NAME_BROKER_SERVER_GROUP = PNAME + "BrkSG";
	public static String THREAD_NAME_BROKER_SERVER_WORKER = PNAME + "BrkSW";
	public static String THREAD_NANE_TCP_BROKER_CLIENT = PNAME + "BC";

	public static String THREAD_NAME_WEBSERVER_WORKER = PNAME + "WebW";
	public static String THREAD_NAME_WEBSERVER_KERNEL = PNAME + "WebK";

	// only 1 thread for all other continuous scheduled tasks is enough
	// in case of bigger transportation payloads this can increase
	// as the Scheduler will handle permissions thru Semaphore
	public static int MAX_CONCURRENCY = 8;
	private int maxConcurrency;

	public int getMaxConcurrency() {
		return this.maxConcurrency;
	}

	public void setMaxConcurrency(int maxConcurrency) {
		this.maxConcurrency = maxConcurrency;
	}

	public static long SEMAPHORE_UNLOCK_RETRY_FREQUENCY_MS = 100l; // 50l;
	private int semaphoreUnlockRetryFrequencyMs;

	public int getSemaphoreUnlockRetryFrequencyMs() {
		return this.semaphoreUnlockRetryFrequencyMs;
	}

	public void setSemaphoreUnlockRetryFrequencyMs(int semaphoreUnlockRetryFrequencyMs) {
		this.semaphoreUnlockRetryFrequencyMs = semaphoreUnlockRetryFrequencyMs;
	}

	public static int SEMAPHORE_UNLOCK_MAX_RETRIES = 30;
	private int semaphoreUnlockMaxRetries;

	public int getSemaphoreUnlockMaxRetries() {
		return this.semaphoreUnlockMaxRetries;
	}

	public void setSemaphoreUnlockMaxRetries(int semaphoreUnlockMaxRetries) {
		this.semaphoreUnlockMaxRetries = semaphoreUnlockMaxRetries;
	}

}