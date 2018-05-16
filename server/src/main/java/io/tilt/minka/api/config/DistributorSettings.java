package io.tilt.minka.api.config;

public class DistributorSettings {
	public static int MAX_JOURNAL_SIZE = 10;
	
	protected static final boolean RUN_CONSISTENCY_CHECK = false;
	private boolean runConsistencyCheck;
	protected static final boolean RUN_ON_STEALTH_MODE = true;
	private boolean runOnStealthMode;
	protected static final boolean RELOAD_DUTIES_FROM_STORAGE = false;
	private boolean reloadDutiesFromStorage;
	protected static final int RELOAD_DUTIES_FROM_STORAGE_EACH_PERIODS = 10;
	private int reloadDutiesFromStorageEachPeriods;
	/* 10 seconds to let the Proctor discover all Followers before distributing */
	protected final static long START_DELAY_BEATS = 5;
	private long startDelayBeats;
	protected final static long DELAY_BEATS = 3;
	private long delayBeats;
	protected static final int PLAN_EXPIRATION_BEATS = 10;
	private int planExpirationBeats;
	
	protected static final int PLAN_MAX_RETRIES = 3;
	private int planMaxRetries;
	
	public boolean isRunConsistencyCheck() {
		return this.runConsistencyCheck;
	}
	public void setRunConsistencyCheck(boolean runConsistencyCheck) {
		this.runConsistencyCheck = runConsistencyCheck;
	}
	public boolean isRunOnStealthMode() {
		return runOnStealthMode;
	}
	public void setRunOnStealthMode(boolean runOnStealthMode) {
		this.runOnStealthMode = runOnStealthMode;
	}
	public boolean isReloadDutiesFromStorage() {
		return this.reloadDutiesFromStorage;
	}
	public void setReloadDutiesFromStorage(boolean reloadDutiesFromStorage) {
		this.reloadDutiesFromStorage = reloadDutiesFromStorage;
	}
	public int getReloadDutiesFromStorageEachPeriods() {
		return this.reloadDutiesFromStorageEachPeriods;
	}
	public void setReloadDutiesFromStorageEachPeriods(int reloadDutiesFromStorageEachPeriods) {
		this.reloadDutiesFromStorageEachPeriods = reloadDutiesFromStorageEachPeriods;
	}	
	public int getPlanMaxRetries() {
		return this.planMaxRetries;
	}
	public long getStartDelayBeats() {
		return startDelayBeats;
	}
	public void setStartDelayBeats(long startDelayBeats) {
		this.startDelayBeats = startDelayBeats;
	}
	public long getDelayBeats() {
		return delayBeats;
	}
	public void setDelayBeats(long delayBeats) {
		this.delayBeats = delayBeats;
	}
	public int getPlanExpirationBeats() {
		return planExpirationBeats;
	}
	public void setPlanExpirationBeats(int planExpirationBeats) {
		this.planExpirationBeats = planExpirationBeats;
	}
	public void setPlanMaxRetries(int planMaxRetries) {
		this.planMaxRetries = planMaxRetries;
	}
	
}