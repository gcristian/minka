package io.tilt.minka.api.config;

public class DistributorSettings {
	public static int MAX_JOURNAL_SIZE = 10;
	
	protected static final boolean RUN_CONSISTENCY_CHECK = false;
	private boolean runConsistencyCheck;
	protected static final boolean RUN_ON_STEALTH_MODE = true;
	private boolean runOnStealthMode;
	protected static final long STEALTH_HOLD_THRESHOLD = 5;
	private long stealthHoldThreshold;
	protected static final boolean RELOAD_DUTIES_FROM_STORAGE = false;
	private boolean reloadDutiesFromStorage;
	protected static final int DUTIES_RELOAD_FROM_STORAGE_PHASE_FREQUENCY = 10;
	private int dutiesReloadFromStoragePhaseFrequency;
	/* 10 seconds to let the ShardKeeper discover all Followers before distributing */
	protected final static long START_DELAY = 5; // was in 1 in august
	private long startDelay;
	protected final static long PHASE_FREQUENCY = 2;
	private long phaseFrequency;
	protected static final int PLAN_EXPIRATION = 10;
	private int planExpiration;
	
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
	public long getStealthHoldThreshold() {
		return stealthHoldThreshold;
	}
	public void setStealthHoldThreshold(long stealthHoldThreshold) {
		this.stealthHoldThreshold = stealthHoldThreshold;
	}
	public boolean isReloadDutiesFromStorage() {
		return this.reloadDutiesFromStorage;
	}
	public void setReloadDutiesFromStorage(boolean reloadDutiesFromStorage) {
		this.reloadDutiesFromStorage = reloadDutiesFromStorage;
	}
	public int getDutiesReloadFromStoragePhaseFrequency() {
		return this.dutiesReloadFromStoragePhaseFrequency;
	}
	public void setDutiesReloadFromStoragePhaseFrequency(int dutiesReloadFromStoragePhaseFrequency) {
		this.dutiesReloadFromStoragePhaseFrequency = dutiesReloadFromStoragePhaseFrequency;
	}	
	public int getPlanMaxRetries() {
		return this.planMaxRetries;
	}
	public long getStartDelay() {
		return startDelay;
	}
	public void setStartDelay(long startDelay) {
		this.startDelay = startDelay;
	}
	public long getPhaseFrequency() {
		return phaseFrequency;
	}
	public void setPhaseFrequency(long frequency) {
		this.phaseFrequency = frequency;
	}
	public int getPlanExpiration() {
		return planExpiration;
	}
	public void setPlanExpiration(int planExpiration) {
		this.planExpiration = planExpiration;
	}
	public void setPlanMaxRetries(int planMaxRetries) {
		this.planMaxRetries = planMaxRetries;
	}
	
}
