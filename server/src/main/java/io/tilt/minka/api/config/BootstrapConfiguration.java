package io.tilt.minka.api.config;

public class BootstrapConfiguration {
    
    public static final String NAMESPACE_MASK_LEADER_LATCH = "minka/%s/leader-latch";
    public static final String NAMESPACE_MASK_LEADER_SHARD_RECORD = "minka/%s/leader-shard-record";

    public static final boolean DROP_VM_LIMIT = false;
    protected boolean dropVMLimit;
    
	protected static final String SERVICE_NAME = ("default-name");
	private String namespace;
	
	/**
	 * Optional. To be used by custom balancers as a server reference.
	 * @param tag  any user's meaningful value to the current Minka's location 
	 * @return	the event mapper builder
	 */
	public static final String SERVER_TAG = "notatag";
	private String serverTag;

	protected static final long MAX_SERVICES_PER_MACHINE = 1;
	private long maxServicesPerMachine;

	// this sets the pace of all time-synchronized processes
	protected static final long BEAT_UNIT_MS = 500;
	private long beatUnitMs;		
	protected static final long READYNESS_RETRY_FREQUENCY = 5;
	private long readynessRetryFrequency;
	protected static final long LEADER_UNCONFIDENT_START_DELAY = 20;
	private long leaderUnconfidentStartDelay;
	protected static final long LEADER_UNCONFIDENT_FREQUENCY = 10;
	private long leaderUnconfidentFrequency;
	protected static final long RESOURCE_RELEASE_WAIT = 10;
	private long resourceReleaseWait;
	
	protected static final int REPUBLISH_LEADER_CANDIDATE_AFTER_LOST = 2;
	private int republishLeaderCandidateAfterLost;
	
	protected final static boolean PUBLISH_LEADER_CANDIDATURE = true;
	private boolean publishLeaderCandidature;
	protected static final boolean LEADER_SHARD_ALSO_FOLLOWS = true;
	private boolean leaderShardAlsoFollows;
	protected static final String ZOOKEEPER_HOST_PORT = "localhost:2181";
	private String zookeeperHostPort;
	
	protected static final boolean ENABLE_WEBSERVER = true;
	private boolean enableWebserver;
	protected static final int WEB_SERVER_PORT = 57480;
	public static final String WEB_SERVER_HOST_PORT = "localhost:" + WEB_SERVER_PORT;
	private String webServerHostPort;
	
	protected static final String WEB_SERVER_CONTEXT_PATH = "minka";
	private String webServerContextPath;

	protected static final boolean ENABLE_CORE_DUMP = false;
	private boolean enableCoreDump;
	protected static final int CORE_DUMP_FREQUENCY = 5;
	private int coreDumpFrequency;
	protected static final String CORE_DUMP_FILEPATH = "/tmp/";
	private String coreDumpFilepath;
	protected static final boolean CORE_DUMP_OVERWRITE = true;
	private boolean coreDumpOverwrite;
	
	
	public String getNamespace() {
		return namespace;
	}
	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}
	public String getServerTag() {
		return serverTag;
	}
	public void setServerTag(String serverTag) {
		this.serverTag = serverTag;
	}
	public void setDropVMLimit(boolean dropVMLimit) {
		this.dropVMLimit = dropVMLimit;
	}
	public boolean isDropVMLimit() {
		return dropVMLimit;
	}
	public long getMaxServicesPerMachine() {
		return maxServicesPerMachine;
	}
	public void setMaxServicesPerMachine(long maxServicesPerMachine) {
		this.maxServicesPerMachine = maxServicesPerMachine;
	}
	public long getBeatUnitMs() {
		return beatUnitMs;
	}
	public long getReadynessRetryFrequency() {
		return readynessRetryFrequency;
	}
	public long getLeaderUnconfidentFrequency() {
		return leaderUnconfidentFrequency;
	}
	public void setLeaderUnconfidentFrequency(long leaderUnconfidentFrequency) {
		this.leaderUnconfidentFrequency = leaderUnconfidentFrequency;
	}
	public long getLeaderUnconfidentStartDelay() {
		return leaderUnconfidentStartDelay;
	}
	public void setLeaderUnconfidentStartDelay(long leaderUnconfidentStartDelay) {
		this.leaderUnconfidentStartDelay = leaderUnconfidentStartDelay;
	}
	public void setBeatUnitMs(long beatUnitMs) {
		this.beatUnitMs = beatUnitMs;
	}
	public void setReadynessRetryFrequency(long readynessRetryFrequency) {
		this.readynessRetryFrequency = readynessRetryFrequency;
	}
	public void setResourceReleaseWait(long resourceReleaseWaits) {
		this.resourceReleaseWait = resourceReleaseWaits;
	}
	public long getResourceReleaseWait() {
		return resourceReleaseWait;
	}
	public void setRepublishLeaderCandidateAfterLost(int republishLeaderCandidateAfterLost) {
		this.republishLeaderCandidateAfterLost = republishLeaderCandidateAfterLost;
	}
	public int getRepublishLeaderCandidateAfterLost() {
		return republishLeaderCandidateAfterLost;
	}
	public boolean isPublishLeaderCandidature() {
		return this.publishLeaderCandidature;
	}
	public void setPublishLeaderCandidature(boolean publishLeaderCandidature) {
		this.publishLeaderCandidature = publishLeaderCandidature;
	}
	public boolean isLeaderShardAlsoFollows() {
		return this.leaderShardAlsoFollows;
	}
	public void setLeaderShardAlsoFollows(boolean leaderShardAlsoFollows) {
		this.leaderShardAlsoFollows = leaderShardAlsoFollows;
	}
	public String getZookeeperHostPort() {
		return this.zookeeperHostPort;
	}
	public void setZookeeperHostPort(String zookeeperHostPort) {
		this.zookeeperHostPort = zookeeperHostPort;
	}
	public boolean isEnableWebserver() {
		return this.enableWebserver;
	}
	public void setEnableWebserver(boolean enableWebserver) {
		this.enableWebserver = enableWebserver;
	}
	public String getWebServerHostPort() {
		return this.webServerHostPort;
	}
	public void setWebServerHostPort(String webServerHostPort) {
		this.webServerHostPort = webServerHostPort;
	}
	public String getWebServerContextPath() {
		return webServerContextPath;
	}
	public void setWebServerContextPath(String webServerContextPath) {
		this.webServerContextPath = webServerContextPath;
	}

	public void setEnableCoreDump(boolean enableCoreDump) {
		this.enableCoreDump = enableCoreDump;
	}
	public boolean isEnableCoreDump() {
		return enableCoreDump;
	}
	public void setCoreDumpFrequency(int coreDumpFrequency) {
		this.coreDumpFrequency = coreDumpFrequency;
	}
	public int getCoreDumpFrequency() {
		return coreDumpFrequency;
	}
	public void setCoreDumpFilepath(String coreDumpFilepath) {
		this.coreDumpFilepath = coreDumpFilepath;
	}
	public String getCoreDumpFilepath() {
		return coreDumpFilepath;
	}
	public boolean isCoreDumpOverwrite() {
		return coreDumpOverwrite;
	}
	public void setCoreDumpOverwrite(boolean coreDumpOverwrite) {
		this.coreDumpOverwrite = coreDumpOverwrite;
	}
}