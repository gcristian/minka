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
	protected static final long READYNESS_RETRY_DELAY_BEATS = 5;
	private long readynessRetryDelayBeats;
	protected static final long LEADER_UNCONFIDENT_START_DELAY_BEATS = 20;
	private long leaderUnconfidentStartDelayBeats;
	protected static final long LEADER_UNCONFIDENT_DELAY_BEATS = 10;
	private long leaderUnconfidentDelayBeats;
	protected static final long RESOURCE_RELEASE_WAIT_BEATS = 10;
	private long resourceReleaseWaitBeats;
	
	//protected static final long READYNESS_RETRY_DELAY_MS = 5000l;
	//private long readynessRetryDelayMs;
	protected static final int REPUBLISH_LEADER_CANDIDATE_AFTER_LOST_BEATS = 2;
	private int republishLeaderCandidateAfterLostBeats;
	
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
	protected static final int CORE_DUMP_DELAY_BEATS = 5;
	private int coreDumpDelayBeats;
	protected static final String CORE_DUMP_FILEPATH = "/tmp/";
	private String coreDumpFilepath;
	protected static final boolean CORE_DUMP_SNAPSHOT = true;
	private boolean coreDumpSnapshot;
	
	
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
	public long getReadynessRetryDelayBeats() {
		return readynessRetryDelayBeats;
	}
	public long getLeaderUnconfidentDelayBeats() {
		return leaderUnconfidentDelayBeats;
	}
	public void setLeaderUnconfidentDelayBeats(long leaderUnconfidentDelayBeats) {
		this.leaderUnconfidentDelayBeats = leaderUnconfidentDelayBeats;
	}
	public long getLeaderUnconfidentStartDelayBeats() {
		return leaderUnconfidentStartDelayBeats;
	}
	public void setLeaderUnconfidentStartDelayBeats(long leaderUnconfidentStartDelayBeats) {
		this.leaderUnconfidentStartDelayBeats = leaderUnconfidentStartDelayBeats;
	}
	public void setBeatUnitMs(long beatUnitMs) {
		this.beatUnitMs = beatUnitMs;
	}
	public void setReadynessRetryDelayBeats(long readynessRetryDelayBeats) {
		this.readynessRetryDelayBeats = readynessRetryDelayBeats;
	}
	public void setResourceReleaseWaitBeats(long resourceReleaseWaitBeats) {
		this.resourceReleaseWaitBeats = resourceReleaseWaitBeats;
	}
	public long getResourceReleaseWaitBeats() {
		return resourceReleaseWaitBeats;
	}
	public void setRepublishLeaderCandidateAfterLostBeats(int republishLeaderCandidateAfterLostBeats) {
		this.republishLeaderCandidateAfterLostBeats = republishLeaderCandidateAfterLostBeats;
	}
	public int getRepublishLeaderCandidateAfterLostBeats() {
		return republishLeaderCandidateAfterLostBeats;
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
	public void setCoreDumpDelayBeats(int coreDumpDelayBeats) {
		this.coreDumpDelayBeats = coreDumpDelayBeats;
	}
	public int getCoreDumpDelayBeats() {
		return coreDumpDelayBeats;
	}
	public void setCoreDumpFilepath(String coreDumpFilepath) {
		this.coreDumpFilepath = coreDumpFilepath;
	}
	public String getCoreDumpFilepath() {
		return coreDumpFilepath;
	}
	public boolean isCoreDumpSnapshot() {
		return coreDumpSnapshot;
	}
	public void setCoreDumpSnapshot(boolean coreDumpSnapshot) {
		this.coreDumpSnapshot = coreDumpSnapshot;
	}
}