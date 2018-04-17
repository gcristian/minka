package io.tilt.minka.api.config;

public class BootstrapConfiguration {
	protected static final String SERVICE_NAME = ("default-name");
	private static String serviceName;
	
	// this sets the pace of all time-synchronized processes
	protected static final long BEAT_UNIT_MS = 500;
	private long beatUnitMs;		
	protected static final long READYNESS_RETRY_DELAY_BEATS = 5;
	private long readynessRetryDelayBeats;
	
	//protected static final long READYNESS_RETRY_DELAY_MS = 5000l;
	//private long readynessRetryDelayMs;
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

	protected static final boolean ENABLE_LOGGING = true;
	private boolean enableLogging;
	
	public String getServiceName() {
		return serviceName;
	}
	public void setServiceName(String serviceName) {
		BootstrapConfiguration.serviceName = serviceName;
	}
	public long getBeatUnitMs() {
		return beatUnitMs;
	}
	public long getReadynessRetryDelayBeats() {
		return readynessRetryDelayBeats;
	}
	public void setBeatUnitMs(long beatUnitMs) {
		this.beatUnitMs = beatUnitMs;
	}
	public void setReadynessRetryDelayBeats(long readynessRetryDelayBeats) {
		this.readynessRetryDelayBeats = readynessRetryDelayBeats;
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

	public boolean isEnableLogging() {
		return enableLogging;
	}

	public void setEnableLogging(boolean enableLogging) {
		this.enableLogging = enableLogging;
	}
}