package io.tilt.minka.api.config;

public class BrokerConfiguration {
	public final static int PORT = 5748;
	protected final static String HOST_PORT = "localhost:" + PORT;
	private String hostPort;
	// tested with a cluster of 10 nodes: 1 thread was enough
	// either case Heartbeats from followers will compete for leader's atention at most
	// and broker's messages range 8-30k bytes: which means a fast netty channel switch and no starvation   
	protected final static int CONNECTION_HANDLER_THREADS = 1;
	private int connectionHandlerThreads;
	protected final static int MAX_RETRIES = 3;
	private int maxRetries;
	protected final static int RETRY_DELAY_MILI_BEATS = 300;
	private long retryDelayMiliBeats;
	protected final static int MAX_LAG_BEFORE_DISCARDING_CLIENT_QUEUE = 10;
	private long maxLagBeforeDiscardingClientQueue;
	protected final static int MAX_CLIENT_QUEUE_SIZE = 50;
	private int maxClientQueueSize;
	//protected final static int RETRY_DELAY_MS = 300;
	//private int retryDelayMs;
	/** True: try number-consecutive open ports if specified is busy, False: break bootup */
	protected static final boolean ENABLE_PORT_FALLBACK = true;
	public boolean enablePortFallback;
	protected static final boolean USE_MACHINE_HOSTNAME = false;
	public boolean useMachineHostname;
	protected static final String SHARD_ID_SUFFIX = "";
	private String shardIdSuffix;
	protected static final String NETWORK_INTERFASE = "lo";
	private String networkInterfase;
	
	protected final static long SHUTDOWN_QUIET = 5;
	private long shutdownQuiet;
	protected final static long SHUTDOWN_TIMEOUT = SHUTDOWN_QUIET * 3;
	private long shutdownTimeout;
	
	public String getHostPort() {
		return this.hostPort;
	}
	public void setHostPort(String hostPort) {
		this.hostPort = hostPort;
	}
	public int getConnectionHandlerThreads() {
		return this.connectionHandlerThreads;
	}
	public void setConnectionHandlerThreads(int connectionHandlerThreads) {
		this.connectionHandlerThreads = connectionHandlerThreads;
	}
	public int getMaxRetries() {
		return this.maxRetries;
	}
	public void setMaxRetries(int maxRetries) {
		this.maxRetries = maxRetries;
	}
	public long  getRetryDelayMiliBeats() {
		return retryDelayMiliBeats;
	}
	public void setRetryDelayMiliBeats(int retryDelayMiliBeats) {
		this.retryDelayMiliBeats = retryDelayMiliBeats;
	}
	public long getMaxLagBeforeDiscardingClientQueue() {
		return maxLagBeforeDiscardingClientQueue;
	}
	public void setMaxLagBeforeDiscardingClientQueue(long maxLagBeforeDiscardingClientQueue) {
		this.maxLagBeforeDiscardingClientQueue = maxLagBeforeDiscardingClientQueue;
	}
	public boolean isEnablePortFallback() {
		return this.enablePortFallback;
	}
	public void setEnablePortFallback(boolean enablePortFallback) {
		this.enablePortFallback = enablePortFallback;
	}
	public boolean isUseMachineHostname() {
		return this.useMachineHostname;
	}

	public void setUseMachineHostname(boolean useMachineHostname) {
		this.useMachineHostname = useMachineHostname;
	}
	public String getShardIdSuffix() {
		return this.shardIdSuffix;
	}
	public void setShardIdSuffix(String shardIdSuffix) {
		this.shardIdSuffix = shardIdSuffix;
	}
	public String getNetworkInterfase() {
		return this.networkInterfase;
	}
	public void setNetworkInterfase(String networkInterfase) {
		this.networkInterfase = networkInterfase;
	}
	public int getMaxClientQueueSize() {
		return maxClientQueueSize;
	}
	public void setMaxClientQueueSize(final int maxClientQueueSize) {
		this.maxClientQueueSize = maxClientQueueSize;
	}
	public long getShutdownQuiet() {
		return shutdownQuiet;
	}
	public void setShutdownQuiet(long shutdownQuiet) {
		this.shutdownQuiet = shutdownQuiet;
	}
	public long getShutdownTimeout() {
		return shutdownTimeout;
	}
	public void setShutdownTimeout(long shutdownTimeout) {
		this.shutdownTimeout = shutdownTimeout;
	}
}