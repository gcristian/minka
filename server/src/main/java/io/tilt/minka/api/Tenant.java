package io.tilt.minka.api;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang.StringUtils;
import org.glassfish.grizzly.http.server.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import io.tilt.minka.shard.TCPShardIdentifier;

/**
 * A way to permit several Server instances running within the same VM
 */
class Tenant {

	protected static final Logger logger = LoggerFactory.getLogger(Server.class);

	/* to enable many minka shards on the same JVM */
	private static final Map<String, Tenant> tenants = new ConcurrentHashMap<>();
	private static Lock lock = new ReentrantLock();

	private HttpServer webServer;
	private String connectReference;
	private ClassPathXmlApplicationContext context;
	private Config config;
	
	public HttpServer getWebServer() {
		return this.webServer;
	}
	public void setWebServer(HttpServer webServer) {
		this.webServer = webServer;
	}
	public void setConnectReference(String connectReference) {
		this.connectReference = connectReference;
	}
	public String getConnectReference() {
		String ret = StringUtils.EMPTY;
		if (StringUtils.isEmpty(connectReference)) {
			if (getContext()!=null 
					&& getContext().isActive()) {
				ret = connectReference = getContext().getBean(TCPShardIdentifier.class).getConnectString();
			}
		} else {
			ret = connectReference;
		}
		return ret;
	}
	public ClassPathXmlApplicationContext getContext() {
		return this.context;
	}
	public void setContext(ClassPathXmlApplicationContext context) {
		this.context = context;
	}
	public Config getConfig() {
		return this.config;
	}
	public void setConfig(Config config) {
		this.config = config;
	}
	
	public Tenant(final Config config) {
		try {
			lock.tryLock(500l, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			throw new IllegalThreadStateException("Other Server instances are being created concurrently (lock wait exhausted)");
		}
		final String namespace = config.getBootstrap().getNamespace();
		RuntimeException runtime = null;
		try {
			// namespace cannot be default or preexistent if ZK's chroot is not set
			final boolean chrootUsed = config.getBootstrap().getZookeeperHostPort().indexOf('/') > 0;
			final boolean duplicateName = tenants.containsKey(namespace);
			final boolean vmLimitAware = config.getBootstrap().isDropVMLimit();
			if (!chrootUsed && duplicateName && !vmLimitAware) {
				runtime = exceptionSameName(namespace, null);
			} else {
				if (!vmLimitAware) {
					if (!duplicateName) {
						final long maxTenants = config.getBootstrap().getMaxServicesPerMachine();
						if (tenants.size()>maxTenants) {
							runtime = exceptionMaxTenants(null);
						}
					} else {
						runtime = exceptionMaxTenantsSameVM(null);
					}
				}
			}
			for (Tenant t: tenants.values()) {
				Server.logger.warn("Other tenant within the VM on the namespace: {} on broker hostport: {}", 
						t.getConfig().getBootstrap().getNamespace(), t.getConfig().getBroker().getHostPort());
			}
			if (duplicateName && !vmLimitAware) {
				// client should not depend on it anyway
				final String newName = namespace + "_" + new Random(System.currentTimeMillis()).nextInt(999999);
				Server.logger.warn("{}: Overwritting service name: {} to avoid colission with other servers within the same JVM", 
						Tenant.class.getSimpleName(), newName);
				config.getBootstrap().setNamespace(newName);
			}
			if (runtime == null) {
				this.config = config;
				tenants.put(config.getBootstrap().getNamespace(), this);
			}
		} finally {
			lock.unlock();
		}
		if (runtime != null) {
			throw runtime;
		}
	}
	
	protected synchronized void destroy(final boolean wait) {
		if (getContext()!=null && getContext().isActive()) {
			try {
				getContext().close();	
			} catch (Exception e) {
				logger.error("{}: {} Unexpected while destroying context at client call", 
						getClass().getSimpleName(), getConnectReference(), e.getMessage());
			}
			if (getConfig().getBootstrap().isEnableWebserver() && getWebServer()!=null) {
				try {
					getWebServer().shutdown();
				} catch (Exception e) {
					logger.error("{}: {} Unexpected while stopping server at client call", 
							getClass().getSimpleName(), getConnectReference(), e.getMessage());
				}
			}
			tenants.remove(getConfig().getBootstrap().getNamespace());
			if (wait && !holdUntilDisconnect()) {
				logger.error("{}: {} Couldnt wait for finalization of resources (may still remain open)", 
						getClass().getSimpleName(), getConnectReference());
			}
		}
	}
	
	/** 
	 * sleep and block current thread 3 times with 1s delay until broker's host-port is available again
	 * in order to properly enable further tenant systems to initiate with a clean environment  
	 */
	private boolean holdUntilDisconnect() {
		final Config c = getConfig();
		final String[] parts = c.getBroker().getHostPort().split(":");
		for(int retry = 0; retry < 3; retry++) {
			try (ServerSocket tmp = new ServerSocket(Integer.parseInt(parts[1]))) {
				return true;
			} catch (IOException ioe) {
				try {
					Thread.sleep(c.beatToMs(c.getBootstrap().getResourceReleaseWait()));
				} catch (InterruptedException e) {
				}
			}
		}
		return false;
	}

	
	private static IllegalArgumentException exceptionMaxTenantsSameVM(final Tenant t) {
		return new IllegalArgumentException(new StringBuilder()
				.append(t.getConnectReference())
				.append(": There're ")
				.append(tenants.size())
				.append(" server/s already")
				.append(" in this VM with the same service-name: set a different one")
				.toString());
	}
	private static IllegalStateException exceptionMaxTenants(final Tenant t) {
		return new IllegalStateException(new StringBuilder()
				.append(t.getConnectReference())
				.append(": There's been created ")
				.append(tenants.size()) 
				.append(" server/s already in this VM. If you indeed want that many: ")
				.append(" increase bootstrap's MAX_SERVICES_PER_MACHINE default value")
				.toString());
	}
	private static IllegalArgumentException exceptionSameName(final String namespace, final Tenant t) {
		return new IllegalArgumentException(new StringBuilder()
					.append(t.getConnectReference())
					.append(" a service on the namespace: ")
					.append(namespace)
					.append(" already exists!")
					.toString());
	}


}