package io.tilt.minka.api;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang.Validate;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.NetworkListener;
import org.glassfish.grizzly.threadpool.GrizzlyExecutorService;
import org.glassfish.grizzly.threadpool.ThreadPoolConfig;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.uri.internal.JerseyUriBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import io.tilt.minka.api.config.BootstrapConfiguration;
import io.tilt.minka.api.config.BrokerConfiguration;
import io.tilt.minka.api.config.SchedulerSettings;
import io.tilt.minka.api.inspect.AdminEndpoint;
import io.tilt.minka.domain.AwaitingDelegate;
import io.tilt.minka.domain.DependencyPlaceholder;
import io.tilt.minka.domain.PartitionDelegate;
import io.tilt.minka.domain.PartitionMaster;
import io.tilt.minka.utils.LogUtils;

/**
 * System initiator and holder.<br>
 * Use an {@linkplain EventMapper} to map required and optional sharding events, or use a custom implementation
 * of {@linkplain PartitionMaster} and {@linkplain PartitionDelegate} <br>
 * <br>
 * Each server instance will spawn the underlying context which connects to Zookkeeper and starts network broker services.<br>   
 * Although many instances can coexist within the same JVM under different service names, is not recommended.
 * You should only create one instance per application willing to distribute duties .<br>
 * <br>
 * By default all Minka services excluding broker service, run several tasks within a one-thread-only ThreadPoolExecutor.<br>
 * After an instance is created: the context gets initialized and waits for {@linkplain Server.load()}. <br>  
 * 
 * @author Cristian Gonzalez
 * @since Sept 20, 2016
 * @param <D>	the duty payload type
 * @param <P>	the pallet payload type
 */
public class Server<D extends Serializable, P extends Serializable> {

	private static final String CONTEXT_PATH = "classpath:io/tilt/minka/config/context-minka-spring.xml";
	
	protected static final Logger logger = LoggerFactory.getLogger(Server.class);
	private final String name = getClass().getSimpleName();
	
	/* to enable many minka shards on the same JVM */
	private static final Map<String, Tenant> tenants = new ConcurrentHashMap<>();
	private static Lock lock = new ReentrantLock();
	
	/* current holder's tenant, and one for each instance held by applications */
	private Tenant tenant;
	private EventMapper<D, P> mapper;
	
	/** 
	 * Create a Minka server. All mandatory events must be mapped to consumers/suppliers.
	 * @param jsonFormatConfig with a configuration in a JSON file, 
	 * whose format must comply {@linkplain Config} class serialization 
	 * @throws Exception when given file is invalid
	 * */
	public Server(final File jsonFormatConfig) throws Exception {
		Validate.notNull(jsonFormatConfig);
		Config config = Config.fromJsonFile(jsonFormatConfig);
		init(config);
	}
	/**
	 * Create a Minka server. All mandatory events must be mapped to consumers/suppliers. 
	 * @param config Create a Minka server with a specific configuration */
	public Server(final Config config) {
		Validate.notNull(config);
		init(config);
	}
	/** 
	 * Create a Minka server. All mandatory events must be mapped to consumers/suppliers.
	 * one liner to custom main TCP hostname/ports only
	 * @param zookeeperConnectionString		in the form hostname:port/chroot 
	 * @param minkaHostPort		in the form hostname:port
	 */
	public Server(final String zookeeperConnectionString, final String minkaHostPort, final String serviceName)  {
		Validate.notNull(zookeeperConnectionString);
		Validate.notNull(minkaHostPort);
		Validate.notNull(serviceName);
		final Config config = new Config(zookeeperConnectionString, minkaHostPort);
		config.getBootstrap().setServiceName(serviceName);
		init(config);
	}
	/**
	 * Create a Minka server with default configuration. 
	 * All mandatory events must be mapped to consumers/suppliers.
	 * Shard will attempt to take a port over 5748, trying increased ports if busy.
	 * Rest API will take port 57480
	 * 
	 * @param zookeeperConnectionString in the zookeeper form hostname:port/chroot
	 * @param zookeeperConnectionString in the zookeeper form hostname:port/chroot
	 */
	public Server(final String zookeeperConnectionString, final String serviceName)  {
		Validate.notNull(zookeeperConnectionString);
		Validate.notNull(serviceName);
		Config config = new Config(zookeeperConnectionString);
		config.getBootstrap().setServiceName(serviceName);
		init(config);
	}
	/**
	 * Create a Minka server with default configuration. 
	 * All mandatory events must be mapped to consumers/suppliers.
	 * Shard will attempt to take a port over 5748, trying increased ports if busy.
	 */
	public Server()  {
		init(new Config());
	}

	public Config getConfig() {
		return tenant.getConfig();
	}
	
	/**
	 * Used when avoiding a client implementation of {@linkplain PartitionMaster}. 
	 * @return the event mapper instance associated with this server
	 */
	public EventMapper<D, P> getEventMapper() {
		return this.mapper;
	}
	
	private void init(final Config config) {
		createTenant(config);
		tenant.setConfig(config);
		final ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext(new String[] { CONTEXT_PATH }, false);
		tenant.setContext(ctx);
		ctx.addBeanFactoryPostProcessor(beanFactory-> beanFactory.registerSingleton("config", config));
		final String serviceName = config.getBootstrap().getServiceName();
		ctx.setDisplayName(new StringBuilder("minka-")
				.append(serviceName)
				.append("-ts:")
				.append(System.currentTimeMillis())
				.toString());
		logger.info("{}: Using configuration: {}", name, config.toString());
		ctx.setId(serviceName);
		logger.info("{}: Naming context: {}", name, ctx.getId());
		Runtime.getRuntime().addShutdownHook(new Thread(() -> destroy()));
		mapper = new EventMapper<D, P>(tenant);
		startContext(config);
	}
	
	private void createTenant(final Config config) {
		try {
			lock.tryLock(500l, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			throw new IllegalThreadStateException("Other Server instances are being created concurrently (lock wait exhausted)");
		}
		final String serviceName = config.getBootstrap().getServiceName();
		RuntimeException runtime = null;
		try {
			// service cannot be default or preexistent if ZK's chroot is not set
			final boolean chrootUsed = config.getBootstrap().getZookeeperHostPort().indexOf('/') > 0;
			final boolean duplicateName = tenants.containsKey(serviceName);
			final boolean vmLimitAware = config.getBootstrap().isDropVMLimit();
			if (!chrootUsed && duplicateName && !vmLimitAware) {
				runtime = new IllegalArgumentException("a service with the name: " + serviceName + " already exists!");
			} else {
				if (!vmLimitAware) {
					if (!duplicateName) {
						final long maxTenants = config.getBootstrap().getMaxServicesPerMachine();
						if (tenants.size()>maxTenants) {
							runtime = new IllegalStateException("There's been created " + tenants.size() 
								+ " server/s already in this VM. If you indeed want that many: "
								+ " increase bootstrap's MAX_SERVICES_PER_MACHINE default value");
						}
					} else {
						runtime = new IllegalArgumentException("There're " + tenants.size() + " server/s already" 
								+ " in this VM with the same service-name: set a different one");
					}
				}
			}
			for (Tenant t: tenants.values()) {
				logger.warn("Tenant: service name: {} on broker hostport: {}", 
						t.getConfig().getBootstrap().getServiceName(), t.getConfig().getBroker().getHostPort());
			}
			logger.info("{}: Initializing context for service: {}", name, serviceName);
			if (duplicateName && !vmLimitAware) {
				// client should not depend on it anyway
				final String newName = serviceName + "_" + new Random(System.currentTimeMillis()).nextInt(999999);
				logger.warn("{}: Overwritting service name: {} to avoid colission with other servers within the same JVM", 
						name, newName);
				config.getBootstrap().setServiceName(newName);
			}
			tenants.put(config.getBootstrap().getServiceName(), tenant = new Tenant());
		} finally {
			lock.unlock();
		}
		if (runtime != null) {
			throw runtime;
		}
	}
	   
	private void startContext(final Config config) {
		if (!tenant.getContext().isActive()) {
			tenant.getContext().refresh();
			if (tenant.getConfig().getBootstrap().isEnableWebserver()) {
				startWebserver();
				logger.info(LogUtils.getGreetings(
						config.getResolvedShardId(), 
						config.getBootstrap().getServiceName(), 
						config.getBootstrap().getWebServerHostPort()));

			} else {
				logger.info("{}: Webserver disabled by configuration. Enable for a fully functional shard", name);
			}
		} else {
			logger.error("{}: Can only load Minka once !", name);
		}
	}

	private void startWebserver() {
		final ResourceConfig res = new ResourceConfig(AdminEndpoint.class);
		
		res.property("contextConfig", tenant.getContext());
		final HttpServer webServer = GrizzlyHttpServerFactory.createHttpServer(
		        resolveWebServerBindAddress(tenant.getConfig()), res);
		
		final ThreadPoolConfig config = ThreadPoolConfig.defaultConfig()
				.setCorePoolSize(1)
				.setMaxPoolSize(1);
		
		
		final Iterator<NetworkListener> it = webServer.getListeners().iterator();
		while (it.hasNext()) {
			final NetworkListener listener = it.next();
			logger.info("{}: Reconfiguring webserver listener {}", name, listener);
			listener.getTransport().setSelectorRunnersCount(1);
			((GrizzlyExecutorService)listener.getTransport().getWorkerThreadPool()).reconfigure(
					config.copy().setPoolName(SchedulerSettings.THREAD_NAME_WEBSERVER_WORKER));
			((GrizzlyExecutorService)listener.getTransport().getKernelThreadPool()).reconfigure(
					config.copy().setPoolName(SchedulerSettings.THREAD_NAME_WEBSERVER_KERNEL));
		}
		
        // TODO disable ssl etc
		tenants.get(tenant.getConfig().getBootstrap().getServiceName()).setWebServer(webServer);
		try {
			webServer.start();
		} catch (IOException e) {
			logger.info("{}: Unable to start web server", name, e);
		}
    }

	/* by default bind to the same broker's host interfase and if changed use broker's port plus 100 */
	private URI resolveWebServerBindAddress(final Config config) {
		final String[] brokerHostPort = config.getBroker().getHostPort().split(":");
    	final JerseyUriBuilder builder = new JerseyUriBuilder();
    	final BootstrapConfiguration bs = config.getBootstrap();
		final String[] webHostPort = bs.getWebServerHostPort().split(":");
		int webPort = Integer.parseInt(webHostPort[1]);
		final boolean webHostPortUntouched = bs.getWebServerHostPort().equals(BootstrapConfiguration.WEB_SERVER_HOST_PORT);
		String webhostport;
		if (webHostPortUntouched) {
			int brokerPort = Integer.parseInt(brokerHostPort[1]);
			webPort = brokerPort == BrokerConfiguration.PORT ? webPort: brokerPort + 100;
			final String host = config.getResolvedShardId().getId().split(":")[0];
			builder.host(host).port(webPort);
			webhostport = host + ":" + webPort;
		} else {
			builder.host(webHostPort[0]).port(webPort);
			webhostport = webHostPort[0]+ ":" + webPort;
		}
		config.getResolvedShardId().setWebHostPort(webhostport);
		config.getBootstrap().setWebServerHostPort(webhostport);
		logger.info("{}: Web host:port = {}", name, webhostport);
		builder.path(config.getBootstrap().getWebServerContextPath());
		return builder.build();
	}
    
	private void checkInit() {
		if (!tenant.getContext().isActive()) {
			throw new IllegalStateException("Minka service must be started first !");
		}
	}
	
	protected synchronized void destroy() {
		if (tenant != null && tenant.getContext().isActive()) {
			tenant.getContext().close();
			if (tenant.getConfig().getBootstrap().isEnableWebserver() && tenant.getWebServer()!=null) {
				tenant.getWebServer().shutdown();
			}
			tenant = null;
		} else {
			logger.error("{}: Can only destroy service's context once !", name);
		}
	}
	
	private DependencyPlaceholder getDepPlaceholder() {
		return tenant.getContext().getBean(DependencyPlaceholder.class);
	}

	
	/**
	 * An alternative way of mapping duty and pallet events, thru an implementation class.
	 * @param delegate	a fully implementation class of a partition delegate
	 * @return	the server builder
	 */
	public void setDelegate(final PartitionDelegate<?, ?> delegate) {
		Validate.notNull(delegate);
		checkInit();		
		final DependencyPlaceholder holder = getDepPlaceholder();
		Validate.isTrue(holder==null || (holder.getDelegate() instanceof AwaitingDelegate), 
				"You're overwriting previous delegate or event's consumer: " + delegate.getClass().getSimpleName());
		logger.info("{}: Using new PartitionDelegate: {}", name, delegate.getClass().getSimpleName());
		holder.setDelegate(delegate);
	}
	/**
	 * An alternative way of mapping duty and pallet events, thru an implementation class.
	 * @param master a fully implementation class of a partition master
	 * @return	the server builder
	 */
	public void setMaster(final PartitionMaster<?, ?> master) {
		Validate.notNull(master);
		checkInit();		
		final DependencyPlaceholder holder = getDepPlaceholder();
		Validate.isTrue(holder==null || (holder.getMaster() instanceof AwaitingDelegate), 
				"You're overwriting previous delegate or event's consumer: " + master.getClass().getSimpleName());
		logger.info("{}: Using new PartitionMaster: {}", name, master.getClass().getSimpleName());
		getDepPlaceholder().setMaster(master);
	}

	
	/**
	 * Minka service must be fully initialized before being able to obtain an operative client
	 * @return	an instance of a client   
	 */
	public Client<D, P> getClient() {
		checkInit();
		return tenant.getContext().getBean(Client.class);
	}

	/**
	 * <p>
	 * Calls the termination of the system in an orderly manner.
	 * Closing the API and system context, which in turn will trigger finalization
	 * of processes Leader and Follower, dropping leadership candidature at Zookeeper, 
	 * <p>
	 * This will also drop follower's captured entities, calling the client's consumer
	 * of events passed at the EventMapper. 
	 */
	public void shutdown() {
		try {
			tenant.getWebServer().shutdown();
			tenant.getContext().close();
		} catch (Exception e) {
			logger.error("{}: Unexpected while stopping server at client call", name, e.getMessage());
		}
	}
	

	/**
	 * A way to permit several Server instances running within the same VM
	 */
	protected static class Tenant {
		private HttpServer webServer;
		private ClassPathXmlApplicationContext context;
		private Config config;
		private Tenant() {}
		public HttpServer getWebServer() {
			return this.webServer;
		}
		public void setWebServer(HttpServer webServer) {
			this.webServer = webServer;
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
	}
}
