package io.tilt.minka.api;

import static io.tilt.minka.api.config.SchedulerSettings.THREAD_NAME_WEBSERVER_KERNEL;
import static io.tilt.minka.api.config.SchedulerSettings.THREAD_NAME_WEBSERVER_WORKER;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;

import org.apache.commons.lang.Validate;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.NetworkListener;
import org.glassfish.grizzly.nio.transport.TCPNIOTransport;
import org.glassfish.grizzly.threadpool.ThreadPoolConfig;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.uri.internal.JerseyUriBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import io.tilt.minka.api.config.BootstrapConfiguration;
import io.tilt.minka.api.config.BrokerConfiguration;
import io.tilt.minka.domain.AwaitingDelegate;
import io.tilt.minka.domain.ConsumerDelegate;
import io.tilt.minka.domain.DependencyPlaceholder;
import io.tilt.minka.model.PartitionDelegate;
import io.tilt.minka.model.PartitionMaster;
import io.tilt.minka.utils.LogUtils;

/**
 * System initiator and holder.<br>
 * Use an {@linkplain EventMapper} to map required and optional sharding events, or use a custom implementation
 * of {@linkplain PartitionMaster} and {@linkplain PartitionDelegate} <br>
 * <br>
 * Each server instance will spawn the underlying context which connects to Zookkeeper and starts network broker services.<br>   
 * Although many instances can coexist within the same JVM under different namespaces, is not recommended.
 * You should only create one instance per application willing to distribute duties .<br>
 * <br>
 * By default all Minka services excluding broker service, run several tasks within a one-thread-only ThreadPoolExecutor.<br>
 * After an instance is created: the context gets initialized and waits for {@linkplain EventMapper::done}. <br>  
 * 
 * @author Cristian Gonzalez
 * @since Sept 20, 2016
 */
public class Server {

	private static final String CONTEXT_PATH = "classpath:io/tilt/minka/config/context-minka-spring.xml";
	
	protected static final Logger logger = LoggerFactory.getLogger(Server.class);
	private final String name = getClass().getSimpleName();
	
	
	/* current holder's tenant, and one for each instance held by applications */
	private Tenant tenant;
	private EventMapper mapper;
	
	/** 
	 * Create a Minka server. All mandatory events must be mapped to consumers/suppliers.
	 * @param jsonFormatConfig with a configuration in a JSON file, 
	 * whose format must comply {@linkplain Config} class serialization 
	 * @throws Exception when given file is invalid
	 * */
	public Server(final File jsonFormatConfig) throws Exception {
		//Validate.notNull(jsonFormatConfig);
		this(Config.fromJsonFile(jsonFormatConfig));
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
	 * @param namespace			all cluster members must reach themselves within it   
	 */
	public Server(final String zookeeperConnectionString, final String minkaHostPort, final String namespace)  {
		Validate.notNull(zookeeperConnectionString);
		Validate.notNull(minkaHostPort);
		Validate.notNull(namespace);
		final Config config = new Config(zookeeperConnectionString, minkaHostPort);
		config.getBootstrap().setNamespace(namespace);
		init(config);
	}
	/**
	 * Create a Minka server with default configuration. 
	 * All mandatory events must be mapped to consumers/suppliers.
	 * Shard will attempt to take a port over 5748, trying increased ports if busy.
	 * Rest API will take port 57480
	 * 
	 * @param zookeeperConnectionString in the zookeeper form hostname:port/chroot
	 * @param namespace					all cluster members must reach themselves within it
	 */
	public Server(final String zookeeperConnectionString, final String namespace)  {
		Validate.notNull(zookeeperConnectionString);
		Validate.notNull(namespace);
		Config config = new Config(zookeeperConnectionString);
		config.getBootstrap().setNamespace(namespace);
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
		if (tenant!=null) {
			return tenant.getConfig();
		} else {
			throw new IllegalStateException("server already shutdown");
		}
	}
	
	/**
	 * Used when avoiding a client implementation of {@linkplain PartitionMaster}. 
	 * @return the event mapper instance associated with this server
	 */
	public EventMapper getEventMapper() {
		if (tenant!=null) {
			return this.mapper;
		} else {
			throw new IllegalStateException("server already shutdown");
		}
	}
	
	private void init(final Config config) {
		config.getBootstrap().validate();
		config.getBroker().validate();
		
		tenant = new Tenant(config);
		logger.info("{}: Initializing context for namespace: {}", name, config.getBootstrap().getNamespace());
		Runtime.getRuntime().addShutdownHook(new Thread(() -> destroy(false)));
		final ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext(new String[] { CONTEXT_PATH }, false);
		tenant.setContext(ctx);
		ctx.addBeanFactoryPostProcessor(beanFactory-> beanFactory.registerSingleton("config", config));
		final String namespace = config.getBootstrap().getNamespace();
		ctx.setDisplayName(new StringBuilder("minka-")
				.append(namespace)
				.append("-ts:")
				.append(System.currentTimeMillis())
				.toString());
		//logger.info("{}: Using configuration: {}", name, config.toString());
		ctx.setId(namespace);
		mapper = new EventMapper(tenant);
		startContext(tenant);
	}
	
	   
	private void startContext(final Tenant t) {
		t.getContext().refresh();
		if (t.getConfig().getBootstrap().isEnableWebserver()) {
			startWebserver(t);
			logger.info(LogUtils.getGreetings(
					t.getConfig().getBootstrap().getZookeeperHostPort(),
					t.getConfig().getResolvedShardId(), 
					t.getConfig().getBootstrap().getNamespace(), 
					t.getConfig().getBootstrap().getWebServerHostPort()));

		} else {
			logger.info("{}: {} Webserver disabled by configuration. Enable for a fully functional shard", name,
					t.getConnectReference());
		}
	}

	private void startWebserver(final Tenant t) {
		final ResourceConfig res = new ResourceConfig(
				AdminEndpoint.class,
				CRUDEndpoint.class);
		
		res.property("contextConfig", t.getContext());
		final URI uri = resolveWebServerBindAddress(t.getConfig());
		final HttpServer webServer = GrizzlyHttpServerFactory.createHttpServer(uri, res, false);
		t.setWebServer(webServer);
		final ThreadPoolConfig config = ThreadPoolConfig.defaultConfig()
				.setCorePoolSize(10)
				.setMaxPoolSize(50);
		
		final Iterator<NetworkListener> it = webServer.getListeners().iterator();
		while (it.hasNext()) {
			final NetworkListener listener = it.next();
			logger.info("{}: {} Configuring webserver listener {}", name, t.getConnectReference(), listener);
			final TCPNIOTransport transport = listener.getTransport();
			transport.setSelectorRunnersCount(10);
			transport.setWorkerThreadPoolConfig(config.copy().setPoolName(THREAD_NAME_WEBSERVER_WORKER));
			transport.setWorkerThreadPoolConfig(config.copy().setPoolName(THREAD_NAME_WEBSERVER_KERNEL));
		}
		
		try {
			webServer.start();
		} catch (IOException e) {
			logger.info("{}: {} Unable to start web server", name, t.getConnectReference(), e);
		}
    }

	/* by default bind to the same broker's host interfase and if changed use broker's port plus 100 */
	private URI resolveWebServerBindAddress(final Config config) {
		final String[] brokerHostPort = config.getBroker().getHostPort().split(":");
    	final JerseyUriBuilder builder = new JerseyUriBuilder();
    	final BootstrapConfiguration bs = config.getBootstrap();
		final String[] webHostPort = bs.getWebServerHostPort().split(":");
		int webPort = Integer.parseInt(webHostPort[1]);
		final boolean untouched = bs.getWebServerHostPort().equals(BootstrapConfiguration.WEB_SERVER_HOST_PORT);
		String webhostport;
		if (untouched) {
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
		logger.info("{}: {} Web host:port = {}", name, tenant.getConnectReference(), webhostport);
		builder.path(config.getBootstrap().getWebServerContextPath());
		return builder.build();
	}
    
	private void checkInit() {
		if (tenant!=null && !tenant.getContext().isActive()) {
			throw new IllegalStateException(tenant.getConnectReference() + " Minka service must be started first !");
		}
	}
	
	protected synchronized void destroy(final boolean wait) {
		if (tenant != null) {
			tenant.destroy(wait);
			tenant = null;
		}
	}
	
	private DependencyPlaceholder getDepPlaceholder() {
		checkInit();
		return tenant.getContext().getBean(DependencyPlaceholder.class);
	}


	/**
	 * An alternative way of mapping duty and pallet events, thru an implementation class.
	 * @param delegate	a fully implementation class of a partition delegate
	 * @return	the server builder
	 */
	public void setDelegate(final ConsumerDelegate delegate) {
		Validate.notNull(delegate);
		final DependencyPlaceholder holder = getDepPlaceholder();
		Validate.isTrue(holder==null || (holder.getDelegate() instanceof AwaitingDelegate), 
				"You're overwriting previous delegate or event's consumer: " + delegate.getClass().getSimpleName());
		logger.info("{}: {} Using new PartitionDelegate: {}", name, tenant.getConnectReference(), 
				delegate.getClass().getSimpleName());
		holder.setDelegate(delegate);
	}
	/**
	 * An alternative way of mapping duty and pallet events, thru an implementation class.
	 * @param master a fully implementation class of a partition master
	 * @return	the server builder
	 */
	public void setMaster(final PartitionMaster master) {
		Validate.notNull(master);
		final DependencyPlaceholder holder = getDepPlaceholder();
		Validate.isTrue(holder==null || (holder.getMaster() instanceof AwaitingDelegate), 
				"You're overwriting previous delegate or event's consumer: " + master.getClass().getSimpleName());
		logger.info("{}: {} Using new PartitionMaster: {}", name, tenant.getConnectReference(),
				master.getClass().getSimpleName());
		getDepPlaceholder().setMaster(master);
	}

	
	/**
	 * Minka service must be fully initialized before being able to obtain an operative client
	 * @return	an instance of a client   
	 */
	public Client getClient() {
		checkInit();
		final Client cli = tenant.getContext().getBean(Client.class);
		if (cli.getEventMapper()==null) {
			cli.setEventMapper(getEventMapper());
		}
		return cli;
	}

	/**
	 * <p>
	 * Warning: This executes automatically at VM shutdown (hook), but must be called independently 
	 * when in need to release unnecesary resource consumption. 
	 * <p>
	 * Calls the termination of the system in an orderly manner.
	 * Closing the API webserver and system context, which in turn will trigger finalization
	 * of all spawned processes: dropping leadership candidature at Zookeeper, 
	 * and follower's captured entities. (properly calling the passed lambda at EventMapper)
	 */
	public void shutdown(final boolean wait) {
		if (tenant!=null) {
			logger.info("{}: {} Shutting down at request", name, tenant.getConnectReference());
			destroy(true);
		}
	}
	public void shutdown() {
		shutdown(true);
	}
}
