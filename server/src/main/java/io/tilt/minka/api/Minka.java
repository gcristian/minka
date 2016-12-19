package io.tilt.minka.api;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.commons.lang.Validate;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.uri.internal.JerseyUriBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import io.tilt.minka.api.Config.BootstrapConf;
import io.tilt.minka.api.ConsumerDelegate.Event;
import io.tilt.minka.core.leader.distributor.Balancer;

/**
 * An alternative way of programatically loading Minka  <br>
 * The service remains alive while this class stays loaded and non-stopped. <br>
 * Each application instance hosting a Minka shard must create this loader. <br> <br>
 *  
 * Minka requires some events to be mapped and others are optional depending client's needs,
 * strictly related to the usage of {@linkplain MinkaClient}. <br>
 * Once the context is created, Minka boots-up and waits for all events to be mapped,
 * unless load() is called which validates for mapped mandatory events. <br>
 * Client may or not need some pallet events, all duties hold its pallet information as well. <br>
 * Shard capacities are required if client uses a balancer that depends on duty weights. <br> <br>
 * 
 * Usually client will map events duty::load(),take(),release(),report() and pallet::load(),
 * and specify current shard's capacities for pallets thru setCapacity(...)
 * 
 * @author Cristian Gonzalez
 * @since Sept 20, 2016
 * @param <D>	the duty payload type
 * @param <P>	the pallet payload type
 */
@SuppressWarnings("unchecked")
public class Minka<D extends Serializable, P extends Serializable> {

	private static final Logger logger = LoggerFactory.getLogger(Minka.class);

	/* to enable many minka shards on the same JVM */
	private static final Map<String, MinkaTenant> tenants = new ConcurrentHashMap<>();
	
	/* current holder's tenant, and one for each instance held by applications */
	private MinkaTenant tenant;
	
	/** 
	 * Create a Minka server. All mandatory events must be mapped to consumers/suppliers.
	 * @param jsonFormatConfig with a configuration in a JSON file, 
	 * whose format must comply {@linkplain Config} class serialization 
	 * @throws Exception when given file is invalid
	 * */
	public Minka(final File jsonFormatConfig) throws Exception {
		Validate.notNull(jsonFormatConfig);
		Config config = Config.fromJsonFile(jsonFormatConfig);
		init(config);
	}
	/**
	 * Create a Minka server. All mandatory events must be mapped to consumers/suppliers. 
	 * @param config Create a Minka server with a specific configuration */
	public Minka(final Config config) {
		Validate.notNull(config);
		init(config);
	}
	/** 
	 * Create a Minka server. All mandatory events must be mapped to consumers/suppliers.
	 * one liner to custom main TCP hostname/ports only
	 * @param zookeeperConnectionString		in the form hostname:port/chroot 
	 * @param minkaHostPort		in the form hostname:port
	 */
	public Minka(final String zookeeperConnectionString, final String minkaHostPort)  {
		Validate.notNull(zookeeperConnectionString);
		Validate.notNull(minkaHostPort);
		final Config config = new Config(zookeeperConnectionString, minkaHostPort);
		init(config);
	}
	/**
	 * Create a Minka server with default configuration. 
	 * All mandatory events must be mapped to consumers/suppliers.
	 * Shard will attempt to take a port over 5748, trying increased ports if busy.
	 * Rest API will take port 57480
	 * @param zookeeperConnectionString in the zookeeper form hostname:port/chroot
	 */
	public Minka(final String zookeeperConnectionString)  {
		Validate.notNull(zookeeperConnectionString);
		Config conf = new Config(zookeeperConnectionString);
		init(conf);
	}
	/**
	 * Create a Minka server with default configuration. 
	 * All mandatory events must be mapped to consumers/suppliers.
	 * Shard will attempt to take a port over 5748, trying increased ports if busy.
	 */
	public Minka()  {
		init(new Config());
	}

	public ClassPathXmlApplicationContext getContext() {
		return tenant.getContext();
	}
	public Config getConfig() {
		return tenant.getConfig();
	}
	public HttpServer getWebServer() {
		if (getConfig().getBootstrap().isEnableWebserver()) {
			return tenant.getWebServer();
		} else {
			throw new IllegalAccessError("Config has the web server disabled");
		}
	}
	
	private void init(final Config config) {
		// TODO fix im ignoring config arg.
		final String serviceName = config.getBootstrap().getServiceName();
		logger.info("{}: Initializing context for service: {}", getClass().getSimpleName(), serviceName);
		tenants.put(config.getBootstrap().getServiceName(), tenant = new MinkaTenant());
		tenant.setConfig(config);
		final String configPath = "classpath:io/tilt/minka/config/context-minka-spring.xml";
		final ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext(new String[] { configPath }, false);
		tenant.setContext(ctx);
		ctx.addBeanFactoryPostProcessor(beanFactory-> beanFactory.registerSingleton("config", config));
		ctx.setDisplayName("minka-" + serviceName + "-ts:" + System.currentTimeMillis());
		logger.info("{}: Using configuration", getClass().getSimpleName(), config);
		ctx.setId(serviceName);
		logger.info("{}: Naming context: {}", getClass().getSimpleName(), ctx.getId());
		Runtime.getRuntime().addShutdownHook(new Thread(() -> destroy()));
		start();
	}
	   
	private void start() {
		if (!tenant.getContext().isActive()) {
			tenant.getContext().refresh();
			if (tenant.getConfig().getBootstrap().isEnableWebserver()) {
				startWebserver();
			}
		} else {
			logger.error("{}: Can only load Minka once !", getClass().getSimpleName());
		}
	}

	public void startWebserver() {
		final ResourceConfig res = new ResourceConfig(AdminEndpoint.class);
		res.property("contextConfig", tenant.getContext());
		final HttpServer webServer = GrizzlyHttpServerFactory.createHttpServer(resolveBindAddress(tenant.getConfig()), res);
		tenants.get(tenant.getConfig().getBootstrap().getServiceName()).setWebServer(webServer);
		try {
			webServer.start();
		} catch (IOException e) {
			logger.info("{}: Unable to start web server", getClass().getSimpleName(), e);
		}
    }

	/* by default bind to the same broker's host interfase and if changed use broker's port plus 100 */
	public URI resolveBindAddress(final Config config) {
		final String[] brokerHostPort = config.getBroker().getHostPort().split(":");
    	final JerseyUriBuilder builder = new JerseyUriBuilder();
    	final BootstrapConf bs = config.getBootstrap();
		final String[] webHostPort = bs.getWebServerHostPort().split(":");
		int webPort = Integer.parseInt(webHostPort[1]);
		final boolean webHostPortUntouched = bs.getWebServerHostPort().equals(Config.BootstrapConf.WEB_SERVER_HOST_PORT);
		if (webHostPortUntouched) {
			int brokerPort = Integer.parseInt(brokerHostPort[1]);
			webPort = brokerPort == Config.BrokerConf.PORT ? webPort: brokerPort + 100;
			final String host = config.getResolvedShardId().getStringIdentity().split(":")[0];
			builder.host(host).port(webPort);
			config.getBootstrap().setWebServerHostPort(host + ":" + webPort);
		} else {
			builder.host(webHostPort[0]).port(webPort);
			config.getBootstrap().setWebServerHostPort(webHostPort[0]+ ":" + webPort);
		}
		return builder.build();
	}
    
	private void checkInit() {
		if (!tenant.getContext().isActive()) {
			throw new IllegalStateException("Minka service must be started first !");
		}
	}
	
	public synchronized void destroy() {
		if (tenant != null && tenant.getContext().isActive()) {
			tenant.getContext().close();
			if (tenant.getConfig().getBootstrap().isEnableWebserver() && tenant.getWebServer()!=null) {
				tenant.getWebServer().shutdown();
			}
			tenant = null;
		} else {
			logger.error("{}: Can only destroy service's context once !", getClass().getSimpleName());
		}
	}
	
	/**
	 * Minka service must be fully initialized before being able to obtain an operative client
	 * @return	an instance of a client   
	 */
	public MinkaClient<D, P> getClient() {
		checkInit();
		return tenant.getContext().getBean(MinkaClient.class);
	}
	
	private DependencyPlaceholder getDepPlaceholder() {
		return tenant.getContext().getBean(DependencyPlaceholder.class);
	}
	/**
	 * An alternative way of mapping duty and pallet events, thru an implementation class.
	 * @param delegate	a fully implementation class of a partition delegate
	 * @return	the server builder
	 */
	public Minka<D, P> setDelegate(final PartitionDelegate<D, P> delegate) {
		Validate.notNull(delegate);
		checkInit();
		logger.info("{}: Using new PartitionDelegate: {}", getClass().getSimpleName(),
				delegate.getClass().getSimpleName());
		final DependencyPlaceholder holder = getDepPlaceholder();
		Validate.isTrue(holder==null || (holder.getDelegate() instanceof AwaitingDelegate), 
				"You're overwriting previous delegate or event's consumer");
		holder.setDelegate(delegate);
		return this;
	}
	/**
	 * An alternative way of mapping duty and pallet events, thru an implementation class.
	 * @param master a fully implementation class of a partition master
	 * @return	the server builder
	 */
	public Minka<D, P> setMaster(final PartitionMaster<D, P> master) {
		Validate.notNull(master);
		checkInit();
		logger.info("{}: Using new PartitionMaster: {}", getClass().getSimpleName(), master.getClass().getSimpleName());
		final DependencyPlaceholder holder = getDepPlaceholder();
		Validate.isTrue(holder==null || (holder.getMaster() instanceof AwaitingDelegate), 
				"You're overwriting previous delegate or event's consumer");
		getDepPlaceholder().setMaster(master);
		return this;
	}
	
	// ---------- consumer delegate usage 
	
	private synchronized void initConsumerDelegate() {
		checkInit();
		DependencyPlaceholder holder = getDepPlaceholder();
		if (holder.getMaster() == null || holder.getMaster() instanceof AwaitingDelegate) {
			final ConsumerDelegate<Serializable, Serializable> delegate = new ConsumerDelegate<>();
			holder.setDelegate(delegate);
			holder.setMaster(delegate);
		} else {
			Validate.isTrue(holder.getDelegate() instanceof ConsumerDelegate, 
					"You're trying event's consumer with an already implementation set");				
		}
	}
	/**
	 * Mandatory. In case the current shard's elected as Leader.
	 * Note duty instances should be created only once and then references returned. 
	 * To avoid inconsistency their return must always include any additions made thru {@linkplain MinkaClient}
	 * @param supplier	to be called only at shard's election as Leader  
	 * @return	the server builder
	 */
	public Minka<D, P> onDutyLoad(final Supplier<Set<Duty<D>>> supplier) {
		initConsumerDelegate();
		((ConsumerDelegate<D, P>)getDepPlaceholder().getMaster()).addSupplier(Event.loadduties, supplier);
		return this;
	}
	/**
	 * Mandatory. In case the current shard's elected as Leader.
	 * To avoid inconsistency their return must always include any additions made thru {@linkplain MinkaClient}
	 * @param supplier	to be called only at shard's election as Leader
	 * @return	the server builder  
	 */
	public Minka<D, P> onPalletLoad(final Supplier<Set<Pallet<P>>> supplier) {
		initConsumerDelegate();
		((ConsumerDelegate<D, P>)getDepPlaceholder().getMaster()).addPalletSupplier(supplier);
		return this;
	}
	/**
	 * Mandatory. Map duty assignation responsibilities to a consumer 
	 * @see PartitionDelegate
	 * @param consumer	to be called anytime a distribution and balance runs in the leader shard
	 * @return	the server builder
	 */
	public Minka<D, P> onDutyCapture(final Consumer<Set<Duty<D>>> consumer) {
		initConsumerDelegate();
		((ConsumerDelegate<D, P>)getDepPlaceholder().getDelegate()).addConsumer(consumer, Event.capture);
		return this;
	}
	/**
	 * Required optional. Map pallet assignation responsibilities to a consumer 
	 * @see PartitionDelegate
	 * @param consumer	to be called anytime a distribution and balance runs in the leader shard
	 * @return	the server builder
	 */
	public Minka<D, P> onPalletCapture(final Consumer<Set<Pallet<P>>> consumer) {
		initConsumerDelegate();
		((ConsumerDelegate<D, P>)getDepPlaceholder().getDelegate()).addConsumerPallet(consumer, Event.capture);
		return this;
	}	
	/**
	 * Mandatory. Map duty release contract to a consumer 
	 * @see PartitionDelegate
	 * @param consumer	to be called anytime a distribution and balance runs in the leader shard
	 * @return	the server builder
	 */
	public Minka<D, P> onDutyRelease(final Consumer<Set<Duty<D>>> consumer) {
		initConsumerDelegate();
		((ConsumerDelegate<D, P>)getDepPlaceholder().getDelegate()).addConsumer(consumer, Event.release);
		return this;
	}
	/**
	 * Required optional. Map pallet release contract to a consumer 
	 * @see PartitionDelegate
	 * @param consumer	to be called anytime a distribution and balance runs in the leader shard
	 * @return	the server builder
	 */
	public Minka<D, P> onPalletRelease(final Consumer<Set<Pallet<P>>> consumer) {
		initConsumerDelegate();
		((ConsumerDelegate<D, P>)getDepPlaceholder().getDelegate()).addConsumerPallet(consumer, Event.releasePallet);
		return this;
	}
	/**
	 * Mandatory. Map report contract to a consumer 
	 * @see PartitionDelegate
	 * @param supplier	to be called profusely by the follower process at the current shard
	 * @return	the server builder
	 */
	public Minka<D, P> onDutyReport(final Supplier<Set<Duty<D>>> supplier) {
		initConsumerDelegate();
		((ConsumerDelegate<D, P>)getDepPlaceholder().getDelegate()).addSupplier(Event.report, supplier);
		return this;
	}
	/**
	 * Optional. Map an update on the duty's payload to a consumer
	 * @param consumer	to be called only on client's call thru MinkaClient.update(..)
	 * @return	the server builder
	 */
	public Minka<D, P> onDutyUpdate(final Consumer<Duty<D>> consumer) {
		initConsumerDelegate();
		((ConsumerDelegate<D, P>)getDepPlaceholder().getDelegate()).addConsumerUpdate(consumer);
		return this;
	}
	/**
	 * Optional. Map an update on the pallet's payload to a consumer
	 * @param consumer	to be called only on client's call thru MinkaClient.update(..)
	 * @return	the server builder
	 */
	public Minka<D, P> onPalletUpdate(final Consumer<Pallet<P>> consumer) {
		initConsumerDelegate();
		((ConsumerDelegate<D, P>)getDepPlaceholder().getDelegate()).addConsumerUpdatePallet(consumer);
		return this;
	}
	/**
	 * Optional. Map duty object's transfer responsibilities to a receptionist consumer 
	 * @see PartitionDelegate
	 * @param biconsumer	to be called only on client's call thru MinkaClient.deliver(...)
	 * @return	the server builder
	 */
	public Minka<D, P> onDutyTransfer(final BiConsumer<Duty<D>, Serializable> biconsumer) {
		initConsumerDelegate();
		((ConsumerDelegate<D, P>)getDepPlaceholder().getDelegate()).addBiConsumerTransfer(biconsumer);
		return this;
	}
	public Minka<D, P> onPalletTransfer(final BiConsumer<Pallet<P>, Serializable> biconsumer) {
		initConsumerDelegate();
		((ConsumerDelegate<D, P>)getDepPlaceholder().getDelegate()).addBiConsumerTransferPallet(biconsumer);
		return this;
	}
	/**
	 * Optional. Adds a custom balancer
	 * @param balancer	to use at balancing phase
	 * @return	the server builder
	 */
	public Minka<D, P> onBalance(final Balancer balancer) {
		Validate.notNull(balancer);
		Balancer.Directory.addCustomBalancer(balancer);
		return this;
	}
	/**
	 * Optional. Map minka's service start to a consumer
	 * @param runnable callback to run at event dispatch
	 * @return	the server builder
	 */
	public Minka<D, P> onActivation(final Runnable runnable) {
		initConsumerDelegate();
		((ConsumerDelegate<D, P>)getDepPlaceholder().getDelegate()).addRunnable(Event.activation, runnable);
		return this;
	}
	/**
	 * Optional. Map minka's service shutdown to a consumer. 
	 * @param runnable	callback to run at event dispatch
	 * @return	the server builder
	 */
	public Minka<D, P> onDeactivation(final Runnable runnable) {
		initConsumerDelegate();
		((ConsumerDelegate<D, P>)getDepPlaceholder().getDelegate()).addRunnable(Event.deactivation, runnable);
		return this;
	}
	/**
	 * Mandatory for pallets using a weighted-balancer to be actually distributed. <br> 
	 * Explicitly set current shard's capacity for a given Pallet 
	 * @param weight 	must be in the same measure than duty weights grouped by this pallet
	 * @param pallet	the pallet to report capacity about
	 * @return	the server builder
	 */
	public Minka<D, P> setCapacity(final Pallet<P> pallet, final double weight) {
		initConsumerDelegate();
		((ConsumerDelegate<D, P>)getDepPlaceholder().getDelegate()).addCapacity(pallet, weight);
		return this;
	}
	/**
	 * Mandatory. After creating this loader the context is created and the Minka's bootstrap process waits
	 * for all mandatory and optional events to be mapped: before calling load()
	 * This explicitly releases the bootstrap wait, but not without event's mapping validation.
	 * The load will also occurr if all optional and mandatory events are mapped.
	 * @return	the server builder
	 */
	public Minka<D, P> load() {
		initConsumerDelegate();
		((ConsumerDelegate<D, P>)getDepPlaceholder().getDelegate()).setExplicitlyReady();
		return this;
	}

	private static class MinkaTenant {
		private HttpServer webServer;
		private ClassPathXmlApplicationContext context;
		private Config config;
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
