package io.tilt.minka.api;

import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import io.tilt.minka.api.ConsumerDelegate.Event;

/**
 * An alternative of starting the Minka service instead of including Minka Spring bean in your context.
 * Enabling programatic customization.
 * The service remains alive while this class stays loaded and non-stopped.
 * 
 * 1) create loader with custom json config file
 * 2) load the service with a mocked {@link PartitionDelegate} (never ready)
 * 3) after set another partition delegates or master
 * 4) or use event consumers 
 */
@SuppressWarnings("unchecked")
public class MinkaContextLoader<D extends Serializable, P extends Serializable> {

	private static final Logger logger = LoggerFactory.getLogger(MinkaContextLoader.class);

	private static Map<String, ClassPathXmlApplicationContext> ctxs = new HashMap<>();

	private final String service;

	/** 
	 * Create a Minka server. All mandatory events must be mapped to consumers/suppliers.
	 * @param configFilepath with a configuration in a JSON file, 
	 * whose format must comply {@linkplain Config} class serialization */
	public MinkaContextLoader(final File jsonFormatConfig) throws Exception {
		Validate.notNull(jsonFormatConfig);
		Config config = Config.fromJsonFile(jsonFormatConfig);
		this.service = config.getBootstrap().getServiceName();
		init(config);
	}
	/**
	 * Create a Minka server. All mandatory events must be mapped to consumers/suppliers. 
	 * @param config Create a Minka server with a specific configuration */
	public MinkaContextLoader(final Config config) {
		Validate.notNull(config);
		this.service = config.getBootstrap().getServiceName();
		init(config);
	}
	/** 
	 * Create a Minka server. All mandatory events must be mapped to consumers/suppliers.
	 * one liner to custom main TCP hostname/ports only
	 * @param zookeeperConnectionString		in the form hostname:port/chroot 
	 * @param minkaHostPort		in the form hostname:port
	 */
	public MinkaContextLoader(final String zookeeperConnectionString, final String minkaHostPort)  {
		Validate.notNull(zookeeperConnectionString);
		Validate.notNull(minkaHostPort);
		final Config config = new Config(zookeeperConnectionString, minkaHostPort);
		this.service = config.getBootstrap().getServiceName();
		init(config);
	}
	/**
	 * Create a Minka server with default configuration. 
	 * All mandatory events must be mapped to consumers/suppliers.
	 * Shard will attempt to take a port over 5748, trying increased ports if busy, depending on Config.
	 * @param zookeeperConnectionString in the zookeeper form hostname:port/chroot
	 */
	public MinkaContextLoader(final String zookeeperConnectionString)  {
		Validate.notNull(zookeeperConnectionString);
		Config conf = new Config(zookeeperConnectionString);
		this.service = conf.getBootstrap().getServiceName();
		init(conf);
	}
	/**
	 * Create a Minka server with default configuration. 
	 * All mandatory events must be mapped to consumers/suppliers.
	 * Shard will attempt to take a port over 5748, trying increased ports if busy.
	 */
	public MinkaContextLoader()  {
		Config conf = new Config();
		this.service = conf.getBootstrap().getServiceName();
		init(conf);
	}

	private void init(final Config config) {
		logger.info("{}: Initializing context for service: {}", getClass().getSimpleName(), 
				service);
		ClassPathXmlApplicationContext ctx = ctxs.get(service);
		if (ctx != null) {
			throw new IllegalStateException("Minka service " + service + " already loaded !");
		}
		final String configPath = "classpath:io/tilt/minka/config/context-minka-spring.xml";
		ctx = new ClassPathXmlApplicationContext(new String[] { configPath }, false);
		ctxs.put(service, ctx);
		ctx.setDisplayName("minka-" + service + "-ts:" + System.currentTimeMillis());
		if (config != null) {
			logger.info("{}: Using configuration", getClass().getSimpleName());
			
			//((ConfigurableApplicationContext)ctx).getBeanFactory().registerSingleton(
			//	config.getClass().getCanonicalName(), config);
			logger.info("{}: Naming context: {}", getClass().getSimpleName(), ctx.getId());
			ctx.setId(service);
		}
		Runtime.getRuntime().addShutdownHook(new Thread(() -> destroy()));
		start();
	}

	public boolean isActive() {
		return ctxs.get(service).isActive();
	}
	
	/**
	 * load and start Minka service.   
	 */
	private void start() {
		if (!ctxs.get(service).isActive()) {
			ctxs.get(service).refresh();
		} else {
			logger.error("{}: Can only load Minka once !", getClass().getSimpleName());
		}
	}

	private void checkInit() {
		if (!ctxs.get(service).isActive()) {
			throw new IllegalStateException("Minka service must be started first !");
		}
	}
	
	public synchronized void destroy() {
		if (ctxs.get(service) != null && ctxs.get(service).isActive()) {
			ctxs.get(service).close();
		} else {
			logger.error("{}: Can only destroy service's context once !", getClass().getSimpleName());
		}
	}
	
	private DependencyPlaceholder getDepPlaceholder() {
		return ctxs.get(service).getBean(DependencyPlaceholder.class);
	}

	public MinkaContextLoader<D, P> setDelegate(final PartitionDelegate<D, P> delegate) {
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


	public MinkaContextLoader<D, P> setMaster(final PartitionMaster<D, P> master) {
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
	 */
	public MinkaContextLoader<D, P> onDutyLoad(final Supplier<Set<Duty<D>>> supplier) {
		initConsumerDelegate();
		((ConsumerDelegate<D, P>)getDepPlaceholder().getMaster()).addSupplier(Event.loadduties, supplier);
		return this;
	}
	/**
	 * Mandatory. In case the current shard's elected as Leader.
	 * To avoid inconsistency their return must always include any additions made thru {@linkplain MinkaClient}
	 * @param supplier	to be called only at shard's election as Leader  
	 */
	public MinkaContextLoader<D, P> onPalletLoad(final Supplier<Set<Pallet<P>>> supplier) {
		initConsumerDelegate();
		((ConsumerDelegate<D, P>)getDepPlaceholder().getMaster()).addPalletSupplier(supplier);
		return this;
	}
	/**
	 * Mandatory. Map duty assignation responsibilities to a consumer 
	 * @see PartitionDelegate
	 * @param consumer	to be called anytime a distribution and balance runs in the leader shard
	 */
	public MinkaContextLoader<D, P> onTake(final Consumer<Set<Duty<D>>> consumer) {
		initConsumerDelegate();
		((ConsumerDelegate<D, P>)getDepPlaceholder().getDelegate()).addConsumer(consumer, Event.take);
		return this;
	}
	public MinkaContextLoader<D, P> onTakePallet(final Consumer<Set<Pallet<P>>> consumer) {
		initConsumerDelegate();
		((ConsumerDelegate<D, P>)getDepPlaceholder().getDelegate()).addConsumerPallet(consumer, Event.take);
		return this;
	}	/**
	 * Mandatory. Map duty release contract to a consumer 
	 * @see PartitionDelegate
	 * @param consumer	to be called anytime a distribution and balance runs in the leader shard
	 */
	public MinkaContextLoader<D, P> onRelease(final Consumer<Set<Duty<D>>> consumer) {
		initConsumerDelegate();
		((ConsumerDelegate<D, P>)getDepPlaceholder().getDelegate()).addConsumer(consumer, Event.release);
		return this;
	}
	public MinkaContextLoader<D, P> onReleasePallet(final Consumer<Set<Pallet<P>>> consumer) {
		initConsumerDelegate();
		((ConsumerDelegate<D, P>)getDepPlaceholder().getDelegate()).addConsumerPallet(consumer, Event.releasePallet);
		return this;
	}
	/**
	 * Mandatory. Map report contract to a consumer 
	 * @see PartitionDelegate
	 * @param supplier	to be called profusely by the follower process at the current shard
	 */
	public MinkaContextLoader<D, P> onReport(final Supplier<Set<Duty<D>>> supplier) {
		initConsumerDelegate();
		((ConsumerDelegate<D, P>)getDepPlaceholder().getDelegate()).addSupplier(Event.report, supplier);
		return this;
	}
	/**
	 * Optional. Map an update on the duty's payload to a consumer
	 * @param consumer	to be called only on client's call thru MinkaClient.update(..)
	 */
	public MinkaContextLoader<D, P> onUpdate(final Consumer<Duty<D>> consumer) {
		initConsumerDelegate();
		((ConsumerDelegate<D, P>)getDepPlaceholder().getDelegate()).addConsumerUpdate(consumer);
		return this;
	}
	/**
	 * Optional. Map duty object's delivery responsibilities to a consumer 
	 * @see PartitionDelegate
	 * @param consumer	to be called only on client's call thru MinkaClient.deliver(...)
	 */
	public MinkaContextLoader<D, P> onDeliver(final BiConsumer<Duty<D>, Serializable> biconsumer) {
		initConsumerDelegate();
		((ConsumerDelegate<D, P>)getDepPlaceholder().getDelegate()).addBiConsumerDeliver(biconsumer);
		return this;
	}
	/**
	 * Optional. 
	 * @param runnable
	 */
	public MinkaContextLoader<D, P> onActivation(final Runnable runnable) {
		initConsumerDelegate();
		((ConsumerDelegate<D, P>)getDepPlaceholder().getDelegate()).addRunnable(Event.activation, runnable);
		return this;
	}
	/**
	 * Optional. 
	 * @param runnable
	 */
	public MinkaContextLoader<D, P> onDeactivation(final Runnable runnable) {
		initConsumerDelegate();
		((ConsumerDelegate<D, P>)getDepPlaceholder().getDelegate()).addRunnable(Event.deactivation, runnable);
		return this;
	}
	/**
	 * Mandatory for pallets using a weighted-balancer to be actually distributed. 
	 * Explicitly set current shard's capacities for a given Pallet 
	 * @param weight 	must be in the same measure than duty weights grouped by this pallet
	 */
	public MinkaContextLoader<D, P> setCapacity(final Pallet<P> pallet, final double weight) {
		initConsumerDelegate();
		((ConsumerDelegate<D, P>)getDepPlaceholder().getDelegate()).addCapacity(pallet, weight);
		return this;
	}
	/**
	 * Optional. After creating this loader the context is created and the Minka's bootstrap process waits
	 * for all mandatory and optional events to be mapped: before calling load()
	 * This explicitly releases the bootstrap wait, but not without event's mapping validation.
	 * The load will also occurr if all optional and mandatory events are mapped.
	 */
	public MinkaContextLoader<D, P> load() {
		initConsumerDelegate();
		((ConsumerDelegate<D, P>)getDepPlaceholder().getDelegate()).setExplicitlyReady();
		return this;
	}

}