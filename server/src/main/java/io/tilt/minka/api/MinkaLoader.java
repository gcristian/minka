package io.tilt.minka.api;

import java.util.Properties;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * An alternative of starting the Minka service instead of including Minka Spring bean in your context.
 * Enabling programatic customization.
 * The service remains alive while this class stays loaded and non-stopped.
 * 
 * 1) create loader with custom properties or custom property path
 * 2) load the service with a mocked {@link PartitionDelegate} (never ready)
 * 3) after set another partition delegates or master
 */
public class MinkaLoader {

	private static final Logger logger = LoggerFactory.getLogger(MinkaLoader.class);
	
	private static ClassPathXmlApplicationContext ctx; 

	public MinkaLoader(final Properties p) {
		Validate.notNull(p);
		init(p);
	}

	public MinkaLoader() {
		init(null);
	}

	private void init(final Properties p) {
		logger.info("{}: Initializing context", getClass().getSimpleName());
		if (ctx !=null) {
			throw new IllegalStateException("Minka service already loaded !");
		}	
		String configPath = "classpath:io/tilt/minka/config/context-minka-spring.xml";
		ctx = new ClassPathXmlApplicationContext(new String[]{configPath}, false);
		if (p!=null) {
			logger.info("{}: Using custom properties", getClass().getSimpleName());
			ctx.setId(p.getProperty("serviceName", "minka-default-unnamed-" + System.currentTimeMillis()));
			logger.info("{}: Naming context: {}", getClass().getSimpleName(), ctx.getId());
			PropertyPlaceholderConfigurer propConfig = new PropertyPlaceholderConfigurer();
			propConfig.setProperties(p);
			ctx.addBeanFactoryPostProcessor(propConfig);
		}
		Runtime.getRuntime().addShutdownHook(new Thread(() -> destroy() ));
	}
	
	public boolean isActive() {
		return ctx.isActive();
	}

	/**
	 * load and start Minka service.   
	 */	
	public void load() {
		if (!ctx.isActive()) {
			ctx.refresh();
		} else {
			logger.error("{}: Can only load Minka once !", getClass().getSimpleName());
		}
	}
	
	public <T, P>void setDelegate(final PartitionDelegate<?, ?> delegate) {
		Validate.notNull(delegate);
		checkInit();
		logger.info("{}: Using new PartitionDelegate: {}", getClass().getSimpleName(), delegate.getClass().getSimpleName());
		DependencyPlaceholder holder = ctx.getBean(DependencyPlaceholder.class);
		holder.setDelegate(delegate);
	}

	private void checkInit() {
		if (!ctx.isActive()) {
			throw new IllegalStateException("Minka service must be started first !"); 
		}
	}
	
	public <T, P>void setMaster(final PartitionMaster<?, ?> master) {
		Validate.notNull(master);
		checkInit();
		logger.info("{}: Using new PartitionMaster: {}", getClass().getSimpleName(), master.getClass().getSimpleName());
		DependencyPlaceholder holder = ctx.getBean(DependencyPlaceholder.class);
		holder.setMaster(master);
	}
	
	public synchronized void destroy() {
		if (ctx!=null && ctx.isActive()) {
			ctx.close();
		} else {
			logger.error("{}: Can only destroy service's context once !", getClass().getSimpleName());
		}
	}
	
	
}
