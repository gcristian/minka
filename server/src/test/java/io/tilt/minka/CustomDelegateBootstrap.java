package io.tilt.minka;

import org.junit.Test;

import io.tilt.minka.api.MinkaClient;
import io.tilt.minka.api.MinkaContextLoader;
import io.tilt.minka.api.PartitionMaster;

public class CustomDelegateBootstrap extends BootstrapTesting {

	public static void main(String[] args) throws Exception {
		CustomDelegateBootstrap custom = new CustomDelegateBootstrap();
		custom.test();
	}

	@Test
	public void test() {
		try {
			logger.info("loading minka");
			MinkaContextLoader loader = new MinkaContextLoader();
			loader.load();
			final String clazz = System.getProperty("delegate", "MultiPalletRandomSample");
			logger.info("creating custom delegate");
			final PartitionMaster<?, ?> master = (PartitionMaster<?, ?>) 
					Class.forName("io.tilt.minka.delegates." + clazz).newInstance();
			loader.setMaster(master);
			loader.setDelegate(master);
			final MinkaClient cli = MinkaClient.getInstance();
			sleep(1);
			logger.info("sending new duties");
			//cli.add(DutyBuilder.build(String.class, "hola", "1"));
			int mins = Integer.getInteger("mins", 30);
			logger.info("sleeping for {} minutes zz.zz.....", mins);
			sleep(mins);
			logger.info("planned suicide... ");
			loader.destroy();
			Thread.sleep(10 * 1000l);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
