package io.tilt.minka;

import org.junit.Test;

import io.tilt.minka.api.MinkaClient;
import io.tilt.minka.api.Minka;
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
			Minka<String, String> server = new Minka<>();
			final String clazz = System.getProperty("delegate", "MultiPalletRandomSample");
			logger.info("creating custom delegate");
			@SuppressWarnings("unchecked")
			final PartitionMaster<String, String> master = (PartitionMaster<String, String>) 
					Class.forName("io.tilt.minka.delegates." + clazz).newInstance();
			server.setMaster(master);
			server.setDelegate(master);
			server.load();
			final MinkaClient<String, String> cli = server.getClient();
			sleep(1);
			logger.info("sending new duties");
			//cli.add(DutyBuilder.build(String.class, "hola", "1"));
			int mins = Integer.getInteger("mins", 30);
			logger.info("sleeping for {} minutes zz.zz.....", mins);
			sleep(mins);
			logger.info("planned suicide... ");
			server.destroy();
			Thread.sleep(10 * 1000l);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
