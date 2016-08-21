/**
 * Copyright (c) 2011-2015 Zauber S.A. -- All rights reserved
 */
package io.tilt.minka;

import org.junit.Test;

import io.tilt.minka.api.DutyBuilder;
import io.tilt.minka.api.MinkaClient;
import io.tilt.minka.api.MinkaLoader;
import io.tilt.minka.api.PartitionMaster;

public class CustomDelegateBootstrap {

	public static void main(String[] args) throws Exception {
		CustomDelegateBootstrap custom = new CustomDelegateBootstrap();
		custom.test();
	}

	@Test
	public void test()
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, InterruptedException {
		MinkaLoader loader = new MinkaLoader();
		loader.load();
		final String clazz = System.getProperty("delegate", "MultiPalletSample");
		final PartitionMaster<?, ?> master = (PartitionMaster<?, ?>)
				Class.forName("io.tilt.minka.delegates." + clazz).newInstance();
		loader.setMaster(master);
		loader.setDelegate(master);
		final MinkaClient  cli = MinkaClient.getInstance();
		sleep(1);
		cli.add(DutyBuilder.build(String.class, "hola", "PALETA"));
		int mins = Integer.getInteger("mins", 30);
		sleep(mins);
		loader.destroy();
		Thread.sleep(10 * 1000l);
	}

	private static void sleep(final int mins) throws InterruptedException {
		for (int i = 0; i < mins; i++) {
			Thread.sleep(60 * 1000l);
			for (int j = 0; j < 200000; j++);
		}
	}

}
