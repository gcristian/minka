
package io.tilt.minka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.MinkaClient;

public class BootstrapTesting {

	protected static final Logger logger = LoggerFactory.getLogger(MinkaClient.class);
	public static void sleep(final int mins) throws InterruptedException {
		for (int i = 0; i < mins; i++) {
			Thread.sleep(60 * 1000l);
			for (int j = 0; j < 200000; j++)
				;
		}
	}

}
