
package io.tilt.minka;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import io.tilt.minka.core.task.Bootstrap;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:io/tilt/minka/config/context-minka-test-spring.xml" })
public class BootstrapTest {

	@Autowired
	private Bootstrap bootstrap;

	@Test
	public void test_bootstrap() throws InterruptedException {

		int mins = Integer.getInteger("mins", 30);
		for (int i = 0; i < mins; i++) {
			Thread.sleep(60 * 1000l);
			for (int j = 0; j < 200000; j++)
				;
		}
		bootstrap.stop();
		Thread.sleep(10 * 1000l);

	}

}
