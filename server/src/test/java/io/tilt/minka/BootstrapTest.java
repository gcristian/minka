/**
 * Copyright (c) 2011-2015 Zauber S.A. -- All rights reserved
 */
package io.tilt.minka;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import io.tilt.minka.api.PartitionService;
import io.tilt.minka.core.Bootstrap;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:io/tilt/minka/config/context-minka-test-spring.xml" })
public class BootstrapTest {

		@Autowired
		private Bootstrap bootstrap;
		@Autowired
		private PartitionService service;

		@Test
		public void test_bootstrap() throws InterruptedException {

			System.out.println("--------------- hello -----------------");
			System.out.println("Sleeping while bootstrap running");
			int mins = Integer.getInteger("mins", 30);
			for (int i = 0; i < mins; i++) {
				Thread.sleep(60 * 1000l);
				for (int j = 0; j < 200000; j++)
						;
			}
			System.out.println("--------------- god bye -----------------");
			bootstrap.destroy();
			Thread.sleep(10 * 1000l);

		}

}
