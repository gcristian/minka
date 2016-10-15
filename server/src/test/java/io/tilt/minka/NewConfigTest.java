/**
 * TASKS * Copyright (c) 2011-2015 Zauber S.A. -- All rights reserved
 */

package io.tilt.minka;

import java.util.Properties;

import org.junit.Test;

import io.tilt.minka.api.NewConfig;
import io.tilt.minka.utils.Defaulter;
import junit.framework.Assert;

public class NewConfigTest {

	@Test
	public void testConfigSerialization() throws Exception {

		final NewConfig conf = new NewConfig();
		final String json = conf.toJson();
		System.out.println(json);
		
		NewConfig fromString = NewConfig.fromString(json);
		String toStr = fromString.toJson();
		System.out.println(toStr);
		Assert.assertTrue(toStr.equals(json));
	}

	@Test
	public void testConfigDefaults() throws Exception {

		final Properties prop = new Properties();
		NewConfig conf = new NewConfig();
		Defaulter.apply(prop, "concistency.", conf.getConsistency());
		Defaulter.apply(prop, "balancer.", conf.getBalancer());
		Defaulter.apply(prop, "bootstrap.", conf.getBootstrap());
		Defaulter.apply(prop, "broker.", conf.getBroker());
		Defaulter.apply(prop, "distributor.", conf.getDistributor());
		Defaulter.apply(prop, "follower.", conf.getFollower());
		Defaulter.apply(prop, "scheduler.", conf.getScheduler());
		Defaulter.apply(prop, "shepherd.", conf.getShepherd());

		System.out.println(conf.toJson());
		
	}}