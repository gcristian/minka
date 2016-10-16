/**
 * Copyright (c) 2011-2015 Zauber S.A. -- All rights reserved
 */

package io.tilt.minka;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import io.tilt.minka.api.Config;
import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.broker.EventBroker.Channel;
import io.tilt.minka.broker.impl.SocketBroker;
import io.tilt.minka.core.task.LeaderShardContainer;
import io.tilt.minka.core.task.impl.SchedulerImpl;
import io.tilt.minka.core.task.impl.SpectatorSupplier;
import io.tilt.minka.core.task.impl.SynchronizedAgentFactoryImpl;
import io.tilt.minka.core.task.impl.SynchronizedFactoryImpl;
import io.tilt.minka.domain.NetworkShardID;

public class SocketBrokerTest extends AbstractBrokerTester {

	@Override
	protected EventBroker buildBroker(final Consumer<Serializable> driver, final LeaderShardContainer container,
			final Config config, final NetworkShardID shard) {

		final EventBroker broker = new SocketBroker(config, shard, container,
				new SchedulerImpl(config, new SpectatorSupplier(config), shard, new SynchronizedAgentFactoryImpl(),
						new SynchronizedFactoryImpl()));
		broker.subscribe(broker.buildToTarget(config, Channel.INSTRUCTIONS_TO_FOLLOWER, shard), AtomicInteger.class,
				driver, System.currentTimeMillis());
		return broker;
	}

}
