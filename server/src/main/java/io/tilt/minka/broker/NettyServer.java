/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.tilt.minka.broker;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.tilt.minka.api.config.SchedulerSettings;
import io.tilt.minka.core.task.Scheduler;
import io.tilt.minka.core.task.Scheduler.Agent;
import io.tilt.minka.core.task.Scheduler.Frequency;
import io.tilt.minka.core.task.Scheduler.PriorityLock;
import io.tilt.minka.core.task.Semaphore.Action;
import io.tilt.minka.spectator.MessageMetadata;

/**
 * Listen connections from other server's clients forwarding input data to a consumer 
 *  
 * @author Cristian Gonzalez
 * @since Mar 9, 2016
 *
 */
class NettyServer {

	final Logger logger = LoggerFactory.getLogger(getClass());

	/* for server */
	private NettyReceiver receiver;
	private EventLoopGroup serverWorkerGroup;
	final Consumer<MessageMetadata> consumer;

	private final int connectionHandlerThreads;
	private final int serverPort;
	private final String serverAddress;
	private final String networkInterfase;
	final Scheduler scheduler;
	private final Agent agent;
	
	private int retry;
	private final AtomicLong count;
	final String loggingName;
	final String classname = getClass().getSimpleName();
	
	private ChannelFuture channelFuture;
	

	NettyServer(
			final Consumer<MessageMetadata> consumer, 
			final int connectionHandlerThreads, 
			final int serverPort,
			final String serverAddress, 
			final String networkInterfase, 
			final Scheduler scheduler, 
			final int retryDelay, 
			final int maxRetries, 
			final String loggingName) {
		
		Validate.notNull(consumer);
		Validate.notNull(scheduler);
		
		this.receiver = new NettyReceiver(loggingName, (meta)-> {
			scheduler.schedule(scheduler.getAgentFactory()
					.create(
						Action.BROKER_INCOMING_MESSAGE, 
						PriorityLock.HIGH_ISOLATED, 
						Frequency.ONCE, 
						() -> consumer.accept(meta))
					.build());
		});
		this.consumer = consumer;
		this.connectionHandlerThreads = connectionHandlerThreads;
		this.serverPort = serverPort;
		this.serverAddress = serverAddress;
		this.networkInterfase = networkInterfase;
		this.scheduler = scheduler;
		this.count = new AtomicLong();
		this.loggingName = loggingName;
		this.agent = scheduler.getAgentFactory()
			.create(
				Action.BROKER_SERVER_START, 
				PriorityLock.HIGH_ISOLATED,
				Frequency.PERIODIC, () -> listenWithRetries(maxRetries, retryDelay))
			.every(5000l)
			.build();
		scheduler.schedule(agent);
	}

	/*
	 * a client for a shard's broker to send any type of messages, whether be
	 * follower or leader
	 */
	private void listenWithRetries(final int maxRetries, final int retryDelay) {
		final boolean alreadyInitiated = this.channelFuture!=null;
		final boolean withoutErrors = alreadyInitiated && channelFuture.cause()==null;
		final boolean shuttingDown = alreadyInitiated && serverWorkerGroup.isShuttingDown();
		if ((alreadyInitiated && withoutErrors) || shuttingDown) {
			return;
		}
		
		synchronized(this) {
			if (serverWorkerGroup==null) {
				this.serverWorkerGroup = new NioEventLoopGroup(
					this.connectionHandlerThreads,
					new ThreadFactoryBuilder()
						.setNameFormat(SchedulerSettings.THREAD_NAME_BROKER_SERVER_WORKER)
						.build());
			}
		}
		
		boolean disconnected = true;
		while (retry < maxRetries && disconnected &&!serverWorkerGroup.isShuttingDown()) {
			if (retry++ > 0) {
				try {
					Thread.sleep(retryDelay);
				} catch (InterruptedException e) {
					logger.error("{}: ({}) Unexpected while waiting for next server bootup retry",
							classname, loggingName, e);
				}
			}
			disconnected = keepListening();
		}
		if (disconnected) {
			scheduler.schedule(agent);
		}
	}

	/*
	 * whether be follower or leader the server will always hear for messages
	 */
	private boolean keepListening() {
		boolean disconnected;
		if (logger.isInfoEnabled()) {
		    logger.info("{}: ({}) Building server (using i: {}) with up to {} concurrent requests",
				classname, loggingName, networkInterfase, this.connectionHandlerThreads);
		}

		ServerBootstrap server = null;
		try {
			server = new ServerBootstrap();
			server.group(serverWorkerGroup)
			    .channel(NioServerSocketChannel.class)
				.handler(new LoggingHandler(LogLevel.INFO))
				.childHandler(new ChannelInitializer<SocketChannel>() {
					@Override
					public void initChannel(SocketChannel ch) throws Exception {
						ch.pipeline()
						    .addLast("encoder", new ObjectEncoder())
						    .addLast("decoder", new ObjectDecoder(ClassResolvers.weakCachingResolver(null)))
						    .addLast("handler", receiver)
						    .addLast(new ExceptionHandler())
						    ;
					}
				});			
			server.childOption(ChannelOption.SO_KEEPALIVE, true);
			server.childOption(ChannelOption.TCP_NODELAY, true);
			server.childOption(ChannelOption.AUTO_READ, true);
			server.childOption(ChannelOption.SO_REUSEADDR, true);
			
			if (logger.isInfoEnabled()) {
			    logger.info("{}: ({}) Listening to client connections (using i:{}) with up to {} concurrent requests",
					classname, loggingName, networkInterfase, this.connectionHandlerThreads);
			}

			this.channelFuture = server.bind(this.serverAddress, this.serverPort);
			disconnected = false;
		} catch (Exception e) {
			disconnected = true;
			logger.error("{}: ({}) Unexpected interruption while listening incoming connections",
					classname, loggingName, e);
		}
		return disconnected;
	}
	
	public void close() {
		logger.warn("{}: ({}) Closing connections to client (total received: {})", classname, loggingName, count.get());
		if (serverWorkerGroup != null) {
			serverWorkerGroup.shutdownGracefully();
		}
	}

	public Map<String, Map<String, Integer>> getCountByTypeAndHost() {
		return this.receiver.countByTypeAndHost;
	}
	
	public AtomicLong getCount() {
		return count;
	}
	public int getRetry() {
		return retry;
	}

}
