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
package io.tilt.minka.broker.impl;

import static java.util.Objects.requireNonNull;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import io.tilt.minka.api.Config;
import io.tilt.minka.broker.EventBroker.BrokerChannel;
import io.tilt.minka.core.task.Scheduler;
import io.tilt.minka.core.task.Scheduler.Agent;
import io.tilt.minka.core.task.Scheduler.Frequency;
import io.tilt.minka.core.task.Scheduler.PriorityLock;
import io.tilt.minka.core.task.Semaphore.Action;
import io.tilt.minka.domain.NetworkShardIdentifier;
import io.tilt.minka.spectator.MessageMetadata;

/**
 * Creates and persist connections to other servers, forwarding data to them 
 * 
 * @author Cristian Gonzalez
 * @since Mar 9, 2016
 */
public class SocketClient {

	final Logger logger = LoggerFactory.getLogger(getClass());
	private final String classname = getClass().getSimpleName();
	
	/* for client */
	private final AtomicBoolean alive;
	private final AtomicInteger retry;
	private final long clientExpiration;
	private final AtomicBoolean antiflapper;
	private final int maxQueueThreshold;
	private final Scheduler scheduler; 
	private final Agent connector;
	
	private EventLoopGroup clientGroup;
	private SocketClientHandler clientHandler;
	
	private String loggingName;
	private long creation;
	private long lastUsage;
	private long sentCounter;

	protected SocketClient(
			final BrokerChannel channel, 
			final Scheduler scheduler, 
			final int retryDelay,
			final int maxRetries, 
			final String loggingName, 
			final Config config) {

		this.loggingName = loggingName;
		this.clientHandler = new SocketClientHandler(
				config.beatToMs(config.getBroker().getMaxLagBeforeDiscardingClientQueueBeats()), 
				config.getBroker().getMaxClientQueueSize());
		this.antiflapper = new AtomicBoolean(true);
		this.alive = new AtomicBoolean();
		this.retry  = new AtomicInteger();
		this.scheduler = requireNonNull(scheduler);
		this.connector = scheduler.getAgentFactory()
			.create(
				Action.BROKER_CLIENT_START, 
				PriorityLock.HIGH_ISOLATED,
				Frequency.ONCE, 
				() -> keepConnecting(channel, maxRetries, retryDelay))
			.build();
		scheduler.schedule(connector);
		this.creation = System.currentTimeMillis();
		this.clientExpiration = Math.max(
				requireNonNull(config).beatToMs(config.getProctor().getDelayBeats()), 
				config.beatToMs(config.getFollower().getClearanceMaxAbsenceBeats()));
		this.maxQueueThreshold = config.getBroker().getConnectionHandlerThreads();
	}

	protected long getCreation() {
		return creation;
	}

	protected boolean hasExpired() {
		if (creation == 0) {
			return false;
		} else {
			long elapsed = System.currentTimeMillis() - lastUsage;
			if (elapsed > this.clientExpiration) {
				logger.warn("{}: ({}) expired ! {} old (for max is: {})", classname, loggingName,
						elapsed, this.clientExpiration);
				return true;
			} else {
				return false;
			}
		}
	}

	protected boolean send(final MessageMetadata msg) {
		logging(msg);
		this.lastUsage = System.currentTimeMillis();
		return this.clientHandler.send(msg);
	}

	private void logging(final MessageMetadata msg) {
		int queueSize = this.clientHandler.size();
		if (!alive.get()) {
			if (!antiflapper.get() || sentCounter ==0) {
				logger.warn("{}: ({}) UNABLE to send SocketChannel Not Ready (enqueuing: {}), {}", classname, 
					loggingName, queueSize, msg);
			}
			antiflapper.set(true);
		}
		if (alive.get() && antiflapper.get()) {
			logger.info("{}: ({}) Back to normal", classname, loggingName);
			antiflapper.set(false);
		}
		sentCounter++;
		if (logger.isDebugEnabled()) {
		    logger.debug("{}: ({}) Sending: {}", classname, loggingName, msg.getPayloadType());
		} else if (logger.isInfoEnabled() && (sentCounter%500==0)) {
			logger.info("{}: ({}) Sent {} # {}", classname, loggingName, msg.getPayloadType(), sentCounter);
		}
		
		if (queueSize>maxQueueThreshold) {
			logger.error("{}: ({}) LAG of {}, threshold {}, increase broker's connection handler threads (enqueuing: {})", 
					classname, loggingName, queueSize, maxQueueThreshold, msg);
		}
	}

	/*
	 * a client for a shard's broker to send any type of messages, whether be follower or leader
	 * retry MAX_TRIES in the same thread or RESCHEDULE IT
	 */
	private void keepConnecting(final BrokerChannel channel, final int maxRetries, final int retryDelay) {
		clientGroup = new NioEventLoopGroup(1,
				new ThreadFactoryBuilder()
					.setNameFormat(Config.SchedulerConf.THREAD_NANE_TCP_BROKER_CLIENT)
					.build());

		boolean wronglyDisconnected = true;
		while (retry.get() < maxRetries && wronglyDisconnected) {
			sleepIfMust(retryDelay);
			wronglyDisconnected = connect(channel);
		}
		if (wronglyDisconnected) {
			// ok failed, leave the thread but re-schedule it
			retry.set(0);
			this.scheduler.schedule(connector);
		}
	}

	private void sleepIfMust(final int retryDelay) {
		if (retry.incrementAndGet() > 0) {
			try {
				if (logger.isInfoEnabled()) {
					logger.info("{}: ({}) Sleeping {} ms before next retry...", classname, loggingName, retryDelay);
				}
				Thread.sleep(retryDelay);
			} catch (InterruptedException e) {
				logger.error("{}: ({}) Unexpected while waiting for next client connection retry",
						classname, loggingName, e);
			}
		}
	}
	

	private boolean connect(final BrokerChannel channel) {
		boolean wrongDisconnection;
		try {
			final NetworkShardIdentifier addr = channel.getAddress();
			final String address = addr.getAddress().getHostAddress();
			final int port = addr.getPort();
			if (logger.isInfoEnabled()) {
				logger.info("{}: ({}) Building client (retry:{}) for outbound messages to: {}", 
					classname, loggingName, retry, addr);
			}
			final Bootstrap bootstrap = new Bootstrap();
			
			bootstrap.group(clientGroup)
			    .channel(NioSocketChannel.class)
			    .handler(new ChannelInitializer<SocketChannel>() {
				@Override
				public void initChannel(SocketChannel ch) throws Exception {
					ch.pipeline()
						.addLast("encoder", new ObjectEncoder())
						.addLast("decoder", new ObjectDecoder(ClassResolvers.weakCachingResolver(null)))
						.addLast("handler", clientHandler)
						.addLast(new ExceptionHandler());
				}
			});
			this.alive.set(true);
			if (logger.isInfoEnabled()) {
				logger.info("{}: ({}) Binding to broker: {}:{} at channel: {}", classname, loggingName,
					address, port, channel.getChannel().name());
			}
			bootstrap.connect(addr.getAddress().getHostAddress(), addr.getPort())
				.sync();
				//.channel().closeFuture().sync();
			wrongDisconnection = false;
		} catch (InterruptedException ie) {
			wrongDisconnection = false;
		} catch (Exception e) {
			this.alive.set(false);
			wrongDisconnection = true;
			logger.error("{}: ({}) Unexpected while contacting shard's broker", classname, loggingName, e);
		} finally {
			if (logger.isInfoEnabled()) {
				logger.info("{}: ({}) Exiting client writing scope", classname, loggingName);
			}
		}
		return wrongDisconnection;
	}

	@Sharable
	protected class SocketClientHandler extends ChannelInboundHandlerAdapter {
		private final BlockingQueue<MessageMetadata> queue;
		private final long maxLagBeforeDiscardingClientQueueBeats;
		private final int maxClientQueueSize;

		public SocketClientHandler(
				final long maxLagBeforeDiscardingClientQueueBeats,
				final int maxClientQueueSize) {
			this.queue = new ArrayBlockingQueue<>(maxClientQueueSize, true);
			this.maxLagBeforeDiscardingClientQueueBeats = maxLagBeforeDiscardingClientQueueBeats;
			this.maxClientQueueSize = maxClientQueueSize;
		}

		protected boolean send(final MessageMetadata msg) {
			boolean sent = queue.offer(msg);
			if (!sent && isLagTooHigh()) {
			    queue.clear();
			    sent = queue.offer(msg);
			}
			return sent;
		}

		private boolean isLagTooHigh() {
			final MessageMetadata eldest = queue.peek();
			if ((System.currentTimeMillis() - eldest.getCreatedAt()) > maxLagBeforeDiscardingClientQueueBeats || queue
					.size() == maxClientQueueSize) {
				logger.error("{}: ({}) Clearing queue for LAG reached LIMIT - increment client connector threads size", getClass()
						.getSimpleName(), loggingName);
				return true;
			} else {
				return false;
			}
		}

		protected int size() {
			return queue.size();
		}
		@Override
		public void channelInactive(ChannelHandlerContext ctx) throws Exception {
			logger.warn("{}: ({}) channel inactivated", classname, loggingName);
			super.channelInactive(ctx);
		}
		@Override
		public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
			logger.warn("{}: ({}) channel unregistered", classname, loggingName);
			super.channelUnregistered(ctx);
		}
		@Override
		public void channelActive(final ChannelHandlerContext ctx) {
			MessageMetadata msg = null;
			try {
				while (!Thread.interrupted()) {
					msg = queue.take();
					if (msg != null) {
						if (logger.isDebugEnabled()) {
							logger.debug("{}: ({}) Writing: {}", classname, loggingName, msg.getPayloadType());
						}
						ctx.writeAndFlush(msg);
					} else {
						logger.error("{}: ({}) Waiting for messages to be enqueued: {}", classname,
								loggingName);
					}
				}
			} catch (InterruptedException e) {
				logger.error("{}: ({}) interrupted while waiting for Socketclient blocking queue gets offered",
						classname, loggingName, e);
			}
		}

		@Override
		public void channelRead(ChannelHandlerContext ctx, Object msg) {
			ctx.write(msg);
		}

		@Override
		public void channelReadComplete(ChannelHandlerContext ctx) {
			ctx.flush();
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
			logger.error("{}: ({}) ChannelInboundHandlerAdapter: Unexpected ", 
					classname, loggingName, cause);
			ctx.close();
		}

	}

	public void close() {
		if (clientGroup != null && !clientGroup.isShuttingDown()) {
			if (logger.isInfoEnabled()) {
				logger.info("{}: ({}) Closing connection to server (total sent: {}, unsent msgs: {})", 
					classname, loggingName, sentCounter, clientHandler.size());
			}
			this.alive.set(false);
			clientGroup.shutdownGracefully();
		} else {
			logger.error("{}: ({}) Invalid state to close client: {}", classname, loggingName,
					clientGroup == null ? "not started" : "already shut-down");
		}
	}

}
