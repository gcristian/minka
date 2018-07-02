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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.stream.ChunkedStream;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.tilt.minka.api.Config;
import io.tilt.minka.api.config.SchedulerSettings;
import io.tilt.minka.broker.EventBroker.BrokerChannel;
import io.tilt.minka.broker.impl.CustomCoder.Block;
import io.tilt.minka.core.task.Scheduler;
import io.tilt.minka.core.task.Scheduler.Agent;
import io.tilt.minka.core.task.Scheduler.Frequency;
import io.tilt.minka.core.task.Scheduler.PriorityLock;
import io.tilt.minka.core.task.Semaphore.Action;
import io.tilt.minka.shard.NetworkShardIdentifier;
import io.tilt.minka.spectator.MessageMetadata;

/**
 * Creates and persist connections to other servers, forwarding data to them 
 * 
 * @author Cristian Gonzalez
 * @since Mar 9, 2016
 */
public class NettyClient {

	final Logger logger = LoggerFactory.getLogger(getClass());
	private final String classname = getClass().getSimpleName();
	
	/* for client */
	@JsonProperty("alive")
	private final AtomicBoolean alive;
	@JsonProperty("connect-retrying")
	private final AtomicInteger retry;
	private final long clientExpiration;
	private final AtomicBoolean antiflapper;
	private final int maxQueueThreshold;
	private final Config config;
	
	private transient final Scheduler scheduler; 
	private transient final Agent connector;
	
	private transient EventLoopGroup clientGroup;
	private transient NettyClientHandler clientHandler;
	
	@JsonProperty("log-name")
	private String loggingName;
	private long creation;
	private long lastUsage;
	private long sentCounter;

	protected NettyClient(
			final BrokerChannel channel, 
			final Scheduler scheduler, 
			final int retryDelay,
			final int maxRetries, 
			final String loggingName, 
			final Config config) {

		this.loggingName = loggingName;
		this.config = config;
		this.scheduler = requireNonNull(scheduler);

		this.clientHandler = new NettyClientHandler(
				config.beatToMs(config.getBroker().getMaxLagBeforeDiscardingClientQueue()), 
				config.getBroker().getMaxClientQueueSize());
		this.antiflapper = new AtomicBoolean(true);
		this.alive = new AtomicBoolean();
		this.retry  = new AtomicInteger();
		this.connector = scheduler.getAgentFactory()
			.create(
				Action.BROKER_CLIENT_START, 
				PriorityLock.HIGH_ISOLATED,
				Frequency.ONCE, 
				() -> keepConnecting(channel, maxRetries, retryDelay))
			.delayed(1000l)
			.build();
		scheduler.schedule(connector);
		this.creation = System.currentTimeMillis();
		this.clientExpiration = Math.max(
				requireNonNull(config).beatToMs(config.getProctor().getPhaseFrequency()), 
				config.beatToMs(config.getFollower().getClearanceMaxAbsence() * 2));
		this.maxQueueThreshold = config.getBroker().getConnectionHandlerThreads();
	}

	protected long getCreation() {
		return creation;
	}

	@JsonProperty("queue-size")
	public int getQueueSize() {
		return this.clientHandler.queue.size();
	}
	@JsonProperty("expired")
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

	protected boolean send(final Block msg) {
		logging(msg);
		this.lastUsage = System.currentTimeMillis();
		return this.clientHandler.send(msg);
	}

	private void logging(final Block msg) {
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
		    logger.debug("{}: ({}) Sending: {}", classname, loggingName, 
		    		((MessageMetadata)msg.getMessage()).getPayloadType().getSimpleName());
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
					.setNameFormat(SchedulerSettings.THREAD_NANE_TCP_BROKER_CLIENT)
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
						//.addLast("encoder", new ObjectEncoder())
						//.addLast("decoder", new ObjectDecoder(ClassResolvers.weakCachingResolver(null)))
						.addLast("streamer", new ChunkedWriteHandler())
						.addLast("handler", clientHandler)
						.addLast(new ExceptionHandler());
				}
			});
			//bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
			bootstrap.option(ChannelOption.SO_BACKLOG, 100);
			bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
			bootstrap.option(ChannelOption.TCP_NODELAY, true);
			bootstrap.option(ChannelOption.AUTO_READ, true);
			bootstrap.option(ChannelOption.SO_REUSEADDR, true);
			this.alive.set(true);
			if (logger.isInfoEnabled()) {
				logger.info("{}: ({}) Binding to broker: {}:{} at channel: {}", classname, loggingName,
					address, port, channel.getChannel().name());
			}
			
			bootstrap.connect(addr.getAddress().getHostAddress(), addr.getPort()).sync();
			wrongDisconnection = false;
		} catch (InterruptedException ie) {
			wrongDisconnection = false;
		} catch (Exception e) {
			this.alive.set(false);
			wrongDisconnection = true;
			logger.error("{}: ({}) Unexpected while contacting shard's broker", classname, loggingName, e);
		} finally {
			if (logger.isInfoEnabled()) {
				logger.info("{}: ({}) Exiting connection scope", classname, loggingName);
			}
		}
		return wrongDisconnection;
	}

	@Sharable
	protected class NettyClientHandler extends ChannelInboundHandlerAdapter {
		private final BlockingQueue<Block> queue;
		private final long maxLagBeforeDiscardingClientQueueBeats;
		private final int maxClientQueueSize;
		
		public NettyClientHandler(
				final long maxLagBeforeDiscardingClientQueueBeats,
				final int maxClientQueueSize) {
			this.queue = new ArrayBlockingQueue<>(maxClientQueueSize, true);
			this.maxLagBeforeDiscardingClientQueueBeats = maxLagBeforeDiscardingClientQueueBeats;
			this.maxClientQueueSize = maxClientQueueSize;
		}

		protected boolean send(final Block msg) {
			boolean sent = queue.offer(msg);
			if (!sent && isLagTooHigh()) {
			    queue.clear();
			    sent = queue.offer(msg);
			}
			return sent;
		}

		private boolean isLagTooHigh() {
			final Block eldest = queue.peek();
			if ((System.currentTimeMillis() - ((MessageMetadata)eldest.getMessage()).getCreatedAt()) 
					> maxLagBeforeDiscardingClientQueueBeats || queue.size() == maxClientQueueSize) {
				logger.error("{}: ({}) Clearing queue for LAG reached LIMIT - increment client connector threads size", 
						getClass().getSimpleName(), loggingName);
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
			keepSending(ctx);
		}

		private void keepSending(final ChannelHandlerContext ctx) {
			Block block = null;
			try {
				while (!Thread.interrupted()) {
					block = queue.poll(100, TimeUnit.MILLISECONDS);
					if (block != null) {
						if (logger.isDebugEnabled()) {
							logger.debug("{}: ({}) Writing: {}", classname, loggingName, 
									((MessageMetadata)block.getMessage()).getPayloadType());
						}
						ctx.writeAndFlush(new ChunkedStream(CustomCoder.Encoder.encode(block)));
					} else {
						logger.error("{}: ({}) Waiting for messages to be enqueued: {}", classname,
								loggingName);
					}
				}
			} catch (Exception e) {
				logger.error("{}: ({}) interrupted while waiting for Socketclient blocking queue gets offered",
						classname, loggingName, e);
			}
		}
		
		@Override
		public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {			
			ChunkedStream c = (ChunkedStream)msg;
			final ByteBuf bb = c.readChunk(ctx);
			final byte[] read = new byte[bb.readableBytes()];
			bb.readBytes(read);
			ctx.write(read);
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
			clientGroup.shutdownGracefully(
					config.beatToMs(config.getBroker().getShutdownQuiet()), 
					config.beatToMs(config.getBroker().getShutdownTimeout()), 
					TimeUnit.MILLISECONDS);
		} else {
			logger.error("{}: ({}) Invalid state to close client: {}", classname, loggingName,
					clientGroup == null ? "not started" : "already shut-down");
		}
	}

	public long getClientExpiration() {
		return clientExpiration;
	}
	public AtomicBoolean getAlive() {
		return alive;
	}
	public long getLastUsage() {
		return lastUsage;
	}
	public AtomicInteger getRetry() {
		return retry;
	}
	public long getSentCounter() {
		return sentCounter;
	}
	
}
