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

import static java.util.Collections.synchronizedMap;
import static java.util.concurrent.CompletableFuture.runAsync;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledUnsafeDirectByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.tilt.minka.api.config.SchedulerSettings;
import io.tilt.minka.broker.impl.CustomCoder.Block;
import io.tilt.minka.broker.impl.CustomCoder.Decoder;
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
public class NettyServer {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	/* for server */
	private NettyServerHandler serverHandler;
	private EventLoopGroup serverWorkerGroup;
	private final Consumer<Block> consumer;

	private final int connectionHandlerThreads;
	private final int serverPort;
	private final String serverAddress;
	private final String networkInterfase;
	private final Scheduler scheduler;
	private final Agent agent;
	
	private int retry;
	private final AtomicLong count;
	private final String loggingName;
	private final String classname = getClass().getSimpleName();
	
	private ChannelFuture channelFuture;
	

	protected NettyServer(
			final Consumer<Block> consumer, 
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
		
		
		this.consumer = consumer;
		this.connectionHandlerThreads = connectionHandlerThreads;
		this.serverPort = serverPort;
		this.serverAddress = serverAddress;
		this.networkInterfase = networkInterfase;
		this.scheduler = scheduler;
		this.count = new AtomicLong();
		this.loggingName = loggingName;
		this.serverHandler = new NettyServerHandler();
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
		
		this.serverWorkerGroup = new NioEventLoopGroup(
				this.connectionHandlerThreads,
				new ThreadFactoryBuilder()
					.setNameFormat(SchedulerSettings.THREAD_NAME_BROKER_SERVER_WORKER)
					.build());

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
						    //.addLast("encoder", new ObjectEncoder())
						    //.addLast("decoder", new ObjectDecoder(ClassResolvers.weakCachingResolver(null)))
						    .addLast("handler", serverHandler)
						    .addLast("streamer", new ChunkedWriteHandler())
						    .addLast(new ExceptionHandler())
						    ;
					}
				});			
			server.childOption(ChannelOption.SO_BACKLOG, 100);
			server.childOption(ChannelOption.SO_KEEPALIVE, true);
			server.childOption(ChannelOption.TCP_NODELAY, true);
			server.childOption(ChannelOption.AUTO_READ, true);
			server.childOption(ChannelOption.SO_REUSEADDR, true);
			//server.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
			
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

	@Sharable
	/**
	 * Sharable makes it able to use the same handler for concurrent clients
	 */
	protected class NettyServerHandler extends ChannelInboundHandlerAdapter {

		public final Map<String, Map<String, Integer>> countByTypeAndHost = synchronizedMap(new LinkedHashMap<>());
		private final Decoder deco;
		
		@Override
		public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
			super.channelUnregistered(ctx);
		}
		
		public NettyServerHandler() {
			super();
			deco = new Decoder((obj, is)-> runAsync(()->consumer.accept(new Block(obj, is))));
		}

		private void test(final Block block) throws IOException {
			//////////////////////////////
			final MessageMetadata msg = (MessageMetadata) block.getMessage();
			int read = 0;
			final ByteArrayOutputStream baos = new ByteArrayOutputStream();  
			while ((read =block.getStream().read())>-1) baos.write(read);
			logger.info("stream: {}", baos.toByteArray());
			//////////////////////////////
		}
		
		@Override
		public void channelRead(ChannelHandlerContext ctx, Object msg) {
			
			try {
				if (msg == null) {
					logger.error("({}) SocketServerHandler: incoming message came NULL", loggingName);
					return;
				}
				final UnpooledUnsafeDirectByteBuf uudbb = (UnpooledUnsafeDirectByteBuf)msg;
				logger.info("received buffer: {} bytes", uudbb.readableBytes());
				final ByteBuf buffer = Unpooled.buffer(uudbb.readableBytes());
				uudbb.readBytes(buffer);
				deco.decode(buffer.array());
				buffer.release();
			} catch (Exception e) {
				logger.error("({}) SocketServerHandler: Unexpected while reading incoming message", loggingName, e);
			}
		}

		private void notifyListener(final Block block) {
			scheduler.schedule(scheduler.getAgentFactory()
					.create(
						Action.BROKER_INCOMING_MESSAGE, 
						PriorityLock.HIGH_ISOLATED, 
						Frequency.ONCE, 
						() -> consumer.accept(block))
					.build());
		}

		private void addMetric(MessageMetadata meta) {
			final String origin = meta.getOriginConnectAddress();
			Map<String, Integer> typesByHost = countByTypeAndHost.get(origin);
			if (typesByHost==null) {
				countByTypeAndHost.put(origin, typesByHost = new LinkedHashMap<>());
			}
			final String type = meta.getPayloadType().getSimpleName();
			final Integer c = typesByHost.get(type);
			typesByHost.put(type, c==null ? new Integer(1) : new Integer(c+1));
		}
		
		@Override
		public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable e) {
			if (logger.isDebugEnabled()) {
				logger.debug("({}) ChannelInboundHandlerAdapter: Unexpected while consuming (who else's using broker port ??)", 
					loggingName, e.getMessage());
			}
			ctx.close();
		}
	}
	
	public Map<String, Map<String, Integer>> getCountByTypeAndHost() {
		return this.serverHandler.countByTypeAndHost;
	}
	
	public AtomicLong getCount() {
		return count;
	}
	public int getRetry() {
		return retry;
	}

}
