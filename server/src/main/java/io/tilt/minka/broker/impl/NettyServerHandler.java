package io.tilt.minka.broker.impl;

import static java.util.Collections.synchronizedMap;
import static java.util.concurrent.CompletableFuture.runAsync;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledUnsafeDirectByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.tilt.minka.broker.CustomCoder;
import io.tilt.minka.broker.CustomCoder.Block;
import io.tilt.minka.broker.CustomCoder.Decoder;
import io.tilt.minka.core.task.Scheduler;
import io.tilt.minka.core.task.Scheduler.Frequency;
import io.tilt.minka.core.task.Scheduler.PriorityLock;
import io.tilt.minka.core.task.Semaphore.Action;
import io.tilt.minka.spectator.MessageMetadata;

/**
 * Sharable makes it able to use the same handler for concurrent clients
 */ 
public class NettyServerHandler extends ChannelInboundHandlerAdapter {
	
	private final Logger logger = LoggerFactory.getLogger(getClass());
	protected final Map<String, Map<String, Integer>> countByTypeAndHost = synchronizedMap(new LinkedHashMap<>());
	private final Decoder deco;
	private final Consumer<Block> consumer;
	private final String tag;
	private final Scheduler scheduler;
	
	public NettyServerHandler(final Consumer<Block> consumer, final Scheduler scheduler, final String loggingName) {
		super();
		this.deco = new CustomCoder.Decoder((s,is)-> runAsync(()->consumer.accept(new Block(s, is))));
		this.consumer = consumer;
		this.tag = loggingName;
		this.scheduler = scheduler; 
	}
	
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) {
		try {
			if (msg == null) {
				logger.error("{} ({}): incoming message came NULL", getClass().getSimpleName(), tag);
				return;
			}
			final UnpooledUnsafeDirectByteBuf uudbb = (UnpooledUnsafeDirectByteBuf)msg;
			if (logger.isDebugEnabled()) {
				logger.debug("{} ({}) Received {} bytes", getClass().getSimpleName(), tag, uudbb.readableBytes());
			}
			final ByteBuf buffer = Unpooled.buffer(uudbb.readableBytes());
			uudbb.readBytes(buffer);
			deco.decode(buffer.array());
			buffer.release();
		} catch (Exception e) {
			logger.error("{}: ({}) Unexpected while reading incoming message", getClass().getSimpleName(), tag, e);
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
	
	private void test(final Block block) throws IOException {
		final MessageMetadata msg = (MessageMetadata) block.getMessage();
		int read = 0;
		final ByteArrayOutputStream baos = new ByteArrayOutputStream();  
		while ((read =block.getStream().read())>-1) baos.write(read);
		logger.info("stream: {}", baos.toByteArray());
	}
	
	@Override
	public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
		super.channelUnregistered(ctx);
	}
	@Override
	public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable e) {
		if (logger.isDebugEnabled()) {
			logger.debug("({}) {} ChannelInboundHandlerAdapter: Unexpected while consuming (who else's using broker port ??)", 
				getClass().getSimpleName(), tag, e.getMessage());
		}
		ctx.close();
	}
}