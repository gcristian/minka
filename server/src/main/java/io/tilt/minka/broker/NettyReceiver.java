package io.tilt.minka.broker;

import static java.util.Collections.synchronizedMap;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.tilt.minka.core.task.Scheduler;
import io.tilt.minka.core.task.Scheduler.Frequency;
import io.tilt.minka.core.task.Scheduler.PriorityLock;
import io.tilt.minka.core.task.Semaphore.Action;
import io.tilt.minka.spectator.MessageMetadata;

@Sharable
/**
 * Sharable makes it able to use the same handler for concurrent clients
 */
class NettyReceiver extends ChannelInboundHandlerAdapter {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	public final Map<String, Map<String, Integer>> countByTypeAndHost = synchronizedMap(new LinkedHashMap<>());
	
	private final String tag;
	private final Consumer<MessageMetadata> consumer;
	
	NettyReceiver(final String tag, final Consumer<MessageMetadata> consumer) {
		this.tag = tag;
		this.consumer = consumer;
	}
	@Override
	public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
		super.channelUnregistered(ctx);
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) {
		try {
			if (msg == null) {
				logger.error("({}) NettyReceiver: incoming message came NULL", tag);
				return;
			}
			MessageMetadata meta = (MessageMetadata) msg;
			if (logger.isDebugEnabled()) {
			    logger.debug("{}: ({}) Reading: {}", getClass().getSimpleName(), tag, meta.getPayloadType());
			}

			consumer.accept(meta);
			addMetric(meta);
		} catch (Exception e) {
			logger.error("({}) NettyReceiver: Unexpected while reading incoming message", tag, e);
		}
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
				tag, e.getMessage());
		}
		ctx.close();
	}
}