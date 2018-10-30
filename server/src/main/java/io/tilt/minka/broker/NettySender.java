package io.tilt.minka.broker;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelHandler.Sharable;
import io.tilt.minka.spectator.MessageMetadata;

@Sharable 
class NettySender extends ChannelInboundHandlerAdapter {

	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final BlockingQueue<MessageMetadata> queue;
	private final long maxLagBeforeDiscardingClientQueueBeats;
	private final int maxClientQueueSize;
	private final String tag;
	
	NettySender(
			final long maxLagBeforeDiscardingClientQueueBeats,
			final int maxClientQueueSize, 
			final String tag) {
		this.queue = new ArrayBlockingQueue<>(maxClientQueueSize, true);
		this.maxLagBeforeDiscardingClientQueueBeats = maxLagBeforeDiscardingClientQueueBeats;
		this.maxClientQueueSize = maxClientQueueSize;
		this.tag = tag;
	}

	boolean send(final MessageMetadata msg) {
		boolean sent = queue.offer(msg);
		if (!sent && isLagTooHigh()) {
		    queue.clear();
		    sent = queue.offer(msg);
		}
		return sent;
	}

	private boolean isLagTooHigh() {
		final MessageMetadata eldest = queue.peek();
		if ((System.currentTimeMillis() - eldest.getCreatedAt()) > maxLagBeforeDiscardingClientQueueBeats 
				|| queue.size() == maxClientQueueSize) {
			logger.error("{}: ({}) Clearing queue for LAG reached LIMIT - increment client connector threads size", 
					getClass().getSimpleName(), tag);
			return true;
		} else {
			return false;
		}
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		logger.warn("{}: ({}) channel inactivated", getClass().getSimpleName(), tag);
		super.channelInactive(ctx);
	}
	@Override
	public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
		logger.warn("{}: ({}) channel unregistered", getClass().getSimpleName(), tag);
		super.channelUnregistered(ctx);
	}
	@Override
	public void channelActive(final ChannelHandlerContext ctx) {
		MessageMetadata msg = null;
		try {
			while (!Thread.interrupted()) {
				msg = queue.take();
				if (msg != null) {
					if (logger.isInfoEnabled()) {
						logger.info("{}: ({}) Writing: {} ({})", getClass().getSimpleName(), tag, 
							msg.getPayloadType(), msg.getPayload().hashCode());
					}
					ctx.writeAndFlush(msg);
				} else {
					logger.error("{}: ({}) Waiting for messages to be enqueued: {}", getClass().getSimpleName(), tag);
				}
			}
		} catch (InterruptedException e) {
			logger.error("{}: ({}) interrupted while waiting for Socketclient blocking queue gets offered",
					getClass().getSimpleName(), tag, e);
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
				getClass().getSimpleName(), tag, cause);
		ctx.close();
	}

	int getQueueSize() {
		return queue.size();
	}

}