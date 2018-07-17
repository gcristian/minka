package io.tilt.minka.broker.impl;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.handler.stream.ChunkedStream;
import io.tilt.minka.broker.CustomCoder;
import io.tilt.minka.broker.CustomCoder.Block;
import io.tilt.minka.spectator.MessageMetadata;

 
class NettyClientHandler extends ChannelInboundHandlerAdapter {
	
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final String classname = getClass().getSimpleName();
	private final String tag;

	protected final BlockingQueue<Block> queue;
	private final long maxLagBeforeDiscardingClientQueueBeats;
	private final int maxClientQueueSize;
	
	public NettyClientHandler(
			final long maxLagBeforeDiscardingClientQueueBeats,
			final int maxClientQueueSize,
			final String tag) {
		this.queue = new ArrayBlockingQueue<>(maxClientQueueSize, true);
		this.maxLagBeforeDiscardingClientQueueBeats = maxLagBeforeDiscardingClientQueueBeats;
		this.maxClientQueueSize = maxClientQueueSize;
		this.tag = tag;
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
					getClass().getSimpleName(), tag);
			return true;
		} else {
			return false;
		}
	}

	protected int size() {
		return queue.size();
	}
	
	private void keepSending(final ChannelHandlerContext ctx) {
		Block block = null;
		try {
			while (!Thread.interrupted()) {
				block = queue.poll(100, TimeUnit.MILLISECONDS);
				if (block != null) {
					if (logger.isDebugEnabled()) {
						logger.debug("{}: ({}) Writing: {}", classname, tag, 
								((MessageMetadata)block.getMessage()).getPayloadType());
					}
					ctx.writeAndFlush(new ChunkedStream(CustomCoder.Encoder.encode(block)));
				} else {
					//logger.error("{}: ({}) Waiting for messages to be enqueued: {}", classname, tag);
				}
			}
		} catch (Exception e) {
			logger.error("{}: ({}) interrupted while waiting for Socketclient blocking queue gets offered", classname, tag, e);
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
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		logger.warn("{}: ({}) channel inactivated", classname, tag);
		super.channelInactive(ctx);
	}
	@Override
	public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
		logger.warn("{}: ({}) channel unregistered", classname, tag);
		super.channelUnregistered(ctx);
	}
	@Override
	public void channelActive(final ChannelHandlerContext ctx) {
		keepSending(ctx);
	}
	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) {
		ctx.flush();
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		logger.error("{}: ({}) ChannelInboundHandlerAdapter: Unexpected ", classname, tag, cause);
		ctx.close();
	}

}