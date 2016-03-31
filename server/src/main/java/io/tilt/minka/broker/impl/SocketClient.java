/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.tilt.minka.broker.impl;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

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
import io.tilt.minka.business.Coordinator;
import io.tilt.minka.business.Coordinator.Frequency;
import io.tilt.minka.business.Coordinator.PriorityLock;
import io.tilt.minka.business.Coordinator.SynchronizedAgentFactory;
import io.tilt.minka.business.Semaphore.Action;
import io.tilt.minka.spectator.MessageMetadata;

/**
 * Creates and persist connections to other servers, forwarding data to them 
 * 
 * @author Cristian Gonzalez
 * @since Mar 9, 2016
 */
public class SocketClient {
    
    private final Logger logger = LoggerFactory.getLogger(getClass());
    
    /* for client */
    private EventLoopGroup clientGroup;
    private SocketClientHandler clientHandler;
    private boolean alive;
    private int retry;
    private String loggingName;
    
    private long creation;
    private long lastUsage;
    private final long clientExpiration;
    
    protected SocketClient(
            final BrokerChannel channel, 
            final Coordinator coordinator, 
            final int retryDelay, 
            final int maxRetries, 
            final String loggingName, 
            final Config config) {
        
        this.loggingName = loggingName;
        this.clientHandler = new SocketClientHandler();
        coordinator.schedule(SynchronizedAgentFactory.build(Action.BROKER_CLIENT_START, 
                PriorityLock.HIGH_ISOLATED, Frequency.ONCE, 
                ()->keepConnectedWithRetries(channel, maxRetries, retryDelay)));
        this.creation = System.currentTimeMillis();
        this.clientExpiration = Math.max(config.getShepherdDelayMs(), config.getFollowerClearanceMaxAbsenceMs());
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
                logger.warn("{}: ({}) expired ! {} old (for max is: {})", getClass().getSimpleName(), loggingName, 
                        elapsed, this.clientExpiration);
                return true;
            } else {
                return false;
            }
        }
    }
    
    protected boolean send(final MessageMetadata msg) {
        if (!alive) {
            logger.warn("{}: ({}) Not Ready to send messages (enqueuing)", 
                    getClass().getSimpleName(), loggingName);
        }
        this.lastUsage = System.currentTimeMillis();
        return this.clientHandler.send(msg);
    }
    
    /* a client for a shard's broker to send any type of messages, whether be follower or leader */
    private void keepConnectedWithRetries(final BrokerChannel channel, final int maxRetries, final int retryDelay) {
        clientGroup = new NioEventLoopGroup(1, 
                new ThreadFactoryBuilder().setNameFormat(Config.THREAD_NANE_TCP_BROKER_CLIENT).build());

        boolean wronglyDisconnected = true;
        while (retry < maxRetries && wronglyDisconnected) {
            if (retry++>0) {
                try {
                    Thread.sleep(retryDelay);
                } catch (InterruptedException e) {
                    logger.error("{}: ({}) Unexpected while waiting for next client connection retry", 
                            getClass().getSimpleName(), loggingName, e);
                }
            }
            wronglyDisconnected = keepConnected(channel);
        }
    }

    private boolean keepConnected(final BrokerChannel channel) {
        boolean wrongDisconnection;
        try {
            final String address = channel.getAddress().getInetAddress().getHostAddress();
            final int port = channel.getAddress().getInetPort();
            logger.info("{}: ({}) Building client (retry:{}) for outbound messages to: {}", 
                    getClass().getSimpleName(), loggingName, retry, channel.getAddress());
            final Bootstrap b = new Bootstrap();
            b.group(clientGroup)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline().addLast(
                        new ObjectEncoder(), 
                        new ObjectDecoder(ClassResolvers.cacheDisabled(null)), 
                        clientHandler); 
                }
            });
            this.alive = true;
            logger.info("{}: ({}) Binding to broker: {}:{} at channel: {}", getClass().getSimpleName(), loggingName, 
                    address, port, channel.getChannel().getName());
            b.connect(channel.getAddress().getInetAddress().getHostAddress(), channel.getAddress().getInetPort())
                .sync().channel().closeFuture().sync();
            wrongDisconnection = false;
        } catch (InterruptedException ie) {
            wrongDisconnection = false;
        } catch (Exception e) {
            wrongDisconnection = true;
            logger.error("{}: ({}) Unexpected while contacting shard's broker", getClass().getSimpleName(), 
                    loggingName, e);
        } finally {
            logger.info("{}: ({}) Exiting client writing scope", getClass().getSimpleName(), loggingName);
        }
        return wrongDisconnection;
    }
    
    @Sharable
    protected class SocketClientHandler extends ChannelInboundHandlerAdapter {
        private BlockingQueue<MessageMetadata> queue;
        
        public SocketClientHandler() {
            this.queue = new ArrayBlockingQueue<>(100, true);
        }
        protected boolean send(final MessageMetadata msg) {
            return queue.offer(msg);
        }
        @Override
        public void channelActive(final ChannelHandlerContext ctx) {
            onActiveChannel(ctx);
        }
        private void onActiveChannel(final ChannelHandlerContext ctx) {
            MessageMetadata msg = null;
            try {
                while (!Thread.interrupted()) {
                    msg = queue.take();
                    if (msg!=null) {
                        logger.debug("{}: ({}) Writing: {} ('{}') ({} bytes)", getClass().getSimpleName(), 
                                loggingName, msg.getPayloadType().getSimpleName(), msg.getInbox());
                        ctx.writeAndFlush(new MessageMetadata(msg.getPayload(), msg.getInbox()));
                    } else {
                        logger.error("{}: ({}) Waiting for messages to be enqueued: {}", 
                                getClass().getSimpleName(),loggingName);
                    }
                }
            } catch (InterruptedException e) {
                logger.error("{}: ({}) interrupted while waiting for Socketclient blocking queue gets offered", 
                        getClass().getSimpleName(), loggingName, e);
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
            logger.error("{}: ({}) Unexpected while posting message payload", 
                    getClass().getSimpleName(), loggingName, cause);
            ctx.close();
        }
        
    }
    
    public void close() {
        if (clientGroup!=null && !clientGroup.isShuttingDown()) {
            logger.info("{}: ({}) Closing connection to server", getClass().getSimpleName(), loggingName);
            this.alive = false;
            clientGroup.shutdownGracefully();
        } else {
            logger.error("{}: ({}) Invalid state to close client: {}", getClass().getSimpleName(), loggingName, 
                    clientGroup == null ? "not started":"already shut-down");
        }
    }

}
