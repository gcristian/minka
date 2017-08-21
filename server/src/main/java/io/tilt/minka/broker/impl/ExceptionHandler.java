/**
 *  Copyright (c) 2011-2015 Zauber S.A.  -- All rights reserved
 */

package io.tilt.minka.broker.impl;

import java.net.SocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

public class ExceptionHandler extends ChannelDuplexHandler {
   
    private final Logger logger = LoggerFactory.getLogger(getClass());
    
    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
        logger.error("{}: Uncaught ", getClass().getSimpleName(), cause);  
    }
    
    @Override
    public void connect(
            final ChannelHandlerContext ctx, 
            final SocketAddress remoteAddress, 
            final SocketAddress localAddress, 
            final ChannelPromise promise) {
        
        ctx.connect(remoteAddress, 
                localAddress, 
                promise.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) {
                if (!future.isSuccess()) {
                    logger.error("{}: Unsuccessful from {} to {} ", 
                            getClass().getSimpleName(), 
                            remoteAddress, localAddress);
                }
            }
        }));
    }
    @Override
    public void write(
            final ChannelHandlerContext ctx, 
            final Object msg, 
            final ChannelPromise promise) {
        
        ctx.write(msg, promise.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) {
                if (!future.isSuccess()) {
                    logger.error("{}: Unsuccessful: {} ", getClass().getSimpleName(), msg);
                }
            }
        }));
    }
    
}