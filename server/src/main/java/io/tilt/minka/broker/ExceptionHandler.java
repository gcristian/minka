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