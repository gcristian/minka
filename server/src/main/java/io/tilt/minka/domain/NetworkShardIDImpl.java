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
package io.tilt.minka.domain;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.util.Enumeration;
import java.util.Random;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.business.follower.Follower;

/**
 * Identity of a shard (followers and leader alike)
 * Created by the {@linkplain Follower}
 * 
 * @author Cristian Gonzalez
 * @since Dec 3, 2015
 *
 */
public class NetworkShardIDImpl implements NetworkShardID {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    
    private static final long serialVersionUID = 3233785408081305735L;
    private static final Random random = new Random();
    
	private String id;
	private final InetAddress sourceHost;
	private int port;
	private final DateTime creation;
		
	public NetworkShardIDImpl(final Config config) throws IOException {
	    this.creation = new DateTime(DateTimeZone.UTC);
	    this.port = config.getBrokerServerPort();
		this.sourceHost = findSiteAddress(config);
		config.setResolvedShardId(this);
		testPort();
		buildId(config);
	}

    private void testPort() {
        ServerSocket socket = null; 
        try {
            socket = new ServerSocket(port);
            logger.info("{}: Testing host {} port {} OK", getClass().getSimpleName(), sourceHost, port);            
        } catch (IOException e) {
            throw new IllegalArgumentException("Testing port cannot be opened: " + port, e);
        } finally {
            if (socket!=null && !socket.isBound()) {
                throw new IllegalArgumentException("Testing port cannot be opened: " + port);
            } else if (socket!=null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    throw new IllegalArgumentException("Testing port cannot be tested: " + port);
                }
            }
        }
    }

    private void buildId(final Config config) {
        String id = null;
		if (sourceHost!=null) {
		    if (!sourceHost.getHostName().isEmpty()) {
		        id = sourceHost.getHostName();
		        final String suffix = config.getFollowerShardIdSuffix();
		        if (suffix!=null && !suffix.isEmpty()) {
		            id+="-" + suffix;
		        }
		    } else {
		        id = sourceHost.getHostAddress();
		    }
		} else {
		    logger.warn("{}: Falling back with just a random shard ID", getClass().getSimpleName());
			id = "-" + random.nextInt(9999);
		}		
		this.id = id + ":" + port;
    }
	
    private InetAddress findSiteAddress(final Config config) {
        InetAddress fallback = null;
        try {
            final String niName = config.getFollowerUseNetworkInterfase();
            logger.info("{}: Looking for configured network interfase: {}", getClass().getSimpleName(), niName);
            final Enumeration<NetworkInterface> nis = NetworkInterface.getNetworkInterfaces();
            while (nis.hasMoreElements()) {
                final NetworkInterface ni = nis.nextElement();
                if (niName.contains(ni.getName())) {
                    logger.info("{}: Interfase found: {}, looking for a Site address..", getClass().getSimpleName(), ni.getName());
                    final Enumeration<InetAddress> ias = ni.getInetAddresses();
                    while (ias.hasMoreElements()) {
                        final InetAddress ia = ias.nextElement();
                        if (ia.isSiteLocalAddress()) {
                            logger.info("{}: Host site address found: {}:{} with HostName {}", getClass().getSimpleName(), 
                                    ia.getHostAddress(), config.getBrokerServerPort(),ia.getHostName());
                            return ia;
                        } else {
                            fallback = ia;
                            logger.warn("{}: Specified interfase: {} is not a site address!!, "
                                    + "you should specify a non local-only valid interfase !! where's your lan?", 
                                getClass().getSimpleName(), ia.getHostAddress());
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("{}: Cannot build shard id value with hostname", getClass().getSimpleName(), e);
        }
        logger.error("{}: Site network interfase not found !", getClass().getSimpleName());
        return fallback;
    }
	
    @Override
	public int getInetPort() {
        return this.port;
    }

	@Override
    public InetAddress getInetAddress() {
		return this.sourceHost;
	}
	
	public int hashCode() {
        return new HashCodeBuilder()
            .append(getStringID())
            .toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj!=null && obj instanceof NetworkShardIDImpl) {
            NetworkShardIDImpl other = (NetworkShardIDImpl)obj;
            return other.getStringID().equals(getStringID());
        } else {
            return false;
        }
    }
	
    @Override
    public DateTime getCreation() {
        return this.creation;
    }

	@Override
	public String toString() {
	    return id;
	}

    @Override
    public String getStringID() {
        return this.id;
    }
}
