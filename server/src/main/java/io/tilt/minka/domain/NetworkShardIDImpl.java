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
package io.tilt.minka.domain;

import static io.tilt.minka.core.Journal.StoryBuilder.compose;

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
import io.tilt.minka.core.Journal;
import io.tilt.minka.core.Journal.Case;
import io.tilt.minka.core.Journal.Fact;
import io.tilt.minka.core.Journal.Result;
import io.tilt.minka.core.Journal.StoryBuilder;
import io.tilt.minka.core.follower.Follower;

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
	
//	private Journal journal;

	//public NetworkShardIDImpl(final Config config, final Journal journal) throws IOException {
	public NetworkShardIDImpl(final Config config) throws IOException {
		this.creation = new DateTime(DateTimeZone.UTC);
		this.port = Integer.parseInt(config.getBrokerServerHost().split(":")[1]);
		this.sourceHost = findLANAddress(config.getBrokerServerHost().split(":")[0], 
				config.getFollowerUseNetworkInterfase());
		//this.journal = journal;
		config.setResolvedShardId(this);
		testPort();
		buildId(config);
	}

	private void testPort() {
		ServerSocket socket = null;
		try {
			socket = new ServerSocket(port);
			logger.info("{}: Testing host {} port {} OK", getClass().getSimpleName(), sourceHost, port);
			//journal.commit(StoryBuilder.compose(this.getClass(), Fact.shard_finding_address).with(Case.FINAL).build());
		} catch (IOException e) {
			//journal.commit(StoryBuilder.compose(this.getClass(), Fact.shard_finding_address).with(Case.ISSUED)
				//	.with("").build());
			throw new IllegalArgumentException("Testing port cannot be opened: " + port, e);
		} finally {
			if (socket != null && !socket.isBound()) {
				throw new IllegalArgumentException("Testing port cannot be opened: " + port);
			} else if (socket != null) {
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
		if (sourceHost != null) {
			if (!sourceHost.getHostName().isEmpty()) {
				id = sourceHost.getHostName();
				final String suffix = config.getFollowerShardIdSuffix();
				if (suffix != null && !suffix.isEmpty()) {
					id += "-" + suffix;
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

	/** trying to find a LAN candidate address */
	private InetAddress findLANAddress(final String specifiedAddress, final String specifiedInterfase) {
		InetAddress fallback = null;
		boolean specified;
		try {
			logger.info("{}: Looking for configured network interfase/address: {}/{}", 
					getClass().getSimpleName(), specifiedInterfase, specifiedAddress);
			final Enumeration<NetworkInterface> nis = NetworkInterface.getNetworkInterfaces();
			while (nis.hasMoreElements()) {
				final NetworkInterface ni = nis.nextElement();
				if (specified = specifiedInterfase.contains(ni.getName())) {
					if (ni.isLoopback()) {
						logger.warn("{}: Loopback address not recomended !!", getClass().getSimpleName());
					}
					logger.info("{}: Specified Interfase found: {}", getClass().getSimpleName(), ni.getName());
				}
				final Enumeration<InetAddress> ias = ni.getInetAddresses();
				while (ias.hasMoreElements()) {
					final InetAddress ia = ias.nextElement();
					if (ia.getHostName().equals(specifiedAddress)) {
						specified&=true;
						logger.info("{}: Specified Host address found: {}:{} with Hostname {}", 
								getClass().getSimpleName(), ia.getHostAddress(),  ia.getHostName());
					}
					if (ia.isSiteLocalAddress()) {
						fallback = fallback == null ? ia : fallback;
						if (specified) {
							return ia;
						}
					} else if (specified) {
						logger.warn("{}: Specified Address: {} is not LAN candidate, "
									+ "you should specify a non local-only valid interfase and address.",
							getClass().getSimpleName(), ia.getHostAddress());
					}
				}
			}
		} catch (Exception e) {
			//journal.commit(compose(getClass(), Fact.shard_finding_address).with(Result.FAILURE).with(e).build());
			logger.error("{}: Cannot build shard id value with hostname", getClass().getSimpleName(), e);
		}
		if (fallback !=null) {
			logger.warn("{}: Using found fallback: {}!", getClass().getSimpleName(), fallback);
			return fallback;
		} else {
			throw new IllegalArgumentException("None valid Inet address found (Specified: " + specifiedAddress + ")");
		}
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
		return new HashCodeBuilder().append(getStringID()).toHashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj != null && obj instanceof NetworkShardIDImpl) {
			NetworkShardIDImpl other = (NetworkShardIDImpl) obj;
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
