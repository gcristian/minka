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

import static java.util.Objects.requireNonNull;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.util.Enumeration;
import java.util.Random;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang3.Validate;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.tilt.minka.api.Config;
import io.tilt.minka.core.follower.Follower;

/**
 * Identity of a shard (followers and leader alike)
 * Created by the {@linkplain Follower}
 * 
 * @author Cristian Gonzalez
 * @since Dec 3, 2015
 *
 */
public class TCPShardIdentifier implements NetworkShardIdentifier, Closeable {

	@JsonIgnore
	private final Logger logger = LoggerFactory.getLogger(getClass());

	private static final long serialVersionUID = 3233785408081305735L;
	private static final Random random = new Random();
	private static final int PORT_SEARCHES_MAX = 100;
	private final String logName = getClass().getSimpleName();
	// this doesnt go to leader
	private transient final DependencyPlaceholder dependencyPlaceholder;

	@JsonProperty(index=1, value="id")
	private String id;
	@JsonProperty(index=2, value="server-hostname")
	private final InetAddress sourceHost;
	@JsonProperty(index=3, value="server-port")
	private int port;
	@JsonIgnore
	private final int configuredPort;  
	@JsonIgnore
	private final DateTime creation;
	@JsonProperty(index = 5, value = "tag")
	private String tag;
	@JsonIgnore
	private transient ServerSocket bookedSocket;
	@JsonProperty(index = 4, value = "web-host-port")
	private String webhostport;

	public TCPShardIdentifier(
			final Config config,
			final DependencyPlaceholder dependencyPlaceholder) throws Exception {
		this.dependencyPlaceholder = requireNonNull(dependencyPlaceholder);
		final String hostStr = config.getBroker().getHostPort();
		final String[] brokerStr = hostStr.split(":");
		Validate.isTrue(brokerStr.length>1, new StringBuilder("Bad broker host format: ")
					.append(hostStr)
					.append("([hostname]:[port])")
					.toString());
		this.creation = new DateTime(DateTimeZone.UTC);
		this.configuredPort = Integer.parseInt(brokerStr[1]);
		this.port = configuredPort;
		this.sourceHost = findLANAddress(brokerStr[0], config.getBroker().getNetworkInterfase());
		config.setResolvedShardId(this);
		take(config.getBroker().isEnablePortFallback());
		buildId(config);
	}

	@Override
	public void setTag(final String tag) {
		this.tag = tag;
	}
	@Override
	public String getTag() {
		if (tag == null 
				&& dependencyPlaceholder!=null 
				&& dependencyPlaceholder.getDelegate() != null 
				&& dependencyPlaceholder.getDelegate() instanceof ConsumerDelegate) {
			tag = ((ConsumerDelegate)dependencyPlaceholder.getDelegate()).getLocationTag();
		}
		return tag;
	}

	/*  with a best effort for helping multi-tenancy and noisy infra people */
	@Override
	public void take(final boolean findAnyPort) throws Exception {
		Exception cause = null;
		for (int search = 0; search < (findAnyPort ? PORT_SEARCHES_MAX : 1); this.port++, search++) {
			if (logger.isInfoEnabled()) {
				logger.info("{}: {} port {}:{} (search no.{}/{}) ", logName,
					search == 0 ? "Validating" : "Falling back", sourceHost, this.port, search, PORT_SEARCHES_MAX);
			}
			
			try {
				if (logger.isInfoEnabled()) {
					logger.info("{}: Booking port {}", logName, this.port);
				}
				bookedSocket = bookAPort(this.port);
				if (bookedSocket != null) {
					return;
				}
			} catch (Exception e) {
				if (search == 0 && !!findAnyPort || findAnyPort) {
					cause = e;
				}
			}
		}
		this.port = configuredPort; // just going back
		String fallbackFailed = "Fallbacks failed - try configuring a valid open port";
		String configFailed = "To avoid boot-up failure enable configuration parameter: brokerServerPortFallback = true";
		final Exception excp = new IllegalArgumentException(findAnyPort ? fallbackFailed : configFailed, cause);
		logger.error("{}: No open port Available ! {}", logName, excp);
		throw excp;
	}

	private ServerSocket bookAPort(final int testPort) {
		ServerSocket socket = null;
		try {
			socket = new ServerSocket(testPort);
			if (!socket.isBound()) {
				return null;
			}
			logger.debug("{}: Testing host {} port {} OK", logName, sourceHost, testPort);
			return socket;
		} catch (IOException e) {
			throw new IllegalArgumentException("Testing port cannot be opened: " + testPort, e);
		}
	}

	@Override
	public void release() {
		try {
			if (bookedSocket != null && !bookedSocket.isClosed()) {
				bookedSocket.close();
			}
		} catch (IOException e) {
			throw new IllegalArgumentException("Testing port cannot be tested: ", e);
		}
	}

	private void buildId(final Config config) {
		String id = null;
		if (sourceHost != null) {
			if (!sourceHost.getHostName().isEmpty() && config.getBroker().isUseMachineHostname()) {
				id = sourceHost.getHostName();
				if (logger.isInfoEnabled()) {
					logger.info("{}: Using system's hostname enabled by config: {}", logName, id);	
				}
				final String suffix = config.getBroker().getShardIdSuffix();
				if (suffix != null && !suffix.isEmpty()) {
					id += "-" + suffix;
				}
			} else {
				id = sourceHost.getHostAddress();
			}
		} else {
			logger.warn("{}: Falling back with just a random shard ID", logName);
			id = "-" + random.nextInt(9999);
		}
		this.id = id + ":" + port;
		config.getBroker().setHostPort(this.id);
	}

	/** trying to find a LAN candidate address */
	private InetAddress findLANAddress(final String specifiedAddress, final String specifiedInterfase) {
		InetAddress fallback = null;
		boolean specified;
		try {
			if (logger.isInfoEnabled()) {
				logger.info("{}: Looking for configured network interfase/address: {}/{}", logName,
					specifiedInterfase, specifiedAddress);
			}
			final Enumeration<NetworkInterface> nis = NetworkInterface.getNetworkInterfaces();
			while (nis.hasMoreElements()) {
				final NetworkInterface ni = nis.nextElement();
				if (specified = specifiedInterfase.contains(ni.getName())) {
					if (ni.isLoopback()) {
						logger.warn("{}: Loopback address not recomended !!", logName);
					}
					if (logger.isInfoEnabled()) {
						logger.info("{}: Specified Interfase found: {}", logName, ni.getName());
					}
				}
				final Enumeration<InetAddress> ias = ni.getInetAddresses();
				while (ias.hasMoreElements()) {
					final InetAddress ia = ias.nextElement();
					if (ia.getHostName().equals(specifiedAddress)) {
						specified &= true;
						if (logger.isInfoEnabled()) {
							logger.info("{}: Specified Host address found: {}:{} with Hostname {}",
								logName, ia.getHostAddress(), ia.getHostName());
						}
					}
					if (ia.isSiteLocalAddress()) {
						fallback = fallback == null ? ia : fallback;
						if (specified) {
							return ia;
						}
					} else if (specified) {
						logger.warn("{}: Specified Address: {} is not LAN candidate (local)",
								logName, ia.getHostAddress());
					}
				}
			}
		} catch (Exception e) {
			//journal.commit(compose(getClass(), Fact.shard_finding_address).with(Result.FAILURE).with(e).build());
			logger.error("{}: Cannot build shard id value with hostname", logName, e);
		}
		if (fallback != null) {
			logger.warn("{}: Using found fallback: {}!", logName, fallback);
			return fallback;
		} else {
			throw new IllegalArgumentException("None valid Inet address found (Specified: " + specifiedAddress + ")");
		}
	}

	@Override
	public int getPort() {
		return this.port;
	}

	@Override
	public InetAddress getAddress() {
		return this.sourceHost;
	}

	public int hashCode() {
		return new HashCodeBuilder().append(getId()).toHashCode();
	}

	@Override
	public boolean equals(final Object obj) {
		if (obj == null || !(obj instanceof TCPShardIdentifier)) {
			return false;
		} else if (obj == this) {
			return true;
		} else {
			return ((TCPShardIdentifier) obj).getId().equals(getId());
		}
	}

	@Override
	@JsonIgnore
	public DateTime getCreation() {
		return this.creation;
	}

	@Override
	public String toString() {
		return id;
	}

	@Override
	public String getId() {
		return this.id;
	}

	@Override
	public void close() throws IOException {
		release();
	}

	@Override
	public void setWebHostPort(final String hostport) {
		this.webhostport = hostport;
	}

	public String getWebhostport() {
		return this.webhostport;
	}

}
