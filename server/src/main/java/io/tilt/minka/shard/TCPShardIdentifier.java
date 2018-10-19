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
package io.tilt.minka.shard;

import static java.nio.charset.Charset.defaultCharset;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoField;
import java.util.Enumeration;
import java.util.Random;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang3.Validate;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Base64Utils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.config.BootstrapConfiguration;
import io.tilt.minka.core.follower.FollowerBootstrap;

/**
 * Identity of a shard (followers and leader alike)
 * Created by the {@linkplain FollowerBootstrap}
 * 
 * @author Cristian Gonzalez
 * @since Dec 3, 2015
 */
public class TCPShardIdentifier implements NetworkShardIdentifier, Closeable {

	@JsonIgnore
	private static final Logger logger = LoggerFactory.getLogger(TCPShardIdentifier.class);

	private static final long serialVersionUID = 3233785408081305735L;
	private static final Random random = new Random();
	private static final int PORT_SEARCHES_MAX = 100;
	@JsonIgnore
	private static final String logName = TCPShardIdentifier.class.getSimpleName();

	private String id;
	@JsonIgnore
	private final InetAddress sourceHost;
	@JsonIgnore
	private int port;
	@JsonIgnore
	private final int configuredPort;  
	@JsonIgnore
	private final DateTime creation;
	@JsonProperty(index = 5, value = "tag")
	// this transports the follower's config (value) which doesnt reach the leader
	private final String tag;
	@JsonIgnore
	private transient ServerSocket bookedSocket;
	@JsonProperty(index = 4, value = "web-host-port")
	private String webhostport;

	public TCPShardIdentifier(final Config config) throws Exception {
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
		if (config.getBroker().isEnablePortFallback()) {
			take();
		}
		buildId(config);
		
		this.tag = buildTag(config.getBootstrap().getServerTag());
		config.getBootstrap().setServerTag(this.tag);
	}

	/**
	 * @param tag	the configured or default tag
	 * @return		only when default: [second] [millis] @ [host name] 
	 */
	private String buildTag(final String tag) {
		if (tag.equals(BootstrapConfiguration.SERVER_TAG)) {
			
			final StringBuilder tmp = currentSecondAndMillisecond().append('@');
			try {
				tmp.append(InetAddress.getLocalHost().getCanonicalHostName());
			} catch (UnknownHostException e) {
				logger.error("{}: Couldnt gather localhost name falling back to host address's name");
				tmp.append(sourceHost.getCanonicalHostName());
			}
			return tmp.toString();
		} else {
			return tag;
		}
	}
	
	@JsonProperty("broker-connect")
	public String getConnectString() {
		return new StringBuilder()
				.append(sourceHost.getHostName())
				.append(':')
				.append(port)
				.toString();
	}
	
	@Override
	public String getTag() {
		return tag;
	}

	/*  with a best effort for helping multi-tenancy and noisy infra people */
	@Override
	public void take() throws Exception {
		Exception cause = null;
		for (int search = 0; search < PORT_SEARCHES_MAX ; this.port++, search++) {
			if (logger.isInfoEnabled()) {
				logger.info("{}: {} port {}:{} (search no.{}/{}) ", logName,
					search == 0 ? "Testing" : "Trying fallback port", sourceHost, this.port, search, PORT_SEARCHES_MAX);
			}
			
			try {
				bookedSocket = bookAPort(this.port);
				if (bookedSocket != null) {
					return;
				}
			} catch (Exception e) {
				if (search == 0) {
					cause = e;
				}
			}
		}
		this.port = configuredPort; // just going back
		String fallbackFailed = "Fallbacks failed - try configuring a valid open port";
		String configFailed = "To avoid boot-up failure enable configuration parameter: brokerServerPortFallback = true";
		final Exception excp = new IllegalArgumentException(fallbackFailed, cause);
		logger.error("{}: No open port Available ! {}", logName, excp);
		throw excp;
	}

	private ServerSocket bookAPort(final int testPort) {
		ServerSocket socket = null;
		try {
			socket = new ServerSocket(testPort, 1, sourceHost);
			if (!socket.isBound()) {
				return null;
			}
			if (logger.isInfoEnabled()) {
				logger.info("{}: Testing host {} port {} OK", logName, sourceHost, testPort);
			}
			return socket;
		} catch (IOException e) {
			IOUtils.closeQuietly(socket);
			throw new IllegalArgumentException("Testing port cannot be opened: " + testPort, e);
		}
	}

	@Override
	public void release() {
		try {
			if (bookedSocket != null && !bookedSocket.isClosed()) {
				bookedSocket.close();
				bookedSocket = null;
			}
		} catch (IOException e) {
			throw new IllegalArgumentException("Testing port cannot be tested: ", e);
		}
	}

	private void buildId(final Config config) {
		String id = null;
		if (sourceHost != null) {
			if (config.getBroker().isUseMachineHostname() && !sourceHost.getHostAddress().isEmpty()) {
				id = sourceHost.getHostAddress();
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
		
		// we need to add a restart-against variance so the leader doesn't get confused
		final String rid = Base64Utils.encodeToString(currentSecondAndMillisecond().toString().getBytes(defaultCharset()));
		this.id = id + ":" + port + ":" + rid;
		
		config.getBroker().setHostPort(this.id);
	}

	private static StringBuilder currentSecondAndMillisecond() {
		return new StringBuilder()
			.append(LocalDateTime.now().getSecond())
			.append(Instant.now().get(ChronoField.MILLI_OF_SECOND));
	}

	/** 
	 * @return trying to find a LAN candidate address 
	 * @param both nullables 
	 */
	public static InetAddress findLANAddress() {
		return findLANAddress(null, null);
	}
	public static InetAddress findLANAddress(final String specifiedAddress, final String specifiedInterfase) {
		InetAddress fallback = null;
		boolean specified = false;
		try {
			if (logger.isInfoEnabled()) {
				logger.info("{}: Looking for configured network interfase/address: {}/{}", logName,
					specifiedInterfase, specifiedAddress);
			}
			final Enumeration<NetworkInterface> nis = NetworkInterface.getNetworkInterfaces();
			while (nis.hasMoreElements()) {
				final NetworkInterface ni = nis.nextElement();
				if (specifiedInterfase!=null 
						&& (specified = specifiedInterfase.contains(ni.getName()))) {
					if (ni.isLoopback()) {
						logger.warn("{}: Loopback address not recomended !!", logName);
					}
					if (logger.isInfoEnabled()) {
						logger.info("{}: Specified Interfase found: {}", logName, ni.getName());
					}
				}
				if (!ni.isLoopback() && ni.isUp()) {
					final Enumeration<InetAddress> ias = ni.getInetAddresses();
					while (ias.hasMoreElements()) {
						final InetAddress ia = ias.nextElement();
						if (ia.getHostName().equals(specifiedAddress)) {
							specified &= true;
							if (logger.isInfoEnabled()) {
								logger.info("{}: Specified Host address found: {}:{}",
									logName, ia.getHostAddress());
							}
						}
						if (ia.isSiteLocalAddress()) {
							if (!ia.getHostAddress().startsWith("192.") || fallback == null) {
								fallback = ia;
							}
							if (specified) {
								return ia;
							}
						} else if (specified) {
							logger.warn("{}: Specified Address: {} is not LAN candidate (local)",
									logName, ia.getHostAddress());
						}
					}
				}
			}
		} catch (Exception e) {
			//journal.commit(compose(getClass(), Fact.shard_finding_address).with(ChangePlanState.FAILURE).with(e).build());
			logger.error("{}: Cannot build shard id value with hostname", logName, e);
		}
		if (fallback != null) {
			logger.warn("{}: Using found fallback: {}!", logName, fallback);
			return fallback;
		} else {
			throw new IllegalArgumentException("None valid Inet address found (Specified: " + specifiedAddress + ")");
		}
	}

	public static void main(String[] args) throws Exception {
		printDevice();
	}
	
	public static void printDevice() throws Exception {
			final Enumeration<NetworkInterface> nis = NetworkInterface.getNetworkInterfaces();
			while (nis.hasMoreElements()) {
				System.out.println("=================================================");
				final NetworkInterface ni = nis.nextElement();
				System.out.println("NI name     " + ni.getName());
				System.out.println("NI loopback " + ni.isLoopback());
				System.out.println("NI P2P      " + ni.isPointToPoint());
				System.out.println("NI up       " + ni.isUp());
				System.out.println("NI virtual  " + ni.isVirtual());
				final Enumeration<InetAddress> ias = ni.getInetAddresses();
				while (ias.hasMoreElements()) {
					final InetAddress ia = ias.nextElement();
					System.out.println(". . . . . . . . . . . . . . . . . . . . .");
					System.out.println("\tIA host addr " + ia.getHostAddress());
					System.out.println("\tIA localhost " + ia.getLocalHost());
					System.out.println("\tIA host name " + ia.getHostName());
					System.out.println("\tIA canonical " + ia.getCanonicalHostName());
					System.out.println("\tIA loopback addr " + ia.getLoopbackAddress());
					System.out.println("\tIA addr          " + ia.getAddress());

					System.out.println("\t\t IA site local " + ia.isSiteLocalAddress());
					System.out.println("\t\t IA link local " + ia.isLinkLocalAddress());
					System.out.println("\t\t IA any local  " + ia.isAnyLocalAddress());
					System.out.println("\t\t IA multicast  " + ia.isMulticastAddress());

				}
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
		return new StringBuilder(id.length() + 3 + (tag!=null ? tag.length() : 0))
				.append(id)
				.append(" (").append(tag).append(')')
				.toString();
	}

	@Override
	@JsonIgnore
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

	@Override
	public String getWebHostPort() {
		return this.webhostport;
	}

}
