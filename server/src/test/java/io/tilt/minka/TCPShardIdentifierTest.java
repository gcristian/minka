/**
 *  Copyright (c) 2011-2015 Zauber S.A.  -- All rights reserved
 */

package io.tilt.minka;

import java.net.BindException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.util.Enumeration;

import org.junit.Assert;
import org.junit.Test;

import io.tilt.minka.api.Config;
import io.tilt.minka.domain.TCPShardIdentifier;

public class TCPShardIdentifierTest {

	@Test
	public void test_grab_passed_port() throws Exception {
		final int port = 2323;
		final Config config = new Config("", "localhost:" + port);
		config.getBroker().setEnablePortFallback(false);
		final TCPShardIdentifier id = new TCPShardIdentifier(config);
		
		ServerSocket ss1 = null;
		try {
			ss1 = new ServerSocket(port);
			Assert.assertTrue("it didnt grabbed passed port", ! ss1.isBound());
		} catch (BindException be) {	
		} finally {
			id.leavePortReservation();
			if (ss1!=null) {
				ss1.close();
			}
		}
		Thread.sleep(2000l);
		ServerSocket ss2 = new ServerSocket(port);
		Assert.assertTrue("it grabbed passed port but it didnt leave reservation", ss2.isBound());
		id.close();
		
		ss2.close();
	}

	@Test
	public void test_grabs_next_available_port() throws Exception {
		final int port = 2323;
		final ServerSocket ss1 = new ServerSocket(port);
		assert ss1.isBound();
		final Config config = new Config("", "localhost:" + port);
		config.getBroker().setEnablePortFallback(true);
		final TCPShardIdentifier id = new TCPShardIdentifier(config);
		
		try {
			final ServerSocket ss2 = new ServerSocket(port + 1);
			Assert.assertTrue(!ss2.isBound());
		} catch (Exception be) {
			Assert.assertTrue(be instanceof BindException);
		} finally {
			id.leavePortReservation();
			id.close();
			ss1.close();
		}
	}
	
	@Test
	public void test_acquires_a_correct_lan_address() throws Exception {
		final int port = 2323;
		final Config config = new Config("", "localhost:" + port);
		final TCPShardIdentifier id = new TCPShardIdentifier(config);

		final String hostname = config.getBroker().getHostPort().split(":")[0];
		final Enumeration<NetworkInterface> enu = NetworkInterface.getNetworkInterfaces();
		boolean found = false;
		while (enu.hasMoreElements()) {
			final NetworkInterface ni = enu.nextElement();
			final Enumeration<InetAddress> ia = ni.getInetAddresses();
			while (ia.hasMoreElements()) {
				InetAddress iae = ia.nextElement();
				if (iae.getHostName().equals(hostname)) {
					Assert.assertTrue(iae.isSiteLocalAddress());
					found = true;
				}
			}
		}
		Assert.assertTrue("site local address not found", found);
		id.leavePortReservation();
		ServerSocket ss2 = new ServerSocket(port);
		assert ss2.isBound();
		id.close();
		ss2.close();
	}
	
	
}
