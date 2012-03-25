package by.dragoon.proxy;

import org.apache.log4j.Logger;

import java.net.InetSocketAddress;

/**
 * User: dragoon
 * Date: 3/24/12
 * Time: 8:55 PM
 */
public class ConfigNode {

	private static final Logger LOG = Logger.getLogger(ConfigNode.class);

	private static final String LOCAL_PORT = "localPort";
	private static final String REMOTE_PORT = "remotePort";
	private static final String REMOTE_HOST = "remoteHost";

	private int localPort;
	private String remoteHost;
	private int remotePort;
	private String name;
	private InetSocketAddress remoteSocketAdress;

	public ConfigNode(String name) {
		this.name = name;
		localPort = -1;
		remotePort = -1;
		remoteHost = null;
		remoteSocketAdress = null;
	}

	public void setParameter(String parameter, String value) {
		if (LOCAL_PORT.equals(parameter)) {
			setLocalPort(parseInt(value));
		} else if (REMOTE_PORT.equals(parameter)) {
			setRemotePort(parseInt(value));
		} else if (REMOTE_HOST.equals(parameter)) {
			setRemoteHost(value);
		} else {
			LOG.error(String.format("Parameter %s is not supported", parameter));
		}
	}

	private int parseInt(String value) {
		try {
			return Integer.parseInt(value);
		} catch (NumberFormatException e) {
			LOG.error(e, e);
			return -1;
		}
	}

	public int getLocalPort() {
		return localPort;
	}

	public void setRemotePort(int remotePort) {
		if (remotePort != this.remotePort) {
			this.remotePort = remotePort;
			remoteSocketAdress = null;
		}
	}

	public void setRemoteHost(String remoteHost) {
		if (remoteHost != null && !remoteHost.equals(this.remoteHost)) {
			this.remoteHost = remoteHost;
			remoteSocketAdress = null;
		}
	}

	public void setLocalPort(int localPort) {
		this.localPort = localPort;
	}

	public String getName() {
		return name;
	}

	public InetSocketAddress getRemoteSocketAdress() {
		if (remoteSocketAdress == null && isConfigured()) {
			remoteSocketAdress = new InetSocketAddress(remoteHost, remotePort);
		}
		return remoteSocketAdress;
	}

	public boolean isConfigured() {
		return localPort > -1 && remotePort > -1 && remoteHost != null && !"".equals(remoteHost);
	}
}
