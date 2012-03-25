package by.dragoon.proxy;

import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

public class Application {

	private static final Logger LOG = Logger.getLogger(Application.class);
	private static final String PROPERTY_FILE = "proxy.properties";
	private static final String DELIMITER = "\\.";

	public static void main(String[] args) {

		Properties properties = new Properties();
		try {
			properties.load(new FileInputStream(PROPERTY_FILE));
		} catch (FileNotFoundException e) {
			LOG.error(String.format("File %s does not exist", PROPERTY_FILE), e);
		} catch (IOException e) {
			LOG.error(e, e);
		}

		Thread workingThread = new Thread(new NioProxy(getConfiguredNodes(properties)));
		workingThread.setDaemon(true);
		workingThread.start();

		Scanner consoleScanner = new Scanner(System.in);
		String command;
		do {
			LOG.info("Type 'exit' without quotes to close program");
			command = consoleScanner.nextLine();
		} while (!"exit".equals(command));

		workingThread.interrupt();
	}

	private static List<ConfigNode> getConfiguredNodes(Properties properties) {
		List<ConfigNode> configNodeList = new ArrayList<ConfigNode>();
		Map<String, ConfigNode> configNodes = new HashMap<String, ConfigNode>();
		for (Map.Entry<Object, Object> entry : properties.entrySet()) {
			String keys[] = ((String) entry.getKey()).split(DELIMITER);
			if (keys.length == 2) {
				ConfigNode configNode = configNodes.get(keys[0]);
				if (configNode == null) {
					configNode = new ConfigNode(keys[0]);
					configNodes.put(keys[0], configNode);
				}
				configNode.setParameter(keys[1], (String) entry.getValue());
			}
		}

		Map<Integer, String> localPorts = new HashMap<Integer, String>();
		for (ConfigNode entry : configNodes.values()) {
			if (entry.isConfigured()) {
				String conflictName = localPorts.get(entry.getLocalPort());
				if (conflictName == null) {
					configNodeList.add(entry);
					localPorts.put(entry.getLocalPort(), entry.getName());
				} else {
					LOG.error(String.format("Mapping local port configuration '%s' conflict with '%s'. Mapping node with name '%s' skipped.", conflictName, entry.getName(), entry.getName()));
				}
			}
		}

		if (LOG.isInfoEnabled()) {
			LOG.info(String.format("%s mapping ports configured", configNodeList.size()));
		}

		return configNodeList;
	}

}

