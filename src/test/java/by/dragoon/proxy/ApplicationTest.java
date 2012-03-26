package by.dragoon.proxy;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ApplicationTest {

	private final static Logger LOG = Logger.getLogger(ApplicationTest.class);

	private final static int PROXY_CONNECTION_PORT = 8789;
	private final static int PROXY_LISTEN_PORT = 8788;
	private final static int BUFFER_SIZE = 27325253;
	private Thread proxyThread;
	private boolean fail = false;

	@Before
	public void setUp() {
		List<ConfigNode> nodesList = new ArrayList<ConfigNode>();
		ConfigNode node = new ConfigNode("first");
		nodesList.add(node);
		node.setLocalPort(PROXY_LISTEN_PORT);
		node.setRemoteHost("localhost");
		node.setRemotePort(PROXY_CONNECTION_PORT);

		proxyThread = new Thread(new NioProxy(nodesList));
		proxyThread.start();

		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {
			LOG.error(e, e);
			Assert.fail();
		}
		LOG.info("Proxy thread ready");
		fail = false;
	}

	@After
	public void tearDown() {
		assert (proxyThread.isAlive());
		proxyThread.interrupt();
		try {
			proxyThread.join();
		} catch (InterruptedException e) {
			LOG.error(e, e);
			Assert.fail();
		}
		proxyThread = null;
	}

	@Test
	public void concurrentUploadTest() throws IOException {
		Thread concurrentThreads[] = new Thread[10];
		for (int i = 0; i != 10; i++) {
			concurrentThreads[i] = new Thread(oneUploadDownloadClass);
		}
		for (int i = 0; i != 10; i++) {
			concurrentThreads[i].start();
		}
		for (int i = 0; i != 10; i++) {
			try {
				concurrentThreads[i].join();
			} catch (InterruptedException e) {
				LOG.error(e, e);
				Assert.fail();
			}
		}
		assert (!fail);
	}

	@Test
	public void downloadUploadTest() throws IOException {
		for (int i = 0; i != 10; i++) {
			oneUploadDownloadClass.run();
		}
		assert (!fail);
	}

	private Runnable oneUploadDownloadClass = new Runnable() {
		@Override
		public void run() {
			try {
				work();
			} catch (IOException e) {
				LOG.error(e, e);
				fail = true;
			}
		}

		private void work() throws IOException {
			byte array[] = new byte[BUFFER_SIZE];
			new Random().nextBytes(array);
			ByteBuffer buffer = ByteBuffer.wrap(array);
			ByteBuffer retBuffer = ByteBuffer.allocate(array.length);
			SocketChannel writeChannel;
			SocketChannel readChannel;

			synchronized (this) {
				ServerSocketChannel channel = ServerSocketChannel.open();
				channel.socket().bind(new InetSocketAddress(PROXY_CONNECTION_PORT));
				writeChannel = SocketChannel.open(new InetSocketAddress(PROXY_LISTEN_PORT));
				readChannel = channel.accept();
				channel.close();
			}

			int rez = 0;
			do {
				writeChannel.write(buffer);
				rez += readChannel.read(retBuffer);
			} while (buffer.remaining() > 0);

			while (rez < array.length) {
				rez += readChannel.read(retBuffer);
			}

			byte secondBuf[] = retBuffer.array();
			LOG.info(String.format("%s bytes send, %s bytes receive", array.length, secondBuf.length));
			assert (array.length == secondBuf.length);
			int errors = 0;
			for (int i = 0; i != array.length; i++) {
				if (array[i] != secondBuf[i]) {
					LOG.error(String.format("Error at byte %s", i));
					if (++errors == 10) {
						break;
					}
				}
			}

			ByteBuffer echoBuffer = ByteBuffer.allocate(array.length);

			rez = 0;
			retBuffer.flip();
			do {
				readChannel.write(retBuffer);
				rez += writeChannel.read(echoBuffer);
			} while (retBuffer.remaining() > 0);

			while (rez < array.length) {
				rez += writeChannel.read(echoBuffer);
			}
			readChannel.close();
			writeChannel.close();

			byte echoBuf[] = retBuffer.array();
			LOG.info(String.format("%s bytes send, %s bytes receive", echoBuf.length, secondBuf.length));
			assert (echoBuf.length == secondBuf.length);
			for (int i = 0; i != echoBuf.length; i++) {
				if (echoBuf[i] != secondBuf[i]) {
					LOG.error(String.format("Error at byte %s", i));
					if (++errors == 10) {
						break;
					}
				}
			}
			assert (errors == 0);
		}
	};


}
