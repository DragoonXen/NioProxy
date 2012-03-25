package by.dragoon.proxy;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * User: dragoon
 * Date: 3/25/12
 * Time: 12:33 PM
 */
public class NioProxy implements Runnable {

	private static final Logger LOG = Logger.getLogger(NioProxy.class);
	private static final String PEER_RESET_CONNECTION_EXCEPTION = "java.io.IOException: Connection reset by peer";

	private Selector connectionsSelector;
	private ByteBuffer buffer = ByteBuffer.allocate(8192);

	private List<ConfigNode> configNodes;
	private Map<SelectableChannel, LinkedList<ByteBuffer>> pendingMessages = new HashMap<SelectableChannel, LinkedList<ByteBuffer>>();
	private Map<SelectableChannel, Integer> writedBytes = new HashMap<SelectableChannel, Integer>();
	private Map<SelectableChannel, Integer> readedBytes = new HashMap<SelectableChannel, Integer>();

	public NioProxy(List<ConfigNode> configNodes) {
		this.configNodes = configNodes;
	}

	@Override
	public void run() {
		try {
			startSelector();
		} catch (ClosedByInterruptException e) {
			LOG.info("Thread interrupted", e);
		} catch (IOException e) {
			LOG.error(e, e);
		} finally {
			if (connectionsSelector != null) {
				try {
					connectionsSelector.close();
				} catch (IOException e) {
					LOG.error(e, e);
				}
			}
		}
	}

	private void startSelector() throws IOException {
		if (configNodes == null || configNodes.size() == 0) {
			LOG.info("Nothing to do.");
			return;
		}

		connectionsSelector = Selector.open();

		for (ConfigNode configNode : configNodes) {
			ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
			serverSocketChannel.configureBlocking(false);
			InetSocketAddress socketAddress = new InetSocketAddress(configNode.getLocalPort());

			if (LOG.isInfoEnabled()) {
				LOG.info(String.format("Bind to %s", socketAddress));
			}
			try {
				serverSocketChannel.socket().bind(socketAddress);
			} catch (BindException e) {
				LOG.error(String.format("Error while binding to %s", socketAddress), e);
			}

			serverSocketChannel.register(connectionsSelector, SelectionKey.OP_ACCEPT).attach(configNode);
		}

		while (!Thread.interrupted()) {
			int selectionKeysCount = connectionsSelector.select();

			if (selectionKeysCount == 0) {
				continue;
			}

			Iterator selectedKeysIterator = connectionsSelector.selectedKeys().iterator();
			while (selectedKeysIterator.hasNext()) {
				SelectionKey selectionKey = (SelectionKey) selectedKeysIterator.next();
				selectedKeysIterator.remove();

				if (!selectionKey.isValid()) {
					if (selectionKey.channel().isOpen()) {
						LOG.warn("Channel is still working! Closing", new Throwable());
						closeChannel(selectionKey.channel());
					}
					continue;
				}

				if (selectionKey.isReadable()) {
					readData(selectionKey);
				} else if (selectionKey.isWritable()) {
					writeData(selectionKey);
				} else if (selectionKey.isConnectable()) {
					finishConnection(selectionKey);
				} else if (selectionKey.isAcceptable()) {
					acceptConnection(selectionKey);
				}
			}
		}
	}

	private void acceptConnection(SelectionKey selectionKey) throws IOException {
		ConfigNode configNode = (ConfigNode) selectionKey.attachment();
		ServerSocketChannel serverSocketChannel = (ServerSocketChannel) selectionKey.channel();
		if (LOG.isInfoEnabled()) {
			LOG.info(String.format("Attempting connect to %s (local port %s)", configNode.getRemoteSocketAdress(),
					configNode.getLocalPort()));
		}

		SocketChannel readSocketChannel = serverSocketChannel.accept();
		readSocketChannel.configureBlocking(false);
		SelectionKey acceptedSelectionKey = readSocketChannel.register(connectionsSelector, SelectionKey.OP_READ);

		SocketChannel connectSocketChannel = SocketChannel.open();
		connectSocketChannel.configureBlocking(false);
		connectSocketChannel.connect(configNode.getRemoteSocketAdress());
		SelectionKey connectedSelectionKey = connectSocketChannel.register(connectionsSelector, SelectionKey.OP_CONNECT);
		connectedSelectionKey.attach(readSocketChannel);
		acceptedSelectionKey.attach(connectSocketChannel);

		pendingMessages.put(readSocketChannel, new LinkedList<ByteBuffer>());
		pendingMessages.put(connectSocketChannel, new LinkedList<ByteBuffer>());
		readedBytes.put(readSocketChannel, 0);
		readedBytes.put(connectSocketChannel, 0);
		writedBytes.put(readSocketChannel, 0);
		writedBytes.put(connectSocketChannel, 0);

		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("Channels pair %s %s", readSocketChannel.hashCode(), connectSocketChannel.hashCode()));
		}
	}

	private void finishConnection(SelectionKey selectionKey) throws IOException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Connected access " + selectionKey.channel().hashCode());
		}
		SocketChannel socketChannel = (SocketChannel) selectionKey.channel();

		try {
			socketChannel.finishConnect();
		} catch (IOException e) {
			LOG.error(e, e);
			closeChannel(socketChannel);
			return;
		}

		selectionKey.interestOps(SelectionKey.OP_WRITE);
	}

	private void readData(SelectionKey readingSelectionKey) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Read access " + readingSelectionKey.channel().hashCode());
		}
		SocketChannel socketChannel = (SocketChannel) readingSelectionKey.channel();

		buffer.clear();

		SelectableChannel writeSelectionChannel = (SelectableChannel) readingSelectionKey.attachment();
		LinkedList<ByteBuffer> queue = pendingMessages.get(writeSelectionChannel);

		int numRead;
		try {
			numRead = socketChannel.read(buffer);
		} catch (IOException e) {
			if (PEER_RESET_CONNECTION_EXCEPTION.equals(e.toString())) {
				if (LOG.isInfoEnabled()) {
					LOG.info(e, e);
				}
			} else {
				LOG.error(e, e);
			}
			closeBothConnections(readingSelectionKey);
			return;
		}

		if (numRead == -1) {
			closeBothConnections(readingSelectionKey);
			return;
		}

		byte write[] = new byte[numRead];
		System.arraycopy(buffer.array(), 0, write, 0, numRead);

		queue.add(ByteBuffer.wrap(write));
		readedBytes.put(socketChannel, readedBytes.get(socketChannel) + numRead);

		SelectionKey writeSelectionKey = writeSelectionChannel.keyFor(connectionsSelector);
		writeSelectionKey.interestOps(SelectionKey.OP_WRITE);
	}

	private void closeBothConnections(SelectionKey selectionKey) {
		closeChannel(selectionKey.channel());
		SelectableChannel pairedSelectableChannel = (SelectableChannel) selectionKey.attachment();
		List<ByteBuffer> messages = pendingMessages.get(pairedSelectableChannel);
		if (messages != null && messages.size() == 0) {
			closeChannel(pairedSelectableChannel);
		}
	}

	private void closeChannel(SelectableChannel channel) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Close connection " + channel.hashCode());
			LOG.debug(String.format("Bytes writed: %s, Bytes readed: %s", writedBytes.get(channel), readedBytes.get(channel)));
		}
		pendingMessages.remove(channel);
		writedBytes.remove(channel);
		readedBytes.remove(channel);

		try {
			channel.close();
		} catch (IOException e) {
			LOG.error(e, e);
		}
		channel.keyFor(connectionsSelector).cancel();
	}

	private void writeData(SelectionKey selectionKey) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Write access " + selectionKey.channel().hashCode());
		}
		SocketChannel socketChannel = (SocketChannel) selectionKey.channel();

		LinkedList<ByteBuffer> queue = pendingMessages.get(socketChannel);

		while (!queue.isEmpty()) {
			ByteBuffer buf = queue.getFirst();
			int count = buf.remaining();
			try {
				socketChannel.write(buf);
			} catch (IOException e) {
				if (PEER_RESET_CONNECTION_EXCEPTION.equals(e.toString())) {
					if (LOG.isInfoEnabled()) {
						LOG.info(e, e);
					}
				} else {
					LOG.error(e, e);
				}
				closeBothConnections(selectionKey);
			}
			writedBytes.put(socketChannel, writedBytes.get(socketChannel) + count - buf.remaining());
			if (buf.remaining() > 0) {
				break;
			}
			queue.removeFirst();
		}

		if (queue.isEmpty()) {
			if (pendingMessages.get(selectionKey.attachment()) == null) {
				closeChannel(socketChannel);
			} else {
				selectionKey.interestOps(SelectionKey.OP_READ);
			}
		}
	}
}
