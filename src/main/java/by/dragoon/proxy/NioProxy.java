package by.dragoon.proxy;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * User: dragoon
 * Date: 3/25/12
 * Time: 12:33 PM
 */
public class NioProxy implements Runnable {

	private static final Logger LOG = Logger.getLogger(NioProxy.class);
	private static final String PEER_RESET_CONNECTION_EXCEPTION = "java.io.IOException: Connection reset by peer";
	private static final String CONNECTION_REFUSED = "java.net.ConnectException: Connection refused";

	private Selector connectionsSelector;
	private ByteBuffer buffer = ByteBuffer.allocate(8192);

	private List<ConfigNode> configNodes;
	private Map<SelectableChannel, LinkedList<ByteBuffer>> pendingMessages = new HashMap<SelectableChannel,
			LinkedList<ByteBuffer>>();
	private Map<SelectableChannel, Integer> writedBytes = new HashMap<SelectableChannel, Integer>();
	private Map<SelectableChannel, Integer> readedBytes = new HashMap<SelectableChannel, Integer>();
	private Set<SelectableChannel> channels = new HashSet<SelectableChannel>();

	public NioProxy(List<ConfigNode> configNodes) {
		this.configNodes = configNodes;
	}

	@Override
	public void run() {
		try {
			startSelector();
		} catch (CancelledKeyException e) {
			LOG.info(e, e);
		} catch (ClosedByInterruptException e) {
			LOG.info(e, e);
		} catch (IOException e) {
			LOG.error(e, e);
		} finally {
			if (connectionsSelector != null) {
				SelectableChannel channels[] = new SelectableChannel[this.channels.size()];
				this.channels.toArray(channels);
				for (SelectableChannel channel : channels) {
					closeChannel(channel);
				}
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
			channels.add(serverSocketChannel);
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

		SocketChannel localSocketChannel = serverSocketChannel.accept();
		localSocketChannel.configureBlocking(false);
		SelectionKey acceptedSelectionKey = localSocketChannel.register(connectionsSelector, SelectionKey.OP_READ);

		SocketChannel remoteSocketChannel = SocketChannel.open();
		remoteSocketChannel.configureBlocking(false);
		remoteSocketChannel.connect(configNode.getRemoteSocketAdress());
		SelectionKey connectedSelectionKey = remoteSocketChannel.register(connectionsSelector, SelectionKey.OP_CONNECT);
		connectedSelectionKey.attach(localSocketChannel);
		acceptedSelectionKey.attach(remoteSocketChannel);

		pendingMessages.put(localSocketChannel, new LinkedList<ByteBuffer>());
		pendingMessages.put(remoteSocketChannel, new LinkedList<ByteBuffer>());
		readedBytes.put(localSocketChannel, 0);
		readedBytes.put(remoteSocketChannel, 0);
		writedBytes.put(localSocketChannel, 0);
		writedBytes.put(remoteSocketChannel, 0);
		channels.add(localSocketChannel);
		channels.add(remoteSocketChannel);

		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("Channels pair %s %s", localSocketChannel.hashCode(),
					remoteSocketChannel.hashCode()));
		}
	}

	private void finishConnection(SelectionKey selectionKey) throws IOException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Connected access " + selectionKey.channel().hashCode());
		}
		SocketChannel socketChannel = (SocketChannel) selectionKey.channel();

		try {
			socketChannel.finishConnect();
		} catch (ClosedByInterruptException e) {
			throw e;
		} catch (IOException e) {
			String exceptionMessage = e.toString();
			if (CONNECTION_REFUSED.equals(exceptionMessage)) {
				LOG.info(exceptionMessage);
			} else {
				LOG.error(exceptionMessage, e);
			}
			closeBothConnections(selectionKey);
			return;
		}

		selectionKey.interestOps(SelectionKey.OP_WRITE);
	}

	private void readData(SelectionKey readingSelectionKey) throws ClosedByInterruptException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Read access " + readingSelectionKey.channel().hashCode());
		}
		SocketChannel socketChannel = (SocketChannel) readingSelectionKey.channel();

		buffer.clear();

		int numRead;
		try {
			numRead = socketChannel.read(buffer);
		} catch (ClosedByInterruptException e) {
			throw e;
		} catch (IOException e) {
			String exceptionMessage = e.toString();
			if (PEER_RESET_CONNECTION_EXCEPTION.equals(exceptionMessage)) {
				LOG.info(exceptionMessage);
			} else {
				LOG.error(exceptionMessage, e);
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

		SelectableChannel writeSelectionChannel = (SelectableChannel) readingSelectionKey.attachment();
		LinkedList<ByteBuffer> queue = pendingMessages.get(writeSelectionChannel);
		if (queue != null) {
			queue.add(ByteBuffer.wrap(write));
			SelectionKey writeSelectionKey = writeSelectionChannel.keyFor(connectionsSelector);
			writeSelectionKey.interestOps(writeSelectionKey.interestOps() | SelectionKey.OP_WRITE);
		}
		readedBytes.put(socketChannel, readedBytes.get(socketChannel) + numRead);
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
		if (LOG.isInfoEnabled()) {
			LOG.info("Close connection " + channel.hashCode());
			LOG.info(String.format("Bytes writed: %s, Bytes readed: %s", writedBytes.get(channel),
					readedBytes.get(channel)));
		}

		pendingMessages.remove(channel);
		writedBytes.remove(channel);
		readedBytes.remove(channel);

		try {
			channel.close();
			channels.remove(channel);
		} catch (IOException e) {
			LOG.error(e, e);
		}
		channel.keyFor(connectionsSelector).cancel();
	}

	private void writeData(SelectionKey selectionKey) throws ClosedByInterruptException {
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
				writedBytes.put(socketChannel, writedBytes.get(socketChannel) + count - buf.remaining());
			} catch (ClosedByInterruptException e) {
				throw e;
			} catch (IOException e) {
				String exceptionMessage = e.toString();
				if (PEER_RESET_CONNECTION_EXCEPTION.equals(exceptionMessage)) {
					LOG.info(exceptionMessage);
				} else {
					LOG.error(exceptionMessage, e);
				}
				closeBothConnections(selectionKey);
				return;
			}
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
