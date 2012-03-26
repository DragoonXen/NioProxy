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
import java.nio.channels.UnresolvedAddressException;
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
	// channels data write messages queues
	private Map<SelectableChannel, LinkedList<ByteBuffer>> pendingData = new HashMap<SelectableChannel,
			LinkedList<ByteBuffer>>();
	private Set<SelectableChannel> openedChannels = new HashSet<SelectableChannel>();

	public NioProxy(List<ConfigNode> configNodes) {
		this.configNodes = configNodes;
	}

	@Override
	public void run() {
		try {
			startSelector();
		} catch (CancelledKeyException e) {
			if (Thread.interrupted()) {
				// This exception can throws while current thread interrupting
				LOG.info(e.getMessage(), e);
			} else {
				LOG.error(e, e);
			}
		} catch (ClosedByInterruptException e) {
			LOG.info(e.getMessage(), e);
		} catch (IOException e) {
			LOG.error(e, e);
		} finally {
			if (connectionsSelector != null) {
				//Close all remaining openedChannels
				SelectableChannel channels[] = new SelectableChannel[this.openedChannels.size()];
				this.openedChannels.toArray(channels);
				for (SelectableChannel channel : channels) {
					closeChannel(channel);
				}
				// Close selector
				try {
					connectionsSelector.close();
				} catch (IOException e) {
					LOG.error(e.getMessage(), e);
				}
			}
		}
	}

	/**
	 * Main working class method
	 *
	 * @throws IOException If an I/O error occurs
	 */
	private void startSelector() throws IOException {
		if (configNodes == null || configNodes.size() == 0) {
			LOG.info("Nothing to do.");
			return;
		}

		connectionsSelector = Selector.open();

		// Start listening all configured nodes
		for (ConfigNode configNode : configNodes) {
			ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
			openedChannels.add(serverSocketChannel);
			serverSocketChannel.configureBlocking(false);
			InetSocketAddress socketAddress = new InetSocketAddress(configNode.getLocalPort());

			if (LOG.isInfoEnabled()) {
				LOG.info(String.format("Bind to %s", socketAddress));
			}
			try {
				serverSocketChannel.socket().bind(socketAddress);
				// register SocketChannel && attach ConfigNode (can replace with InetSocketAddress)
				serverSocketChannel.register(connectionsSelector, SelectionKey.OP_ACCEPT).attach(configNode);
			} catch (BindException e) {
				// If, for example, local port is busy, we not interrupted, but skip this node
				LOG.error(String.format("Error while binding to %s", socketAddress), e);
			}
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
		// only accepting keys contain ConfigNode as attach
		ConfigNode configNode = (ConfigNode) selectionKey.attachment();
		if (LOG.isInfoEnabled()) {
			LOG.info(String.format("Attempting connect to %s (local port %s)", configNode.getRemoteSocketAddress(),
					configNode.getLocalPort()));
		}

		ServerSocketChannel serverSocketChannel = (ServerSocketChannel) selectionKey.channel();
		SocketChannel remoteSocketChannel = SocketChannel.open();
		remoteSocketChannel.configureBlocking(false);

		try {
			remoteSocketChannel.connect(configNode.getRemoteSocketAddress());
		} catch (UnresolvedAddressException e) {
			// If cann't resolve connection address - unregister serverSocketChannel && close remoteSocketChannel
			LOG.error(e.getMessage(), e);
			closeChannel(serverSocketChannel);
			closeChannel(remoteSocketChannel);
			return;
		}

		SocketChannel localSocketChannel = serverSocketChannel.accept();
		localSocketChannel.configureBlocking(false);

		SelectionKey acceptedSelectionKey = localSocketChannel.register(connectionsSelector, SelectionKey.OP_READ);
		SelectionKey connectedSelectionKey = remoteSocketChannel.register(connectionsSelector, SelectionKey.OP_CONNECT);
		connectedSelectionKey.attach(localSocketChannel);
		acceptedSelectionKey.attach(remoteSocketChannel);

		pendingData.put(localSocketChannel, new LinkedList<ByteBuffer>());
		pendingData.put(remoteSocketChannel, new LinkedList<ByteBuffer>());
		openedChannels.add(localSocketChannel);
		openedChannels.add(remoteSocketChannel);

		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("Channels pair %s %s", localSocketChannel.hashCode(),
					remoteSocketChannel.hashCode()));
		}
	}

	/**
	 * Finished connection to selected socketChannel
	 *
	 * @param selectionKey SelectionKey correspond to connecting socketChannel
	 * @throws ClosedByInterruptException If current thread interrupt
	 */
	private void finishConnection(SelectionKey selectionKey) throws ClosedByInterruptException {
		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("Connected access %s", selectionKey.channel().hashCode()));
		}
		SocketChannel socketChannel = (SocketChannel) selectionKey.channel();

		try {
			socketChannel.finishConnect();
		} catch (ClosedByInterruptException e) {
			throw e;
		} catch (IOException e) {
			String exceptionMessage = e.toString();
			if (CONNECTION_REFUSED.equals(exceptionMessage)) {
				// Usual situation, just logging && closing connection
				LOG.info(exceptionMessage);
			} else {
				LOG.error(exceptionMessage, e);
			}
			closeBothConnections(selectionKey);
			return;
		}

		selectionKey.interestOps(SelectionKey.OP_WRITE);
	}

	/**
	 * Read data from selected readable socketChannel
	 *
	 * @param selectionKey selectionKey correspond to readable socketChannel
	 * @throws ClosedByInterruptException If current thread interrupt
	 */
	private void readData(SelectionKey selectionKey) throws ClosedByInterruptException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Read access " + selectionKey.channel().hashCode());
		}

		SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
		buffer.clear();
		int numRead;
		try {
			numRead = socketChannel.read(buffer);
		} catch (ClosedByInterruptException e) {
			throw e;
		} catch (IOException e) {
			String exceptionMessage = e.toString();
			if (PEER_RESET_CONNECTION_EXCEPTION.equals(exceptionMessage)) {
				// Usual situation, just logging && closing connection
				LOG.info(exceptionMessage);
			} else {
				LOG.error(exceptionMessage, e);
			}
			closeBothConnections(selectionKey);
			return;
		}

		if (numRead == -1) { // If connection was closed remotely
			closeBothConnections(selectionKey);
			return;
		}

		if (numRead > 0) {
			byte write[] = new byte[numRead];
			System.arraycopy(buffer.array(), 0, write, 0, numRead);
			SelectableChannel pairedSelectionChannel = (SelectableChannel) selectionKey.attachment();
			LinkedList<ByteBuffer> queue = pendingData.get(pairedSelectionChannel);
			if (queue != null) { // If paired connection (and his data write queue) exist
				queue.add(ByteBuffer.wrap(write));
				SelectionKey pairedSelectionKey = pairedSelectionChannel.keyFor(connectionsSelector);
				// If paired connection connected, add him write flag
				if ((pairedSelectionKey.interestOps() & SelectionKey.OP_CONNECT) == 0) {
					pairedSelectionKey.interestOps(pairedSelectionKey.interestOps() | SelectionKey.OP_WRITE);
				}
			}
		}
	}

	/**
	 * Close current connection. If second have no queue data to write, it also will closed
	 *
	 * @param selectionKey Closing selectionKey
	 */
	private void closeBothConnections(SelectionKey selectionKey) {
		closeChannel(selectionKey.channel());
		SelectableChannel pairedSelectableChannel = (SelectableChannel) selectionKey.attachment();
		List<ByteBuffer> messages = pendingData.get(pairedSelectableChannel);
		if (messages != null && messages.size() == 0) {
			closeChannel(pairedSelectableChannel);
		}
	}

	/**
	 * Close selectableChannel && cancel his key in selector
	 *
	 * @param channel Closing channel
	 */
	private void closeChannel(SelectableChannel channel) {
		if (LOG.isInfoEnabled()) {
			LOG.info("Close connection " + channel.hashCode());
		}

		pendingData.remove(channel);
		openedChannels.remove(channel);
		try {
			channel.close();
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		}
		SelectionKey channelKey = channel.keyFor(connectionsSelector);
		if (channelKey != null) {
			channelKey.cancel();
		}
	}

	/**
	 * write pending data into selected writable socketChannel
	 *
	 * @param selectionKey selectionKey correspond to writable socketChannel
	 * @throws ClosedByInterruptException If current thread interrupt
	 */
	private void writeData(SelectionKey selectionKey) throws ClosedByInterruptException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Write access " + selectionKey.channel().hashCode());
		}

		SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
		LinkedList<ByteBuffer> queue = pendingData.get(socketChannel);
		while (!queue.isEmpty()) {
			ByteBuffer writingBuffer = queue.getFirst();
			try {
				socketChannel.write(writingBuffer);
			} catch (ClosedByInterruptException e) {
				throw e;
			} catch (IOException e) {
				String exceptionMessage = e.toString();
				if (PEER_RESET_CONNECTION_EXCEPTION.equals(exceptionMessage)) {
					// Usual situation, just logging && closing connection
					LOG.info(exceptionMessage);
				} else {
					LOG.error(exceptionMessage, e);
				}
				closeBothConnections(selectionKey);
				return;
			}
			if (writingBuffer.remaining() > 0) { // If not all buffer data has writed into socket
				break;
			}
			queue.removeFirst();
		}

		if (queue.isEmpty()) {
			// If paired connection exist set read state to this connection. Otherwise close this connection
			if (openedChannels.contains(selectionKey.attachment())) {
				selectionKey.interestOps(SelectionKey.OP_READ);
			} else {
				closeChannel(socketChannel);
			}
		}
	}
}
