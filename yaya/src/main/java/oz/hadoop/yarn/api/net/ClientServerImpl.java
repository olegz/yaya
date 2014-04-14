/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package oz.hadoop.yarn.api.net;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.Assert;

/**
 * @author Oleg Zhurakousky
 * 
 */
class ClientServerImpl extends AbstractSocketHandler implements ClientServer {
	
	private final Log logger = LogFactory.getLog(ClientServerImpl.class);
	
	private final Map<SelectionKey, ArrayBlockingQueue<ByteBuffer>> replyMap;
	
	private final CountDownLatch expectedClientContainersMonitor;
	
	private final int expectedClientContainers;

	private volatile int replyWaitTimeout;
	
	
	/**
	 * Will create an instance of this server using host name as 
	 * {@link InetAddress#getLocalHost()#getHostAddress()} and an 
	 * ephemeral port selected by the system.
	 */
	public ClientServerImpl(int expectedClientContainers){
		this(getDefaultAddress(), expectedClientContainers);
	}
	
	/**
	 * Constructs this ClientServer with specified 'address' and 'expectedClientContainers'.
	 * The 'expectedClientContainers' represents the amount of expected {@link ApplicationContainerClientImpl}s to be connected
	 * with this ClientServer. 
	 * @param address
	 * @param expectedClientContainers
	 */
	public ClientServerImpl(InetSocketAddress address, int expectedClientContainers) {
		super(address, true);
		Assert.isTrue(expectedClientContainers > 0, "'expectedClientContainers' must be > 0");
		this.expectedClientContainers = expectedClientContainers;
		this.replyMap = new HashMap<>();
		this.replyWaitTimeout = 60;
		this.expectedClientContainersMonitor = new CountDownLatch(expectedClientContainers);
	}
	
	/* (non-Javadoc)
	 * @see oz.hadoop.yarn.api.net.ClientServer#setReplyWaitTimeout(int)
	 */
	@Override
	public void setReplyWaitTimeout(int replyWaitTimeout) {
		this.replyWaitTimeout = replyWaitTimeout;
	}
	
	/* (non-Javadoc)
	 * @see oz.hadoop.yarn.api.net.ClientServer#awaitAllClients(long)
	 */
	@Override
	public boolean awaitAllClients(long timeOutInSeconds) {
		try {
			return this.expectedClientContainersMonitor.await(timeOutInSeconds, TimeUnit.SECONDS);
		} 
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException(e);
		}
	}

	/**
	 * Performs message exchange session (request/reply) with the client identified by a {@link SelectionKey}.
	 * Message data is contained in 'buffer' parameter. 
	 * 
	 * The actual exchange happens asynchronously, so the return type is {@link Future} and return immediately.
	 * 
	 * @param selectionKey
	 * @param buffer
	 * @return
	 */
	Future<ByteBuffer> exchangeWith(final SelectionKey selectionKey,  ByteBuffer buffer) {
		this.doWrite(selectionKey, buffer);
		Future<ByteBuffer> result = this.getExecutor().submit(new Callable<ByteBuffer>() {
			@Override
			public ByteBuffer call() throws Exception {
				ByteBuffer reply = null;
				try {
					reply = ClientServerImpl.this.replyMap.get(selectionKey).poll(ClientServerImpl.this.replyWaitTimeout, TimeUnit.SECONDS);
					if (logger.isInfoEnabled()){
						logger.info("Receieved reply from " + ((SocketChannel)selectionKey.channel()).getRemoteAddress());
					}
				} 
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					logger.warn("Current thread was interrupted", e);
				}
				return reply;
			}
		});
		return result;
	}
	
	/**
	 * 
	 */
	@Override
	void init() throws IOException{
		ServerSocketChannel channel = (ServerSocketChannel) this.getChannel();
		channel.configureBlocking(false);
		channel.socket().bind(this.getAddress());
		channel.register(this.getSelector(), SelectionKey.OP_ACCEPT);

		if (logger.isInfoEnabled()){
			logger.info("Bound to " + channel.getLocalAddress());
		}
	}
	
	/**
	 * 
	 */
	@Override
	void doAccept(SelectionKey selectionKey) throws IOException {
		ServerSocketChannel serverChannel = (ServerSocketChannel) selectionKey.channel();
		SocketChannel channel = serverChannel.accept();
		
		try {
			if (this.expectedClientContainersMonitor.getCount() == 0){
				logger.warn("Refusing connection from " + channel.getRemoteAddress() + ", since " + 
						this.expectedClientContainers + " ApplicationContainerClients " +
						"identified by 'expectedClientContainers' already connected.");
//				selectionKey.interestOps(0);
				channel.close();
			}
			else {
				channel.configureBlocking(false);
		        SelectionKey clientSelectionKey = channel.register(this.getSelector(), SelectionKey.OP_READ);
		        if (logger.isInfoEnabled()){
		        	logger.info("Accepted conection request from: " + channel.socket().getRemoteSocketAddress());
		        }
				this.replyMap.put(clientSelectionKey, new ArrayBlockingQueue<ByteBuffer>(1));
				this.expectedClientContainersMonitor.countDown();
			}
		} 
		catch (Exception e) {
			throw new IllegalStateException("Failed in accept()", e);
		}
	}
	
	/**
	 * 
	 */
	@Override
	void read(SelectionKey selectionKey, ByteBuffer messageBuffer) throws IOException {
		if (logger.isInfoEnabled()){
    		logger.info("Receiving and enqueuing result from " + ((SocketChannel)selectionKey.channel()).getRemoteAddress());
    	}
    	Queue<ByteBuffer> replyQueue = this.replyMap.get(selectionKey);
    	replyQueue.offer(messageBuffer);
	}
	
	/**
	 * Returns filtered {@link List}} of {@link SelectionKey}s which will contain 
	 * only {@link SelectionKey}s  that belong to {@link SocketChannel} of the client connection 
	 * @return
	 */
	List<SelectionKey> getClientSelectionKeys() {
		List<SelectionKey> selectorKeys = new ArrayList<>();
		for (SelectionKey selectionKey : this.getSelector().keys()) {
			if (selectionKey.channel() instanceof SocketChannel){
				selectorKeys.add(selectionKey);
			}
		}
		return selectorKeys;
	}
	
	/* (non-Javadoc)
	 * @see oz.hadoop.yarn.api.net.ClientServer#getContainerDelegates()
	 */
	@Override
	public ContainerDelegate[] getContainerDelegates(){
		List<ContainerDelegate> containerDelegates = new ArrayList<>();
		for (SelectionKey selectionKey : getClientSelectionKeys()) {
			if (selectionKey.isValid()){
				containerDelegates.add(new ContainerDelegate(selectionKey, this));
			}
		}
		return containerDelegates.toArray(new ContainerDelegate[]{});
	}
	
	/**
	 * 
	 */
	private void doWrite(SelectionKey selectionKey, ByteBuffer buffer) {
		try {
			ByteBuffer message = ByteBufferUtils.merge(ByteBuffer.allocate(4).putInt(buffer.limit() + 4), buffer);
			message.flip();
			selectionKey.attach(message);
			selectionKey.interestOps(SelectionKey.OP_WRITE);
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 
	 */
	private static InetSocketAddress getDefaultAddress(){
		try {
			return new InetSocketAddress(InetAddress.getLocalHost().getCanonicalHostName(), 0);
		} 
		catch (Exception e) {
			throw new IllegalStateException("Failed to get default address", e);
		}
	}
}