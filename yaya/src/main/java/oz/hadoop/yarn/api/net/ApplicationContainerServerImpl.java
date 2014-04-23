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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.Assert;

import oz.hadoop.yarn.api.DataProcessorReplyListener;

/**
 * @author Oleg Zhurakousky
 * 
 */
class ApplicationContainerServerImpl extends AbstractSocketHandler implements ApplicationContainerServer {
	
	private final Log logger = LogFactory.getLog(ApplicationContainerServerImpl.class);
	
	private final ConcurrentHashMap<SelectionKey, ReplyPostProcessor> replyCallbackMap;
	
	private final CountDownLatch expectedClientContainersMonitor;
	
	private final int expectedClientContainers;
	
	private volatile DataProcessorReplyListener replyListener;
	
	private final Map<SelectionKey, ContainerDelegate> containerDelegates;
	
	private final boolean finite;
	
	
	/**
	 * Will create an instance of this server using host name as 
	 * {@link InetAddress#getLocalHost()#getHostAddress()} and an 
	 * ephemeral port selected by the system. 
	 * The 'expectedClientContainers' represents the amount of expected 
	 * {@link ApplicationContainerClientImpl}s to be connected
	 * with this ClientServer. 
	 * 
	 * @param expectedClientContainers
	 * 			expected Application Containers
	 * @param finite
	 * 			whether the YARN application using finite or reusable Application Containers
	 */
	public ApplicationContainerServerImpl(int expectedClientContainers, boolean finite){
		this(getDefaultAddress(), expectedClientContainers, finite);
	}
	
	/**
	 * Constructs this ClientServer with specified 'address'.
	 * The 'expectedClientContainers' represents the amount of expected 
	 * {@link ApplicationContainerClientImpl}s to be connected
	 * with this ClientServer. 
	 * 
	 * @param address
	 * 			the address to bind to
	 * @param expectedClientContainers
	 * 			expected Application Containers
	 * @param finite
	 * 			whether the YARN application using finite or reusable Application Containers
	 */
	public ApplicationContainerServerImpl(InetSocketAddress address, int expectedClientContainers, boolean finite) {
		super(address, true);
		Assert.isTrue(expectedClientContainers > 0, "'expectedClientContainers' must be > 0");
		this.expectedClientContainers = expectedClientContainers;
		this.replyCallbackMap = new ConcurrentHashMap<SelectionKey, ReplyPostProcessor>();
		this.expectedClientContainersMonitor = new CountDownLatch(expectedClientContainers);
		this.containerDelegates = new HashMap<SelectionKey, ContainerDelegate>();
		this.finite = finite;
	}

	
	/* (non-Javadoc)
	 * @see oz.hadoop.yarn.api.net.ClientServer#awaitAllClients(long)
	 */
	@Override
	public boolean awaitAllClients(long timeOutInSeconds) {
		try {
			boolean connected = this.expectedClientContainersMonitor.await(timeOutInSeconds, TimeUnit.SECONDS);
			this.buildContainerDelegates();
			return connected;
		} 
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			logger.warn("Interrupted while waiting for all Application Containers to report");
		}
		return false;
	}

	/**
	 * 
	 */
	@Override
	public int liveContainers() {
		return this.containerDelegates.size();
	}
	
	/**
	 * 
	 */
	@Override
	public void stop(boolean force){
		boolean working = true;
		while (working){
			working = this.containerDelegates.size() > 0;
			int counter = 0;
			int totalSize = this.containerDelegates.size();
			Iterator<SelectionKey> containerSelectionKeys = this.containerDelegates.keySet().iterator();
			while (containerSelectionKeys.hasNext()){
				SelectionKey selectionKey = containerSelectionKeys.next();
				ContainerDelegate containerDelegate = this.containerDelegates.get(selectionKey);
				if (!force){
					if (containerDelegate.available()){
						counter++;
						this.stopContainer(selectionKey, containerSelectionKeys);
					}
				}
				else {
					counter++;
					this.stopContainer(selectionKey, containerSelectionKeys);
				}
				if (counter == totalSize){
					working = false;
				}
			}
			if (working && this.containerDelegates.size() > 0){
				if (logger.isTraceEnabled()){
					logger.trace("Waiting for remaining " + this.containerDelegates.size() + " containers to finish");
				}
				LockSupport.parkNanos(10000000);
			}
		}
		super.stop(force);
	}
	
	/**
	 * 
	 */
	@Override
	public void registerReplyListener(DataProcessorReplyListener replyListener) {
		this.replyListener = replyListener;
	}
	
	/* (non-Javadoc)
	 * @see oz.hadoop.yarn.api.net.ClientServer#getContainerDelegates()
	 */
	@Override
	public ContainerDelegate[] getContainerDelegates(){
		this.awaitAllClients(10);
		return this.containerDelegates.values().toArray(new ContainerDelegate[]{});
	}

	/**
	 * Performs message exchange session (request/reply) with the client identified by the {@link SelectionKey}.
	 * Message data is contained in 'buffer' parameter. 
	 * 
	 * The actual exchange happens asynchronously, so the return type is {@link Future} and returns immediately.
	 * 
	 * @param selectionKey
	 * @param buffer
	 * @return
	 */
	void process(SelectionKey selectionKey,  ByteBuffer buffer, ReplyPostProcessor replyPostProcessor) {
		Assert.isNull(this.replyCallbackMap.putIfAbsent(selectionKey, replyPostProcessor), 
				"Unprocessed callback remains attached to the SelectionKey. This must be a bug. Please report!");
		this.doWrite(selectionKey, buffer);
	}
	
	/**
	 * 
	 * @param selectionKey
	 */
	@Override
	void onDisconnect(SelectionKey selectionKey) {
		try {
			selectionKey.channel().close();
		} 
		catch (Exception e) {
			logger.error("Failed to clode channel");
		}
		this.containerDelegates.remove(selectionKey);
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
		
		if (this.expectedClientContainersMonitor.getCount() == 0){
			logger.warn("Refusing connection from " + channel.getRemoteAddress() + ", since " + 
					this.expectedClientContainers + " ApplicationContainerClients " +
					"identified by 'expectedClientContainers' already connected.");
			channel.close();
		}
		else {
			channel.configureBlocking(false);
	        channel.register(this.getSelector(), SelectionKey.OP_READ);
	        if (logger.isInfoEnabled()){
	        	logger.info("Accepted conection request from: " + channel.socket().getRemoteSocketAddress());
	        }
			this.expectedClientContainersMonitor.countDown();
		}
	}
	
	/**
	 * Unlike the client side the read on the server will happen using receiving thread.
	 */
	@Override
	void read(SelectionKey selectionKey, ByteBuffer replyBuffer) throws IOException {
		ReplyPostProcessor replyCallbackHandler = ((ReplyPostProcessor)this.replyCallbackMap.remove(selectionKey));
		if (logger.isDebugEnabled()){
			logger.debug("Reply received from " + replyCallbackHandler);
		}
		if (this.replyListener != null){
			this.replyListener.onReply(replyBuffer);
		}
		replyCallbackHandler.postProcess(replyBuffer);
		if (this.finite){
			this.onDisconnect(selectionKey);
		}
	}
	
	/**
	 * 
	 */
	void doWrite(SelectionKey selectionKey, ByteBuffer buffer) {
		ByteBuffer message = ByteBufferUtils.merge(ByteBuffer.allocate(4).putInt(buffer.limit() + 4), buffer);
		message.flip();

		selectionKey.attach(message);
		selectionKey.interestOps(SelectionKey.OP_WRITE);
	}
	
	/**
	 * 
	 * @param selectionKey
	 * @param containerSelectionKeys
	 */
	private void stopContainer(SelectionKey selectionKey, Iterator<SelectionKey> containerSelectionKeys){
		try {
			selectionKey.channel().close();
		} 
		catch (Exception e) {
			logger.warn("Failed to close channel", e);
		}
		containerSelectionKeys.remove();
	}
	
	/**
	 * Returns filtered {@link List}} of {@link SelectionKey}s which will contain 
	 * only {@link SelectionKey}s  that belong to {@link SocketChannel} of the client connection 
	 * @return
	 */
	private List<SelectionKey> getClientSelectionKeys() {
		List<SelectionKey> selectorKeys = new ArrayList<>();
		if (this.getSelector().isOpen()){
			for (SelectionKey selectionKey : this.getSelector().keys()) {
				if (selectionKey.isValid() && selectionKey.channel() instanceof SocketChannel){
					selectorKeys.add(selectionKey);
				}
			}
		}
		return selectorKeys;
	}
	
	/**
	 * 
	 */
	private void buildContainerDelegates(){
		for (SelectionKey selectionKey : this.getClientSelectionKeys()) {
			if (selectionKey.isValid() && selectionKey.selector().isOpen()){
				this.containerDelegates.put(selectionKey, new ContainerDelegateImpl(selectionKey, this));
			}
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