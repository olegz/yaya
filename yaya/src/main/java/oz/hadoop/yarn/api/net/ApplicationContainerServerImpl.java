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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.Assert;

import oz.hadoop.yarn.api.ContainerReplyListener;
import oz.hadoop.yarn.api.utils.ByteBufferUtils;

/**
 * @author Oleg Zhurakousky
 * 
 */
class ApplicationContainerServerImpl extends AbstractSocketHandler implements ApplicationContainerServer {
	
	private final Log logger = LogFactory.getLog(ApplicationContainerServerImpl.class);
	
	private final ConcurrentHashMap<SelectionKey, ReplyPostProcessor> replyCallbackMap;
	
	private final CountDownLatch expectedClientContainersMonitor;
	
	private final int expectedClientContainers;
	
	private volatile ContainerReplyListener replyListener;
	
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
	public ApplicationContainerServerImpl(int expectedClientContainers, boolean finite, Runnable onDisconnectProcess){
		this(getDefaultAddress(), expectedClientContainers, finite, onDisconnectProcess);
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
	public ApplicationContainerServerImpl(InetSocketAddress address, int expectedClientContainers, boolean finite, Runnable onDisconnectTask) {
		super(address, true, onDisconnectTask);
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
	 * Will stop this server, closing all the remaining connection to Application Container 
	 * clients waiting if necessary. 
	 * Closing such connection will force Application Container clients to stop essentially 
	 * stopping and exiting Application Containers, thus stopping the entire application.
	 * 
	 * @param force
	 * 		boolean flag indicating if this application should be terminated immediately or 
	 * 		should it shut down gracefully allowing currently running Application Container 
	 * 		processes to finish.
	 */
	@Override
	void doStop(boolean force) {
		// Need to make a copy so we can remove entries without affecting the global map so it could be cleaned through normal disconnect procedure
		Map<SelectionKey, ContainerDelegate> cDelegates = new HashMap<>(this.containerDelegates);
		boolean working = cDelegates.size() > 0;
		while (working) {
			Iterator<SelectionKey> containerSelectionKeys = cDelegates.keySet().iterator();
			while (containerSelectionKeys.hasNext()){
				SelectionKey selectionKey = containerSelectionKeys.next();
				ContainerDelegate containerDelegate = cDelegates.get(selectionKey);
				containerDelegate.suspend();
				if (!force){
					if (containerDelegate.available()){
						containerSelectionKeys.remove();
					}
				}
				else {
					containerSelectionKeys.remove();
				}
			}
			working = cDelegates.size() > 0;
			if (working){
				if (logger.isTraceEnabled()){
					logger.trace("Waiting for remaining " + cDelegates.size() + " containers to finish");
				}
				LockSupport.parkNanos(10000000);
			}
			else {
				this.containerDelegates.clear();
			}
		}
	}
	
	/**
	 * 
	 */
	@Override
	public void registerReplyListener(ContainerReplyListener replyListener) {
		this.replyListener = replyListener;
	}
	
	/* (non-Javadoc)
	 * @see oz.hadoop.yarn.api.net.ClientServer#getContainerDelegates()
	 */
	@Override
	public ContainerDelegate[] getContainerDelegates(){
		if (this.awaitAllClients(60)){
			return this.containerDelegates.values().toArray(new ContainerDelegate[]{});
		}
		throw new IllegalStateException("Can't establish connection with all Application Containers");
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
		this.containerDelegates.remove(selectionKey);
		if (this.containerDelegates.size() == 0){
			if (this.listening){
				this.listening = false;
			}
		}
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
	        SelectionKey clientSelectionKey = channel.register(this.getSelector(), SelectionKey.OP_READ);
	        if (logger.isInfoEnabled()){
	        	logger.info("Accepted conection request from: " + channel.socket().getRemoteSocketAddress());
	        }
	        this.containerDelegates.put(clientSelectionKey, new ContainerDelegateImpl(clientSelectionKey, this));
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
			logger.debug("Reply received from " + ((SocketChannel)selectionKey.channel()).getRemoteAddress());
		}
		if (this.replyListener != null){
			this.replyListener.onReply(replyBuffer);
		}
		replyCallbackHandler.postProcess(replyBuffer);
		
		if (this.finite) {
			selectionKey.cancel();
			selectionKey.channel().close();
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