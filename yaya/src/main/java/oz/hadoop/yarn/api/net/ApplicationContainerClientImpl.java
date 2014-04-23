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
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * Implementation of network client to enable communication between the 
 * client that submits YARN application and YARN Application Containers.
 * 
 * NOT A PUBLIC API
 * 
 * @author Oleg Zhurakousky
 *
 */
class ApplicationContainerClientImpl extends AbstractSocketHandler implements ApplicationContainerClient {
	
	private final Log logger = LogFactory.getLog(ApplicationContainerClientImpl.class);
	
	private final ApplicationContainerMessageHandler messageHandler;
	
	private final CountDownLatch lifeCycleLatch;
	
	/**
	 * Connects and instance of ApplicationContainerClient for a provided {@link SocketAddress}
	 * which points to the running server (see {@link ApplicationContainerServerImpl})
	 * 
	 * @param address
	 */
	public ApplicationContainerClientImpl(InetSocketAddress address, ApplicationContainerMessageHandler messageHandler){
		super(address, false);
		this.messageHandler = messageHandler;
		this.lifeCycleLatch = new CountDownLatch(1);
	}
	
	/* (non-Javadoc)
	 * @see oz.hadoop.yarn.api.net.ApplicationContainerClient#awaitShutdown()
	 */
	@Override
	public void awaitShutdown(){
		try {
			this.lifeCycleLatch.await();
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			logger.warn("Interrupted while waiting for shutdown", e);
		}
	}
	
	@Override
	void onDisconnect(SelectionKey selectionKey) {
		if (this.running){
			this.running = false;
			this.executor.shutdown();
			this.lifeCycleLatch.countDown();
			this.messageHandler.onDisconnect();
		}
	}
	
	/**
	 * 
	 */
	@Override
	void init() throws IOException {
		SocketChannel channel = (SocketChannel) this.getChannel();
		
		boolean connected = channel.connect(this.getAddress());
		if (connected){
			channel.configureBlocking(false);

			channel.register(this.getSelector(), SelectionKey.OP_READ);

			if (logger.isInfoEnabled()){
				logger.info("Connected to " + this.getAddress());
			}
		}
		else {
			throw new IllegalStateException("Failed to connect to ClientServer at: " + this.getAddress());
		}
	}
	
	/**
	 * 
	 */
	@Override
	void read(SelectionKey selectionKey, ByteBuffer messageBuffer) throws IOException {
		if (logger.isInfoEnabled()){
			logger.info("Buffered full message. Releasing to handler");
		}
		this.getExecutor().execute(new MessageProcessor(messageBuffer, selectionKey));
	}
	
	/**
	 * 
	 */
	private class MessageProcessor implements Runnable {
		private final ByteBuffer messageBuffer;
		
		private final SelectionKey selectionKey;
		
		MessageProcessor(ByteBuffer messageBuffer, SelectionKey selectionKey){
			this.messageBuffer = messageBuffer;
			this.selectionKey = selectionKey;
		}
		@Override
		public void run() {
			ByteBuffer replyBuffer = ApplicationContainerClientImpl.this.messageHandler.handle(this.messageBuffer);
			ByteBuffer message = ByteBufferUtils.merge(ByteBuffer.allocate(4).putInt(replyBuffer.limit() + 4), replyBuffer);
			message.flip();
			try {
				this.selectionKey.attach(message);
				this.selectionKey.interestOps(SelectionKey.OP_WRITE);
			} 
			catch (CancelledKeyException e) {
				// may happen when server kills connection before receiving a reply
				logger.warn("Selection Key was canceled. No reply will be sent");
			}
		}
	}
}
