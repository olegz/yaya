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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import oz.hadoop.yarn.api.utils.ByteBufferUtils;


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
	
	/**
	 * Connects and instance of ApplicationContainerClient for a provided {@link SocketAddress}
	 * which points to the running server (see {@link ApplicationContainerServerImpl})
	 * 
	 * @param address
	 */
	public ApplicationContainerClientImpl(InetSocketAddress address, ApplicationContainerMessageHandler messageHandler, Runnable onDisconnectTask){
		super(address, false, onDisconnectTask);
		this.messageHandler = messageHandler;
	}
	
	@Override
	void onDisconnect(SelectionKey selectionKey) {
		if (this.listening){
			this.listening = false;
		}
		/*
		 * Keep in mind that shutdown is never initiated by the client (e.g., calling stop(). 
		 * It always initiated by the Server by closing client's socket. This means that the Server manages
		 * details around 'force' vs 'graceful'shutdown. So, in the cases of graceful shutdown,
		 * the client's task will already be complete before the shutdown*() is called so shutdownNow() 
		 * would have the same results as shutdown(). However, for 'force' shutdown, calling shutdown now
		 * will have no effect on the currently running task while shutdownNow will cause the interrupt.
		 */
		this.executor.shutdownNow();
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
		logger.debug("Buffered full message. Releasing to handler");
		this.getExecutor().execute(new MessageProcessor(messageBuffer, selectionKey));
	}
	
	/**
	 * 
	 */
	private class MessageProcessor implements Runnable {
		private final ByteBuffer messageBuffer;
		
		private final SelectionKey selectionKey;
		
		/**
		 * 
		 * @param messageBuffer
		 * @param selectionKey
		 */
		MessageProcessor(ByteBuffer messageBuffer, SelectionKey selectionKey){
			this.messageBuffer = messageBuffer;
			this.selectionKey = selectionKey;
		}
		
		/**
		 * 
		 */
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
