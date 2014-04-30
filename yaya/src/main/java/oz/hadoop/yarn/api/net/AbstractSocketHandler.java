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
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ByteChannel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NetworkChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.LockSupport;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.Assert;

import oz.hadoop.yarn.api.utils.ByteBufferUtils;

/**
 * Base class to implement network Client and Server to enable communication between the 
 * client that submits YARN application and YARN Application Containers.
 * 
 * NOT A PUBLIC API
 * 
 * @author Oleg Zhurakousky
 *
 */
abstract class AbstractSocketHandler implements SocketHandler {
	
	private final Log logger = LogFactory.getLog(AbstractSocketHandler.class);
	
	private final Class<? extends AbstractSocketHandler> thisClass;
	
	private final InetSocketAddress address;
	
	private final Selector selector;
	
	private final Runnable listenerTask;
	
	protected final Runnable onDisconnectTask;
	
	protected final NetworkChannel channel;
	
	private final ByteBuffer readingBuffer;
	
	private final ByteBufferPool bufferPoll;
	
	private final CountDownLatch livelinessLatch;
	
	protected final ExecutorService executor;
	
	private volatile boolean running;
	
	volatile boolean listening;
	
	

	/**
	 * 
	 * @param address
	 * @param server
	 */
	public AbstractSocketHandler(InetSocketAddress address, boolean server, Runnable onDisconnectTask){
		Assert.notNull(address);
		this.onDisconnectTask = onDisconnectTask;
		this.address = address;
		this.executor = Executors.newCachedThreadPool();
		this.listenerTask = new ListenerTask();
		this.readingBuffer = ByteBuffer.allocate(16384);
		this.bufferPoll = new ByteBufferPool();
		try {
			this.selector = Selector.open();
			this.channel = server ? ServerSocketChannel.open() : SocketChannel.open();
			if (logger.isDebugEnabled()){
				logger.debug("Created instance of " + this.getClass().getSimpleName());
			}
		} 
		catch (Exception e) {
			throw new IllegalStateException("Failed to create an instance of the ApplicationContainerClient", e);
		}
		this.thisClass = this.getClass();
		this.livelinessLatch = new CountDownLatch(1);
	}
	
	/**
	 * Will asynchronously start this socket handler (client or server)
	 * returning the {@link SocketAddress} which represents server address for 
	 * clients and bind address for server. See {@link ApplicationContainerServerImpl} and {@link ApplicationContainerClientImpl}
	 */
	public InetSocketAddress start() {
		InetSocketAddress serverAddress = this.address;
		try {
			if (!this.listening) {
				this.init();
				
				this.listening = true;
				this.executor.execute(this.listenerTask);
				if (logger.isDebugEnabled()){
					logger.debug("Started listener for " + AbstractSocketHandler.this.getClass().getSimpleName());
				}	
				serverAddress = (InetSocketAddress) this.channel.getLocalAddress();
			}
			this.running = true;
		} 
		catch (ClosedChannelException cce){
			this.listening = false;
			logger.warn("Failed to connect to the Server becouse it rejected the connection. Most likely reason: Illegal/Unexpected" +
					"client was trying to join");
		}
		catch (Exception e) {
			throw new IllegalStateException("Failed to start " + this.getClass().getName(), e);
		}
		return serverAddress;
	}
	
	/**
	 * Will stop and disconnect this socket handler blocking until this handler is completely stopped
	 * 
	 * @param force
	 * 		This method performs shutdown by allowing currently running tasks proxied through {@link ContainerDelegate}
	 *      to finish while not allowing new tasks to be submitted. If you want to interrupt and kill currently running tasks
	 *      set this parameter to 'true'.
	 * 
	 */
	public void stop(boolean force){
		this.doStop(force);
		/*
		 * Close this handler's channel and all connected channels if any by setting running to false
		 * thus exiting the ListenerLoop
		 */
		this.listening = false;
		if (force){
			this.executor.shutdownNow();
		}
		else {
			this.executor.shutdown();
		}
		// Wait for the ListenerTask to complete (may be change to latch)
		while (this.running){
			LockSupport.parkNanos(10000000);
		}
	}
	
	@Override
	public void awaitShutdown() {
		try {
			this.livelinessLatch.await();
		} 
		catch (InterruptedException e) {
			logger.warn("Interrupted while waiting for shutdown");
		}
	}
	
	/**
	 * 
	 * @return
	 */
	@Override
	public boolean isRunning(){
		return this.running;
	}
	
	/**
	 * 
	 * @param force
	 */
	void doStop(boolean force) {
		// noop
	}
	
	/**
	 * Returns an instance of {@link NetworkChannel} representing this socket handler
	 * The actual instance will be {@link ServerSocketChannel} if this socket container is a server
	 * and {@link SocketChannel} if this socket container is client.
	 * 
	 */
	NetworkChannel getChannel(){
		return this.channel;
	}
	
	/**
	 * Returns the {@link SocketAddress} of this socket handler.
	 */
	SocketAddress getAddress(){
		return this.address;
	}
	
	/**
	 * Returns {@link Selector} used by this socket handler
	 * 
	 */
	Selector getSelector(){
		return this.selector;
	}

	/**
	 * Returns {@link ExecutorService} of this socket handler
	 */
	ExecutorService getExecutor(){
		return this.executor;
	}
	
	/**
	 * Initialization method to be implemented by sub-classes of this socket handler.
	 * Typically for bind (server) and connect (client) logic.
	 */
	abstract void init() throws IOException;
	
	/**
	 * Read method to be implemented by the sub-classes of this socket handler.
	 * 
	 * @param selectionKey
	 * 		{@link SelectionKey} for socket from which the data was read
	 * @param buffer
	 * 		Contains data that was read form the Socket
	 * @throws IOException
	 */
	abstract void read(SelectionKey selectionKey, ByteBuffer buffer) throws IOException;
	
	/**
	 * Will be invoked by {@link ListenerTask#accept(SelectionKey)} method after accepting a connection.
	 * 
	 * @param clientSelectionKey
	 * 		{@link SelectionKey} with the socket for the accepted socket connection
	 */
	void doAccept(SelectionKey selectionKey) throws IOException{
		// noop
	}
	
	/**
	 * 
	 */
	abstract void onDisconnect(SelectionKey selectionKey);
	
	/**
	 * Main listener task which will process delegate {@link SelectionKey} selected from the {@link Selector}
	 * to the appropriate processing method (e.g., accept, read, wrote etc.)
	 */
	private class ListenerTask implements Runnable {

		@Override
		public void run() {
			
			try {
				while (AbstractSocketHandler.this.listening){
					this.processSelector(10);	
				}
				
				/*
				 * If the exception is thrown above, below will nevre hapen
				 */
				if (logger.isDebugEnabled()){
					logger.debug(thisClass.getSimpleName() +  " - Exiting Listener loop in " + AbstractSocketHandler.this.getClass().getSimpleName());
				}
				if (AbstractSocketHandler.this.selector.isOpen()){
					for (SelectionKey key : new HashSet<>(AbstractSocketHandler.this.selector.keys())) {
						SocketAddress channelAddress = null;
						SelectableChannel channel = key.channel();
						
						if (channel.isOpen()){
							if (key.channel() instanceof SocketChannel){
								channelAddress = ((SocketChannel)key.channel()).getRemoteAddress();
							}
							else {
								channelAddress = ((ServerSocketChannel)key.channel()).getLocalAddress();
							}
							key.channel().close();
							if (logger.isDebugEnabled()){
								logger.debug(thisClass.getSimpleName() + " - Closed " + channelAddress);
							}
							
						}
					}
					if (AbstractSocketHandler.this.selector.isOpen()){
						AbstractSocketHandler.this.selector.close();
						if (onDisconnectTask != null){
							onDisconnectTask.run();
						}
					}
				}
				
			} 
			catch (AsynchronousCloseException e) {
				logger.warn("Socket has been closed");
			}
			catch (Exception e) {
				throw new IllegalStateException(e);
			}
			finally {
				running = false;
				livelinessLatch.countDown();
			}
		}
		
		/**
		 * 
		 * @param timeout
		 * @throws Exception
		 */
		private void processSelector(int timeout) throws Exception {
			if (AbstractSocketHandler.this.selector.isOpen() && AbstractSocketHandler.this.selector.select(timeout) > 0){	
				Iterator<SelectionKey> keys = AbstractSocketHandler.this.selector.selectedKeys().iterator();
				processKeys(keys);
			}
		}
		
		/**
		 * 
		 * @param keys
		 */
		private void processKeys(Iterator<SelectionKey> keys) throws IOException {
			while (keys.hasNext()) {
                SelectionKey selectionKey = (SelectionKey) keys.next();
                keys.remove();
                if (selectionKey.isValid()) {
                	if (selectionKey.isAcceptable()) {
                        this.accept(selectionKey);
                    }
                    else if (selectionKey.isReadable()) {
                        this.read(selectionKey); 
                    }
                    else if (selectionKey.isConnectable()){
                		this.connect(selectionKey);
                	}
                	else if (selectionKey.isWritable()){
                		this.write(selectionKey);
                	}
                }
            }
		}
		
		/**
		 * 
		 */
		private void accept(SelectionKey selectionKey) throws IOException {
			AbstractSocketHandler.this.doAccept(selectionKey);
		}
		
		/**
		 * 
		 */
		private void connect(SelectionKey selectionKey) throws IOException {
	        SocketChannel clientChannel = (SocketChannel) selectionKey.channel();
	        if (clientChannel.isConnectionPending()){
	        	clientChannel.finishConnect();
	        }
	        clientChannel.register(selector, SelectionKey.OP_READ);
	    }
		
		/**
		 * 
		 * @param selectionKey
		 * @throws IOException
		 */
		private void write(SelectionKey selectionKey) throws IOException {
			ByteBuffer writeBuffer = (ByteBuffer) selectionKey.attachment();
			if (writeBuffer == null){
				throw new IllegalStateException("Failed to get write buffer from " + selectionKey);
			}
			selectionKey.attach(null);
			((ByteChannel)selectionKey.channel()).write(writeBuffer);
			try {
				selectionKey.channel().register(selector, SelectionKey.OP_READ);
			} 
			catch (ClosedChannelException e) {
				logger.warn("Socket was prematurely closed");
			}
		}
		
		/**
		 * 
		 */
		private void read(SelectionKey selectionKey) throws IOException {
			AbstractSocketHandler.this.readingBuffer.clear();
	        SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
	      
	        int count = -1;
	        boolean finished = false;
			while (!finished && (count = socketChannel.read(AbstractSocketHandler.this.readingBuffer)) > -1){
				ByteBuffer messageBuffer = (ByteBuffer) selectionKey.attachment();
				if (messageBuffer == null) { // new message
		    		messageBuffer = bufferPoll.poll();
					selectionKey.attach(messageBuffer);
				}
				AbstractSocketHandler.this.readingBuffer.flip();
				if (AbstractSocketHandler.this.readingBuffer.limit() > 0){
					if (logger.isTraceEnabled()){
						logger.trace(AbstractSocketHandler.this.getClass().getName() + " - Received data message with " + readingBuffer.limit() + " bytes");
			    	}
					messageBuffer = ByteBufferUtils.merge(messageBuffer, AbstractSocketHandler.this.readingBuffer);
				}
				AbstractSocketHandler.this.readingBuffer.clear();
				
				int expectedMessageLength = messageBuffer.getInt(0);

				if (expectedMessageLength == messageBuffer.position()){
					selectionKey.attach(null);
					messageBuffer.flip();
					byte[] message = new byte[messageBuffer.limit()-4];
					messageBuffer.position(4);
					messageBuffer.get(message);
					AbstractSocketHandler.this.bufferPoll.release(messageBuffer);
					AbstractSocketHandler.this.read(selectionKey, ByteBuffer.wrap(message));
					finished = true;
				}
			}
	        
	        if (count < 0) {
	            if (logger.isDebugEnabled()){
	            	logger.debug(thisClass.getSimpleName() + " - Connection closed by: " + socketChannel.socket().getRemoteSocketAddress());
	            }
	            socketChannel.close();
	            AbstractSocketHandler.this.onDisconnect(selectionKey);
	        }
	    }
	}
}
