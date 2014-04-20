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

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import oz.hadoop.yarn.api.YarnApplication;

/**
 * ContainerDelegate is a local proxy to the remote Application Container. 
 * It is created and returned in a form of an array during the invocation of {@link YarnApplication#launch()}
 * method essentially signifying that the Yarn application have been deployed successfully 
 * and interaction with Application Containers through simple messaging 
 * (see {@link ContainerDelegate#exchange(ByteBuffer)}) method can commence.
 * 
 * @author Oleg Zhurakousky
 */
public class ContainerDelegate {
	private final Log logger = LogFactory.getLog(ContainerDelegate.class);
	
	private final SelectionKey selectionKey;
	
	private final ApplicationContainerServerImpl clientServer;
	
	private final Semaphore executionGovernor;
	
	/**
	 * 
	 * @param selectionKey
	 * @param clientServer
	 */
	ContainerDelegate(SelectionKey selectionKey, ApplicationContainerServerImpl clientServer){
		this.selectionKey = selectionKey;
		this.clientServer = clientServer;
		this.executionGovernor = new Semaphore(1);
	}
	
	/**
	 * Returns the host name of the active Application Container represented by this 
	 * ContainerDelegate.
	 */
	public String getHost(){
		try {
			return ((InetSocketAddress)((SocketChannel)this.selectionKey.channel()).getLocalAddress()).getHostName();
		} 
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}
	
	/**
	 * Will attempt to terminate the Application Container represented by this delegate
	 * immediately.
	 */
	public void terminate() {
		
	}
	
	/**
	 * Will attempt to shut down the Application Container gracefully waiting for
	 * it to complete its task
	 */
	public void shutDown(){

	}
	
	/**
	 * Will attempt to shut down the Application Container gracefully waiting for
	 * it to complete its task within specified timeout. It will return 'true'
	 * if task completed and Application Container was shut down and 'false'
	 * if it wasn't after which you may choose to terminate it by calling {@link #terminate()}
	 * method.
	 */
	public boolean shutDown(int timeout){
		return false;
	}
	
	/**
	 * Will return 'true' if the Application Container represented by this
	 * delegate is running and false if its not (shut down or terminated).
	 * 
	 * @return
	 */
	public boolean isRunning(){
		return false;
	}
	
	/**
	 * A general purpose request/reply method which allowing simple messages in the form of the 
	 * {@link ByteBuffer}s to be exchanged with the Application Container represented by this ContainerDelegate.
	 */
	public Future<ByteBuffer> exchange(ByteBuffer messageBuffer){
		try {
			this.executionGovernor.acquire();
			return new ExecutionGoverningFuture(clientServer.exchangeWith(this.selectionKey, messageBuffer));
		} 
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException(e);
		}
	}
	
	/**
	 * Ensures that only one request/reply session at a time with AC
	 *
	 */
	private class ExecutionGoverningFuture implements Future<ByteBuffer> {
		private final Future<ByteBuffer> targetFuture;
		/**
		 * 
		 */
		public ExecutionGoverningFuture(Future<ByteBuffer> targetFuture) {
			this.targetFuture = targetFuture;
		}
		
		/**
		 * 
		 */
		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			try {
				boolean canceled = this.targetFuture.cancel(mayInterruptIfRunning);
				ContainerDelegate.this.executionGovernor.release();
				return canceled;
			} 
			finally {
				try {
					ContainerDelegate.this.selectionKey.channel().close();
				} 
				catch (Exception e) {
					logger.warn("Exception during channel close", e);
				}
			}
		}

		/**
		 * 
		 */
		@Override
		public boolean isCancelled() {
			return this.targetFuture.isCancelled();
		}

		@Override
		public boolean isDone() {
			return this.targetFuture.isDone();
		}

		/**
		 * 
		 */
		@Override
		public ByteBuffer get() throws InterruptedException, ExecutionException {
			ByteBuffer replyBuffer = this.targetFuture.get();
			ContainerDelegate.this.executionGovernor.release();
			return replyBuffer;
		}

		/**
		 * 
		 */
		@Override
		public ByteBuffer get(long timeout, TimeUnit unit)
				throws InterruptedException, ExecutionException, TimeoutException {
			ByteBuffer replyBuffer = this.targetFuture.get(timeout, unit);
			ContainerDelegate.this.executionGovernor.release();
			return replyBuffer;
		}	
	}
}
