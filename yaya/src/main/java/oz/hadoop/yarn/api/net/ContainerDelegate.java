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
	
	private final SelectionKey selectionKey;
	
	private final ClientServerImpl clientServer;
	
	private final Semaphore executionGovernor;
	
	/**
	 * 
	 * @param selectionKey
	 * @param clientServer
	 */
	ContainerDelegate(SelectionKey selectionKey, ClientServerImpl clientServer){
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

		public ExecutionGoverningFuture(Future<ByteBuffer> targetFuture) {
			this.targetFuture = targetFuture;
		}
		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			boolean canceled = this.targetFuture.cancel(mayInterruptIfRunning);
			executionGovernor.release();
			return canceled;
		}

		@Override
		public boolean isCancelled() {
			return this.targetFuture.isCancelled();
		}

		@Override
		public boolean isDone() {
			return this.targetFuture.isDone();
		}

		@Override
		public ByteBuffer get() throws InterruptedException, ExecutionException {
			ByteBuffer replyBuffer = this.targetFuture.get();
			executionGovernor.release();
			return replyBuffer;
		}

		@Override
		public ByteBuffer get(long timeout, TimeUnit unit)
				throws InterruptedException, ExecutionException,
				TimeoutException {
			ByteBuffer replyBuffer = this.targetFuture.get(timeout, unit);
			executionGovernor.release();
			return replyBuffer;
		}
		
	}
}
