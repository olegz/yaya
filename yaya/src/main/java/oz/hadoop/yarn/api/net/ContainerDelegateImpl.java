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
import java.util.concurrent.Semaphore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import oz.hadoop.yarn.api.YarnApplication;

/**
 * ContainerDelegate is a local proxy to the remote Application Container. 
 * It is created and returned in a form of an array during the invocation of {@link YarnApplication#launch()}
 * method essentially signifying that the Yarn application have been deployed successfully 
 * and interaction with Application Containers through simple messaging 
 * method can commence (see {@link #process(ByteBuffer, ReplyPostProcessor)).
 * 
 * @author Oleg Zhurakousky
 */
class ContainerDelegateImpl implements ContainerDelegate {
	private final Log logger = LogFactory.getLog(ContainerDelegateImpl.class);
	
	private final SelectionKey selectionKey;
	
	private final ApplicationContainerServerImpl clientServer;
	
	private final Semaphore executionGovernor;
	
	private final InetSocketAddress applicationContainerAddress;
	
	/**
	 * 
	 * @param selectionKey
	 * @param clientServer
	 */
	ContainerDelegateImpl(SelectionKey selectionKey, ApplicationContainerServerImpl clientServer){
		this.selectionKey = selectionKey;
		this.clientServer = clientServer;
		this.executionGovernor = new Semaphore(1);
		try {
			this.applicationContainerAddress = (InetSocketAddress) ((SocketChannel)this.selectionKey.channel()).getLocalAddress();
		} 
		catch (Exception e) {
			throw new IllegalArgumentException("Failed to get Applicatioin Container's address", e);
		}
	}
	
	/* (non-Javadoc)
	 * @see oz.hadoop.yarn.api.net.ContainerDelegate#getHost()
	 */
	@Override
	public InetSocketAddress getHost(){
		return this.applicationContainerAddress;
	}
	
	/* (non-Javadoc)
	 * @see oz.hadoop.yarn.api.net.ContainerDelegate#exchange(java.nio.ByteBuffer)
	 */
	@Override
	public void process(ByteBuffer data, ReplyPostProcessor replyPostProcessor) {
		try {
			this.executionGovernor.acquire(); 
			replyPostProcessor.setContainerDelegate(this);
			this.clientServer.process(selectionKey, data, replyPostProcessor);
		} 
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			logger.error("Interrupted while waiting for aquiring execution permit for " + this);
		}
	}
	
	/**
	 * 
	 */
	@Override
	public void release() {
		this.executionGovernor.release();
	}
	
	@Override
	public boolean available() {
		return this.executionGovernor.availablePermits() == 1;
	}
	
	/**
	 * 
	 */
	@Override
	public String toString(){
		return "CD:[" + this.getHost().getAddress().getHostAddress() + "]"; 
	}
}
