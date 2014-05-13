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
package oz.hadoop.yarn.api.core;

import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import oz.hadoop.yarn.api.ContainerReplyListener;
import oz.hadoop.yarn.api.YayaConstants;
import oz.hadoop.yarn.api.net.ApplicationContainerServer;
import oz.hadoop.yarn.api.net.ContainerDelegate;
import oz.hadoop.yarn.api.net.ReplyPostProcessor;
import oz.hadoop.yarn.api.utils.PrimitiveImmutableTypeMap;
import oz.hadoop.yarn.api.utils.PrintUtils;
import oz.hadoop.yarn.api.utils.ReflectionUtils;

/**
 * INTERNAL API
 * 
 * @author Oleg Zhurakousky
 *
 */
abstract class AbstractApplicationMasterLauncher<T> implements ApplicationMasterLauncher<T> {
	
	private final Log logger = LogFactory.getLog(AbstractApplicationMasterLauncher.class);
	
	protected final boolean finite;

	protected final Map<String, Object> applicationSpecification;
	
	protected final String applicationName;
	
	protected final PrimitiveImmutableTypeMap applicationContainerSpecification;
	
	protected final YarnConfiguration yarnConfig;
	
	protected final ScheduledExecutorService executor;
	
	private final int awaitAllContainersTimeout;
	
	private volatile ApplicationContainerServer clientServer;
	
	private volatile ContainerReplyListener replyListener;
	
	private T launchResult;
	
//	protected boolean running;
	
	
	/**
	 * 
	 * @param applicationSpecification
	 */
	@SuppressWarnings("unchecked")
	AbstractApplicationMasterLauncher(Map<String, Object> applicationSpecification){
		Assert.notNull(applicationSpecification, "'applicationSpecification' must not be null");
		this.applicationSpecification = applicationSpecification;
		this.applicationName = (String) this.applicationSpecification.get(YayaConstants.APPLICATION_NAME);
		this.applicationContainerSpecification = 
				new PrimitiveImmutableTypeMap((Map<String, Object>) this.applicationSpecification.get(YayaConstants.CONTAINER_SPEC));
		// we only need YarnConfig locally to launch Application Master. No need to pass it along as application arguments.
		this.yarnConfig  = (YarnConfiguration) this.applicationSpecification.remove(YayaConstants.YARN_CONFIG);
		this.executor = Executors.newScheduledThreadPool(2);
		this.finite = (StringUtils.hasText(this.applicationContainerSpecification.getString(YayaConstants.COMMAND)) ||
				this.applicationContainerSpecification.getString(YayaConstants.CONTAINER_ARG) != null) ? true : false;
		String cjt = (String) this.applicationSpecification.get(YayaConstants.CLIENTS_JOIN_TIMEOUT);
		this.awaitAllContainersTimeout = StringUtils.hasText(cjt) ? Integer.parseInt(cjt) : 30;
	}
	
	/**
	 * 
	 */
	@Override
	public void registerReplyListener(ContainerReplyListener replyListener) {
		this.replyListener = replyListener;
	}
	
	/**
	 * 
	 */
	@Override
	public boolean isRunning() {
		
		return this.clientServer != null && this.clientServer.isRunning();
//		
	}
	
//	@Override
//	public boolean isTerminated() {
//		return !this.running;
//	}
	
	/**
	 * 
	 */
	@Override
	public int liveContainers(){
		return this.clientServer == null ? 0 : this.clientServer.liveContainers();
	}
	
	/**
	 * 
	 */
	@Override
	public T launch() {
		
		if (logger.isDebugEnabled()){
			logger.debug("Launching application '" + this.applicationName + "' with the following spec: \n**********\n" + 
					PrintUtils.prettyMap(this.applicationSpecification) + "\n**********");
		}		

		int applicationContainerCount = this.applicationContainerSpecification.getInt(YayaConstants.CONTAINER_COUNT);
		
		this.initApplicationContainerServer(applicationContainerCount, this.finite);
		
		if (this.replyListener != null){
			this.clientServer.registerReplyListener(this.replyListener);
		}
		
		this.doLaunch(applicationContainerCount);
		
		if (logger.isDebugEnabled()){
			logger.debug("Establishing connection with all " + applicationContainerCount + " Application Containers");
		}
		
		if (logger.isInfoEnabled()){
			logger.info("Awaiting " + this.awaitAllContainersTimeout + " seconds for all Application Containers to report");
		}
		
		if (!this.clientServer.awaitAllClients(this.awaitAllContainersTimeout)){
			this.clientServer.stop(true);
			throw new IllegalStateException("Failed to establish connection with all Application Containers. Application shutdown");
		}
//		this.running = true;
		
		if (logger.isInfoEnabled()){
			logger.info("Established connection with all " + applicationContainerCount + " Application Containers");
		}
		this.launchResult = this.buildLaunchResult(this.finite);
		
		return this.launchResult;
	}
	
	/**
	 * 
	 */
	public void shutDown() {
		if (this.clientServer != null){
			this.clientServer.stop(false);
		}
	}
	
	/**
	 * 
	 */
	@Override
	public void terminate() {
		if (this.clientServer != null){
			this.clientServer.stop(true);
		}
	}
	
	/**
	 * 
	 * @param finite
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private T buildLaunchResult(boolean finite){
		T returnValue = null;

		if (!finite){
			DataProcessorImpl dp = new DataProcessorImpl(this.clientServer);
			return (T) dp;
		}
		else {
			final ContainerDelegate[] containerDelegates = clientServer.getContainerDelegates();
			if (containerDelegates.length == 0){
				logger.warn("ClientServer returned 0 ContainerDelegates");
			}
			else {
				if (logger.isDebugEnabled()){
					logger.debug("Sending start to Application Containers");
				}
				final CountDownLatch completionLatch = new CountDownLatch(containerDelegates.length);
				for (ContainerDelegate containerDelegate : containerDelegates) {
					containerDelegate.process(ByteBuffer.wrap("START".getBytes()), new ReplyPostProcessor() {	
						@Override
						public void doProcess(ByteBuffer reply) {
							completionLatch.countDown();
						}
					});
				}
			}
//			if (this.clientServer.isRunning()){
//				/*
//				 * By initiating a graceful shutdown we simply sending a signal
//				 * for an application to stop once complete.
//				 */
//				this.executor.execute(new Runnable() {		
//					@Override
//					public void run() {
//						clientServer.stop(false);
//					}
//				});
//			}
			returnValue = null;
		}
		return returnValue;
	}
	
	/**
	 * 
	 */
	private void initApplicationContainerServer(int applicationContainerCount, boolean finite){
		this.clientServer = this.buildClientServer(applicationContainerCount, finite);	
		InetSocketAddress address = clientServer.start();
		this.applicationSpecification.put(YayaConstants.CLIENT_HOST, address.getHostName());
		this.applicationSpecification.put(YayaConstants.CLIENT_PORT, address.getPort());
	}
	
	/**
	 * 
	 * @param force
	 */
	private void close() {
		if (this.launchResult instanceof DataProcessorImpl){
			((DataProcessorImpl)this.launchResult).stop();
		}
		logger.debug("Shutting down executor");
		this.executor.shutdown();
		ApplicationId shutDownApplication = this.doShutDown();
//		this.executor.shutdown();
		if (logger.isInfoEnabled()){
			logger.info("Application Master for Application: " + this.applicationName 
					+ " with ApplicationId: " + shutDownApplication + " was shut down.");
		}
//		this.running = false;
	}
	
	/**
	 * 
	 * @param launchApplicationMaster
	 */
	abstract ApplicationId doLaunch(int launchApplicationMaster);
	
	/**
	 * 
	 */
	abstract ApplicationId doShutDown();
	
	/**
	 * 
	 */
	private ApplicationContainerServer buildClientServer(int expectedClientContainerCount, boolean finite){
		try {
			InetSocketAddress address = this.buildSocketAddress();
			Constructor<ApplicationContainerServer> clCtr = ReflectionUtils.getInvocableConstructor(
					ApplicationContainerServer.class.getPackage().getName() + ".ApplicationContainerServerImpl", InetSocketAddress.class, int.class, boolean.class, Runnable.class);
			
			ApplicationContainerServer cs = clCtr.newInstance(address, expectedClientContainerCount, finite, new Runnable() {	
				@Override
				public void run() {
					if (logger.isInfoEnabled()){
						logger.info("Shutting down Application Master");
					}
					close();
				}
			});
			return cs;
		} 
		catch (Exception e) {
			throw new IllegalStateException("Failed to create ClientServer instance", e);
		}
	}
	
	/**
	 * 
	 */
	private InetSocketAddress buildSocketAddress(){
		try {
			InetSocketAddress address = new InetSocketAddress(InetAddress.getLocalHost().getCanonicalHostName(), 0);
			return address;
		} 
		catch (UnknownHostException e) {
			throw new IllegalStateException("Failed to get SocketAddress", e);
		}
	}
}
