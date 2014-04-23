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
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.springframework.util.StringUtils;

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
	
	private final boolean finite;

	protected final Map<String, Object> applicationSpecification;
	
	protected final String applicationName;
	
	protected final PrimitiveImmutableTypeMap applicationContainerSpecification;
	
	protected final YarnConfiguration yarnConfig;
	
	protected final ScheduledExecutorService executor;
	
	private volatile ApplicationContainerServer clientServer;
	
	private T launchResult;
	
	/**
	 * 
	 * @param applicationSpecification
	 */
	@SuppressWarnings("unchecked")
	AbstractApplicationMasterLauncher(Map<String, Object> applicationSpecification){
		this.applicationSpecification = applicationSpecification;
		this.applicationName = (String) this.applicationSpecification.get(YayaConstants.APPLICATION_NAME);
		this.applicationContainerSpecification = 
				new PrimitiveImmutableTypeMap((Map<String, Object>) this.applicationSpecification.get(YayaConstants.CONTAINER_SPEC));
		// we only need YarnConfig locally to launch Application Master. No need to pass it along as application arguments.
		this.yarnConfig  = (YarnConfiguration) this.applicationSpecification.remove(YayaConstants.YARN_CONFIG);
		this.executor = Executors.newScheduledThreadPool(2);
		this.finite = (StringUtils.hasText(this.applicationContainerSpecification.getString(YayaConstants.COMMAND)) ||
				this.applicationContainerSpecification.getString(YayaConstants.CONTAINER_ARG) != null) ? true : false;
	}
	
	/**
	 * 
	 */
	@Override
	public boolean isRunning() {
		// clientServer will be before call to launch
		return this.clientServer != null && this.clientServer.liveContainers() > 0;
	}
	
	/**
	 * 
	 */
	@Override
	public int liveContainers(){
		// clientServer will be before call to launch
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
		
		this.initApplicationContainerServer(applicationContainerCount, finite);
		
		ApplicationId launchedApplication = this.doLaunch(applicationContainerCount);
		if (logger.isInfoEnabled()){
			logger.info("Launched application: " + launchedApplication);
		}
		
		if (logger.isDebugEnabled()){
			logger.debug("Establishing connection with all " + applicationContainerCount + " Application Containers");
		}
		if (!this.clientServer.awaitAllClients(30)){
			shutDown();
			throw new IllegalStateException("Failed to establish connection with all Application Containers. Application shutdown");
		}
		if (logger.isInfoEnabled()){
			logger.info("Established connection with all " + applicationContainerCount + " Application Containers");
		}
		this.launchResult = this.buildLaunchResult(finite);
		return this.launchResult;
	}
	
	/**
	 * 
	 */
	@Override
	public void shutDown() {
		if (this.finite){
			if (this.isRunning()){
				this.close(false);
			}
		}
		else {
			this.close(false);
		}
	}
	
	/**
	 * 
	 */
	@Override
	public void terminate() {
		this.close(true);
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
			this.executor.execute(new ApplicationContainerExecutionMonitor(completionLatch));
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
	private void close(boolean force) {
		if (this.launchResult instanceof DataProcessorImpl){
			((DataProcessorImpl)this.launchResult).stop();
		}
		logger.debug("Shutting down ApplicationContainerServer");
		this.clientServer.stop(force);
		logger.debug("Shutting down executor");
		this.executor.shutdown();
		ApplicationId shutDownApplication = this.doShutDown();
		if (logger.isInfoEnabled()){
			logger.info("Application Master for Application: " + this.applicationName 
					+ " with ApplicationId: " + shutDownApplication + " was shut down.");
		}
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
					ApplicationContainerServer.class.getPackage().getName() + ".ApplicationContainerServerImpl", InetSocketAddress.class, int.class, boolean.class);
			ApplicationContainerServer cs = clCtr.newInstance(address, expectedClientContainerCount, finite);
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
	
	/**
	 * 
	 */
	private class ApplicationContainerExecutionMonitor implements Runnable {
		private final CountDownLatch completionLatch;
		
		ApplicationContainerExecutionMonitor(CountDownLatch completionLatch){
			this.completionLatch = completionLatch;
		}

		@Override
		public void run() {
			logger.trace("Waiting for Application Container completion");
			boolean completed = false;
			try {
				completed = this.completionLatch.await(5, TimeUnit.MILLISECONDS);
			} 
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				logger.warn("Interrupted while waiting for Application Containers to finish", e);
				completed = true;
			}
			if (completed){
				close(false);
			}
			else {
				AbstractApplicationMasterLauncher.this.executor.schedule(this, 10, TimeUnit.MILLISECONDS);
			}
		}
	}
}
