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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
import oz.hadoop.yarn.api.net.ShutdownAware;
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

	protected final Map<String, Object> applicationSpecification;
	
	protected final String applicationName;
	
	protected final PrimitiveImmutableTypeMap applicationContainerSpecification;
	
	protected final YarnConfiguration yarnConfig;
	
	protected final ScheduledExecutorService executor;
	
	private volatile ApplicationContainerServer clientServer;
	
	@SuppressWarnings("unchecked")
	AbstractApplicationMasterLauncher(Map<String, Object> applicationSpecification){
		this.applicationSpecification = applicationSpecification;
		this.applicationName = (String) this.applicationSpecification.get(YayaConstants.APPLICATION_NAME);
		this.applicationContainerSpecification = 
				new PrimitiveImmutableTypeMap((Map<String, Object>) this.applicationSpecification.get(YayaConstants.CONTAINER_SPEC));
		// we only need YarnConfig locally to launch Application Master. No need to pass it along as application arguments.
		this.yarnConfig  = (YarnConfiguration) this.applicationSpecification.remove(YayaConstants.YARN_CONFIG);
		this.executor = Executors.newScheduledThreadPool(2);
	}
	
	/**
	 * 
	 */
	@Override
	public boolean isRunning() {
		boolean running = false;
		if (this.clientServer != null){
			int connectedDelegates = this.clientServer.getContainerDelegates().length;
			if (connectedDelegates > 0){
				running = true;
			}
		}
		return running;
	}
	
	/**
	 * 
	 */
	@Override
	public int liveContainers(){
		int liveContainers = 0;
		if (this.clientServer != null){
			liveContainers = this.clientServer.getContainerDelegates().length;
		}
		return liveContainers;
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
		
		boolean reusableContainer = (StringUtils.hasText(this.applicationContainerSpecification.getString(YayaConstants.COMMAND)) ||
				this.applicationContainerSpecification.getString(YayaConstants.CONTAINER_ARG) != null) ? false : true;
		
		this.initApplicationContainerServer(applicationContainerCount);
		
		this.doLaunch(applicationContainerCount);
		
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
		T launchResult = this.buildLaunchResult(reusableContainer);
		return launchResult;
	}
	
	/**
	 * 
	 */
	@Override
	public void shutDown() {
		if (this.isRunning()){
			this.close();
		}
	}
	
	/**
	 * 
	 * @param reusableContainer
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private T buildLaunchResult(boolean reusableContainer){
		T returnValue = null;

		if (reusableContainer){
			returnValue = (T) this.clientServer.getContainerDelegates();
		}
		else {
			final ContainerDelegate[] containerDelegates = clientServer.getContainerDelegates();
			if (logger.isDebugEnabled()){
				logger.debug("Sending start to Application Containers");
			}
			final List<Future<ByteBuffer>> results = new ArrayList<>();
			for (ContainerDelegate containerDelegate : containerDelegates) {
				results.add(containerDelegate.exchange(ByteBuffer.wrap("START".getBytes())));
			}
	
			this.executor.execute(new ApplicationContainerExecutionMonitor(results));
			
			returnValue = null;
		}
		return returnValue;
	}
	
	/**
	 * 
	 */
	private void initApplicationContainerServer(int applicationContainerCount){
		this.clientServer = this.buildClientServer(applicationContainerCount);	
		InetSocketAddress address = clientServer.start();
		this.applicationSpecification.put(YayaConstants.CLIENT_HOST, address.getHostName());
		this.applicationSpecification.put(YayaConstants.CLIENT_PORT, address.getPort());
	}
	
	private void close(){
		logger.debug("Shutting down ApplicationContainerServer");
		this.clientServer.stop();
		logger.debug("Shutting down executor");
		this.executor.shutdownNow();
		ApplicationId shutDownApplication = this.doShutDown();
		if (logger.isInfoEnabled()){
			logger.info("Application Master for Application: " + this.applicationName 
					+ " with ApplicationId: " + shutDownApplication + " was shut down.");
		}
	}
	
	/**
	 * 
	 */
	@Override
	public void preShutdown(){
		// noop
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
	private ApplicationContainerServer buildClientServer(int expectedClientContainerCount){
		try {
			InetSocketAddress address = this.buildSocketAddress();
			Constructor<ApplicationContainerServer> clCtr = ReflectionUtils.getInvocableConstructor(
					ApplicationContainerServer.class.getPackage().getName() + ".ApplicationContainerServerImpl", InetSocketAddress.class, int.class, ShutdownAware.class);
			ApplicationContainerServer cs = clCtr.newInstance(address, expectedClientContainerCount, this);
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
		private final List<Future<ByteBuffer>> results;
		
		private volatile int completedContainers;
		
		ApplicationContainerExecutionMonitor(List<Future<ByteBuffer>> results){
			this.results = results;
		}

		@Override
		public void run() {
			logger.trace("Waiting for Application Container completion");
			for (Future<ByteBuffer> future : results) {
				if (future.isDone()){
					logger.debug("Completed contaiers: " + completedContainers);
					this.completedContainers++;
					future.cancel(false);
				}
			}
			if (this.completedContainers < this.results.size()){
				AbstractApplicationMasterLauncher.this.executor.schedule(this, 100, TimeUnit.MILLISECONDS);
			}
			else {
				close();
			}
		}
	}
}
