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

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.util.Records;

import oz.hadoop.yarn.api.YayaConstants;
import oz.hadoop.yarn.api.utils.PrimitiveImmutableTypeMap;

/**
 * @author Oleg Zhurakousky
 *
 */
public abstract class AbstractApplicationContainerLauncher implements ApplicationContainerLauncher {
	
	private final Log logger = LogFactory.getLog(AbstractApplicationContainerLauncher.class);
	
	protected final PrimitiveImmutableTypeMap applicationSpecification;
	
	protected final PrimitiveImmutableTypeMap containerSpecification;
	
	protected final int containerCount;
	
	protected final AtomicInteger livelinessBarrier;
	
	protected final AtomicInteger containerStarts;
	
	protected final ApplicationMasterCallbackSupport callbackSupport;
	
	protected volatile Throwable error;
	
	protected volatile boolean started;
	
	private final CountDownLatch containerStartBarrier;
	
	private final int containerStartAwaitTime;
	
	private final CountDownLatch containerFinishBarrier;
	
	/**
	 * 
	 * @param applicationSpecification
	 * @param containerSpecification
	 */
	public AbstractApplicationContainerLauncher(PrimitiveImmutableTypeMap applicationSpecification, PrimitiveImmutableTypeMap containerSpecification){
		this.applicationSpecification = applicationSpecification;
		this.containerSpecification = containerSpecification;
		this.containerCount = this.containerSpecification.getInt(YayaConstants.CONTAINER_COUNT);
		this.livelinessBarrier = new AtomicInteger();
		this.containerStarts = new AtomicInteger();
		this.callbackSupport = new ApplicationMasterCallbackSupport();
		this.containerStartBarrier = new CountDownLatch(this.containerCount);
		this.containerStartAwaitTime = 60000; // milliseconds
		this.containerFinishBarrier = new CountDownLatch(this.containerCount);
	}

	/**
	 * 
	 */
	@Override
	public void launch() {
		try {
			this.doLaunch();
			// Wait till all containers are finished. clean up and exit.
			logger.info("Waiting for Application Containers to finish");
		
			boolean success = this.containerStartBarrier.await(this.containerStartAwaitTime, TimeUnit.MILLISECONDS);
			if (success){
				if (this.error != null){
					this.error = new IllegalStateException("Application Contanietr failed", this.error);
				}
				else {
					if (logger.isDebugEnabled()){
						logger.debug("Waiting for container to finish");
					}
					this.containerFinishBarrier.await();
				}
			}
			else {
				this.error = new IllegalStateException("Failed to start " + this.containerCount + " declared containers. Only started " + this.containerStarts);
			}
		} 
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			logger.warn("APPLICATION MASTER's launch thread was interrupted");
		}
		catch (Exception e){
			throw new IllegalStateException("Failed to launch Application Container", e);
		}
		finally {
			this.shutDown();
		}
	}
	
	/**
	 * Will create a {@link ContainerRequest} to the {@link ResourceManager}
	 * to obtain an application container
	 */
	ContainerRequest createConatinerRequest() {
		Priority priority = Records.newRecord(Priority.class);
		priority.setPriority(this.containerSpecification.getInt(YayaConstants.PRIORITY));
		Resource capability = Records.newRecord(Resource.class);
		capability.setMemory(this.containerSpecification.getInt(YayaConstants.MEMORY));
		capability.setVirtualCores(this.containerSpecification.getInt(YayaConstants.VIRTUAL_CORES));

		//TODO support configuration to request resource containers on specific nodes
		ContainerRequest request = new ContainerRequest(capability, null, null, priority);
		if (logger.isDebugEnabled()){
			logger.debug("Created container request: " + request);
		}
		return request;
	}
	
	/**
	 * 
	 * @param containerId
	 */
	void containerStarted(ContainerId containerId) {
		this.livelinessBarrier.incrementAndGet();
		this.containerStarts.incrementAndGet();
		this.containerStartBarrier.countDown();
		if (this.containerStarts.get() == this.containerCount){
			this.started = true;
		}
		if (logger.isInfoEnabled()){
			logger.info("Started Container: " + containerId);
		}
	}
	
	/**
	 * 
	 * @param containerStatus
	 */
	void containerCompleted(ContainerStatus containerStatus) {
		this.livelinessBarrier.decrementAndGet();
		this.containerFinishBarrier.countDown();
		if (containerStatus.getExitStatus() != 0){
			this.error = new IllegalStateException(containerStatus.getDiagnostics());
		}
		if (logger.isInfoEnabled()){
			logger.info("Completed Container: " + containerStatus);
		}
	}
	
	/**
	 * 
	 * @param containers
	 */
	void shutdownRequested(List<Container> containers) {
		logger.info("Shutdown down requested: " + containers);
	}
	
	/**
	 * 
	 * @param t
	 */
	void errorReceived(Throwable t) {
		this.livelinessBarrier.decrementAndGet();
		this.containerFinishBarrier.countDown();
		logger.error("Resource Manager Callback handler reported an error.", t);
		this.error = t;
	}
	
	/**
	 * 
	 * @param containerId
	 * @param t
	 */
	void containerStartupErrorReceived(ContainerId containerId, Throwable t) {
		logger.error("Container " + containerId + " startup error received: ", t);
	}

	/**
	 * 
	 * @param allocatedContainer
	 */
	abstract void containerAllocated(Container allocatedContainer);

	/**
	 * 
	 */
	@Override
	public void shutDown() {
		try {
			this.doShutDown();
			if (logger.isInfoEnabled()){
				logger.info("Shut down " + this.getClass().getName());
			}
		} catch (Exception e) {
			logger.error("Failure during shut down", e);
		}
	}
	
	/**
	 * 
	 * @throws Exception
	 */
	abstract void doLaunch() throws Exception;
	
	/**
	 * 
	 * @throws Exception
	 */
	abstract void doShutDown() throws Exception;
}
