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

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerStatusPBImpl;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;

import oz.hadoop.yarn.api.utils.PrimitiveImmutableTypeMap;
import oz.hadoop.yarn.api.utils.ReflectionUtils;

/**
 * 
 * @author Oleg Zhurakousky
 *
 */
public final class ApplicationContainerLauncherEmulatorImpl extends AbstractApplicationContainerLauncher {
	
	private final Log logger = LogFactory.getLog(ApplicationContainerLauncherEmulatorImpl.class);
	
	private final ExecutorService executor;
	
	private final AMRMClientAsync.CallbackHandler rmCallbackHandler;
	
	private final NMClientAsync.CallbackHandler nmCallbackHandler;
	
	private final ApplicationId applicationId;
	
	private final ApplicationAttemptId applicationAttemptId;
	
	private final Map<Container, ApplicationContainer> applicationContainers;
	

	/**
	 * 
	 * @param applicationSpecification
	 * @param containerSpecification
	 */
	public ApplicationContainerLauncherEmulatorImpl(PrimitiveImmutableTypeMap applicationSpecification, PrimitiveImmutableTypeMap containerSpecification) {
		super(applicationSpecification, containerSpecification);
		this.executor = Executors.newCachedThreadPool();
		this.rmCallbackHandler = this.callbackSupport.buildResourceManagerCallbackHandler(this);
		this.nmCallbackHandler = this.callbackSupport.buildNodeManagerCallbackHandler(this);
		this.applicationId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
		this.applicationAttemptId = ApplicationAttemptId.newInstance(this.applicationId, 1);
		this.applicationContainers = new HashMap<Container, ApplicationContainer>();
		
		// do preallocation early. Important for testing (see ApplicationContainerTests.validateSelfShutdownWithContainerStartupException)
		int containerStartId = 2;
		for (int i = 0; i < this.containerCount; i++) {		
			ContainerRequest containerRequest = this.createConatinerRequest();
			// TODO implement a better mock so it can show ContainerRequest values
			Container container = new ContainerPBImpl();
			ContainerId containerId = ContainerId.newInstance(this.applicationAttemptId, containerStartId++);
			container.setId(containerId);
			ApplicationContainer applicationContainer = new ApplicationContainer(ApplicationContainerLauncherEmulatorImpl.this.applicationSpecification);
			this.applicationContainers.put(container, applicationContainer);
		}
	}

	/**
	 * 
	 */
	@Override
	void doLaunch() throws Exception {
		for (Container container : this.applicationContainers.keySet()) {
			this.rmCallbackHandler.onContainersAllocated(Collections.singletonList(container));
		}
	}

	/**
	 * 
	 */
	@Override
	void doShutDown() throws Exception {
		logger.info("Shutting down executor");
		this.executor.shutdown();
	}

	/**
	 * 
	 */
	@Override
	void containerAllocated(final Container allocatedContainer) {
		final ApplicationContainer applicationContainer = applicationContainers.get(allocatedContainer);
		if (logger.isDebugEnabled()){
			logger.debug("Container allocated");
		}
		final AtomicBoolean error = new AtomicBoolean();
		this.executor.execute(new Runnable() {	
			@Override
			public void run() {
				try {
					applicationContainer.launch();
					logger.info("Container finished");
					// TODO implement a better mock so it can show ContainerRequest values
					ContainerStatus containerStatus = new ContainerStatusPBImpl();
					rmCallbackHandler.onContainersCompleted(Collections.singletonList(containerStatus));
				} 
				catch (Exception e) {
					logger.error("Application Container failed. ", e);
					rmCallbackHandler.onError(e);
					error.set(true);
				}
			}
		});
		
		this.executor.execute(new Runnable() {	
			@Override
			public void run() {
				try {
					Field clientField = ReflectionUtils.getFieldAndMakeAccessible(applicationContainer.getClass(), "client");
					while (clientField.get(applicationContainer) == null && !error.get()){
						LockSupport.parkNanos(1000000);
					}
					if (!error.get()){
						nmCallbackHandler.onContainerStarted(allocatedContainer.getId(), null);
						if (logger.isDebugEnabled()){
							logger.debug("Container started");
						}
					}
				} 
				catch (Exception e) {
					logger.error("Sjould never heppen. Must be a bug. Please report", e);
					e.printStackTrace();
				}
			}
		});
	}
}
