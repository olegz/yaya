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
package oz.hadoop.yarn.api;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.util.Records;

/**
 * Default implementation of YARN Application Master. Currently YARN does not expose the Application Master
 * via any strategy, thus requiring and YARN-based application to implement its own from scratch.
 * The intention of this implementation of ApplicationMaster is to provide such strategy with enough default behavior
 * to allow most of the application containers to be deployed without ever implementing an ApplicationMaster.
 * This implementation could be further customized via {@link ApplicationMasterSpec}.
 *
 * @author Oleg Zhurakousky
 *
 */
class YarnApplicationMaster {

	private static final Log logger = LogFactory.getLog(YarnApplicationMaster.class);

	protected final int containerCount;

	private final int priority;

	private final int memory;

	private final int virtualCores;

	private final ExecutorService executor;

	private final NMClientAsync.CallbackHandler nodeManagerCallbaclHandler;

	private final NMClientAsyncImpl nodeManagerClient;

	private final AMRMClientAsync<ContainerRequest> resourceManagerClient;

	private final String command;

	private final YarnConfiguration yarnConfig;

	private final CountDownLatch containerMonitor;

	private final ApplicationMasterSpec applicationMasterSpec;

	/**
	 *
	 */
	YarnApplicationMaster(String[] args) {
		this.command = args[1];
		this.containerCount = Integer.parseInt(args[3]);
		this.memory = Integer.parseInt(args[5]);
		this.virtualCores = Integer.parseInt(args[7]);
		this.priority = Integer.parseInt(args[9]);
		try {
			this.applicationMasterSpec = (ApplicationMasterSpec) Class.forName(args[11]).newInstance();
		}
		catch (Exception e) {
			throw new IllegalArgumentException("Failed to create an instance of ApplicationMasterSpec", e);
		}

		this.yarnConfig = new YarnConfiguration();

		this.executor = Executors.newFixedThreadPool(this.containerCount);
		this.containerMonitor = new CountDownLatch(this.containerCount);

		this.nodeManagerCallbaclHandler = this.applicationMasterSpec.buildNodeManagerCallbackHandler(this);
		this.nodeManagerClient = new NMClientAsyncImpl(this.nodeManagerCallbaclHandler);
		this.resourceManagerClient = AMRMClientAsync.createAMRMClientAsync(1000, this.applicationMasterSpec.buildResourceManagerCallbackHandler(this));
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		YarnApplicationMaster applicationMaster = new YarnApplicationMaster(args);
		applicationMaster.start(); // will block until
		applicationMaster.stop();
	}

	/**
	 *
	 * @return
	 */
	public String getCommand() {
		return command;
	}

	/**
	 *
	 * @param allocatedContainer
	 */
	protected void launchContainerAsync(final Container allocatedContainer){
		this.executor.execute(new Runnable() {
			@Override
			public void run() {
				logger.info("Setting up container launch container for containerid:" + allocatedContainer.getId());
				ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
				ctx.setCommands(Collections.singletonList(command +
						" 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
						" 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"));

				YarnApplicationMaster.this.nodeManagerClient.startContainerAsync(allocatedContainer, ctx);
			}
		});
	}

	/**
	 *
	 * @param container
	 */
	protected void signalContainerCompletion(ContainerStatus containerStatus) {
		this.containerMonitor.countDown();
	}

	/**
	 *
	 */
	private void start() {
		this.resourceManagerClient.init(this.yarnConfig);
		this.resourceManagerClient.start();
		logger.info("Started AMRMClientAsync client");

		this.nodeManagerClient.init(this.yarnConfig);
		this.nodeManagerClient.start();
		logger.info("Started NMClientAsyncImpl client");

		try {
			this.resourceManagerClient.registerApplicationMaster("", 0, "");
			logger.info("Registered ApplicationMaster with ResourceManager");
		}
		catch (Exception e) {
			throw new IllegalStateException("Failed to register ApplicationMaster with ResourceManager", e);
		}

		for (int i = 0; i < this.containerCount; ++i) {
			ContainerRequest containerRequest = this.createConatinerRequest();
			this.resourceManagerClient.addContainerRequest(containerRequest);
			logger.info("Allocation container " + i + " - " + containerRequest);
		}

		try {
			this.containerMonitor.await();
			logger.info("Shutting down executor");
			this.executor.shutdown();
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RuntimeException("Current thread was interrupted", e);
		}
	}

	/**
	 *
	 */
	private void stop(){
		try {
			logger.info("Unregistering the ApplicationMaster");
			this.resourceManagerClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
					"Application " + this.getClass().getName() + " has finished" , null);
			logger.info("Shutting down Node Manager Client");
			this.nodeManagerClient.close();
			logger.info("Shutting down Resource Manager Client");
			this.resourceManagerClient.close();
		}
		catch (Exception e) {
			throw new IllegalStateException("Failed to shutdown " + this.getClass().getName(), e);
		}
	}

	/**
	 * Will create a {@link ContainerRequest} to the {@link ResourceManager}
	 * to obtain an application container
	 */
	private ContainerRequest createConatinerRequest() {
		Priority priority = Records.newRecord(Priority.class);
		priority.setPriority(this.priority);
		Resource capability = Records.newRecord(Resource.class);
		capability.setMemory(this.memory);
		capability.setVirtualCores(this.virtualCores);

		//TODO support configuration to request resource containers on specific nodes
		ContainerRequest request = new ContainerRequest(capability, null, null, priority);
		logger.info("Created container request: " + request);
		return request;
	}
}
