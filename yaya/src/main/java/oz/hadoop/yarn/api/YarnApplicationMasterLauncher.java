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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.json.simple.JSONObject;

import oz.hadoop.yarn.api.utils.CollectionAssertUtils;

/**
 * Generic implementation of YARN Application Master. Currently YARN does not expose an Application Master
 * via any strategy, thus requiring any YARN-based application to implement its own from scratch as Java Main program.
 * The intention of this implementation of Application Master is to implement general behavior to launch and manage your
 * application container(s) without ever implementing an Application Master.
 *
 * @author Oleg Zhurakousky
 *
 */
final class YarnApplicationMasterLauncher extends BaseContainerLauncher {

	protected final int containerCount;

	private final int priority;

	private final int memory;

	private final int virtualCores;

	private final ExecutorService executor;

	private final NMClientAsync.CallbackHandler nodeManagerCallbaclHandler;

	private final NMClientAsyncImpl nodeManagerClient;

	private final AMRMClientAsync<ContainerRequest> resourceManagerClient;

	private final String command;

	private final CountDownLatch containerMonitor;

	private final ApplicationMasterSpec applicationMasterSpec;

	private final String applicationMasterName;

	private final String applicationMasterId;

	private final PrimitiveImmutableTypeMap argumentMap;

	private final Map<String, Object> containerLaunchArguments;

	private final String containerType;

	private volatile Throwable error;

	/**
	 *
	 */
	YarnApplicationMasterLauncher(String[] args) {
		this.argumentMap = buildArgumentsMap(args[0]);
		if (logger.isInfoEnabled()){
			logger.info("Arguments: " + this.argumentMap);
		}
		this.containerCount = this.argumentMap.getInt(AbstractApplicationContainerSpec.CONTAINER_COUNT);
		this.memory = this.argumentMap.getInt(AbstractApplicationContainerSpec.MEMORY);
		this.virtualCores = this.argumentMap.getInt(AbstractApplicationContainerSpec.VIRTUAL_CORES);
		this.priority = this.argumentMap.getInt(AbstractApplicationContainerSpec.PRIORITY);

		this.applicationMasterName = this.argumentMap.getString(AbstractApplicationContainerSpec.APPLICATION_NAME);
		this.applicationMasterId = this.argumentMap.getString(AbstractApplicationContainerSpec.APPLICATION_ID);
		this.command = this.argumentMap.getString(AbstractApplicationContainerSpec.COMMAND);
		this.containerType = this.argumentMap.getString(AbstractApplicationContainerSpec.CONTAINER_TYPE);

		String containerArguments = this.argumentMap.getString(AbstractApplicationContainerSpec.CONTAINER_SPEC_ARG);
		this.containerLaunchArguments = new HashMap<>();
		this.containerLaunchArguments.put(AbstractApplicationContainerSpec.CONTAINER_SPEC_ARG, containerArguments);
		this.containerLaunchArguments.put(AbstractApplicationContainerSpec.CONTAINER_IMPL, this.argumentMap.getString(AbstractApplicationContainerSpec.CONTAINER_IMPL));

		try {
			this.applicationMasterSpec = (ApplicationMasterSpec) Class.forName(this.argumentMap.getString(AbstractApplicationContainerSpec.AM_SPEC)).newInstance();
		}
		catch (Exception e) {
			throw new IllegalArgumentException("Failed to create an instance of ApplicationMasterSpec", e);
		}

		this.nodeManagerCallbaclHandler = this.applicationMasterSpec.buildNodeManagerCallbackHandler(this);
		this.nodeManagerClient = new NMClientAsyncImpl(this.nodeManagerCallbaclHandler);
		this.resourceManagerClient = AMRMClientAsync.createAMRMClientAsync(1000, this.applicationMasterSpec.buildResourceManagerCallbackHandler(this));

		this.executor = Executors.newFixedThreadPool(this.containerCount);
		this.containerMonitor = new CountDownLatch(this.containerCount);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		CollectionAssertUtils.assertSize(args, 1);
		YarnApplicationMasterLauncher applicationMaster = new YarnApplicationMasterLauncher(args);
		applicationMaster.launch(args);
	}

	@Override
	public void launch(String[] args) throws Exception {
		logger.info("###### Starting APPLICATION MASTER ######");
		if (logger.isDebugEnabled()) {
			logger.debug("SYSTEM PROPERTIES:\n" + System.getProperties());
			logger.debug("ENVIRONMENT VARIABLES:\n" + System.getenv());
		}
		// CALL custom Pre processor

		this.start(); // will block until
		this.stop();
		logger.info("###### Stopped APPLICATION MASTER ######");
		if (this.error != null){
			throw new IllegalStateException("Application Master for " + this.applicationMasterName + "_" + this.applicationMasterId
					 + " failed with error.", this.error);
		}
	}

	/**
	 *
	 * @param allocatedContainer
	 */
	protected void launchContainerAsync(final Container allocatedContainer){
		this.executor.execute(new Runnable() {
			@Override
			public void run() {
				try {
					ContainerLaunchContext containerLaunchContext = Records.newRecord(ContainerLaunchContext.class);
					Map<String, LocalResource> localResources = YarnApplicationMasterLauncher.this.buildLocalResources();
					if (logger.isDebugEnabled()){
				    	logger.debug("Created LocalResources: " + localResources);
				    }
					containerLaunchContext.setLocalResources(localResources);

					String applicationContainerLaunchCommand = this.buildApplicationCommand(containerLaunchContext, localResources);

					if (logger.isInfoEnabled()){
						logger.info("Setting up application container:" + allocatedContainer.getId());
						logger.info("Application Container launch command: " + applicationContainerLaunchCommand);
					}

					containerLaunchContext.setCommands(Collections.singletonList(applicationContainerLaunchCommand));

					YarnApplicationMasterLauncher.this.nodeManagerClient.startContainerAsync(allocatedContainer, containerLaunchContext);
				}
				catch (Exception e){
					logger.warn("Failed to launch container " + allocatedContainer.getId(), e);
					YarnApplicationMasterLauncher.this.error = e;
					YarnApplicationMasterLauncher.this.containerMonitor.countDown();
				}
			}

			/**
			 *
			 */
			private String buildApplicationCommand(ContainerLaunchContext containerLaunchContext, Map<String, LocalResource> localResources) {
				String applicationContainerLaunchCommand;
				if (!"JAVA".equalsIgnoreCase(YarnApplicationMasterLauncher.this.containerType)){
					applicationContainerLaunchCommand = YayaUtils.generateExecutionCommand(
							YarnApplicationMasterLauncher.this.command,
							"",
							"",
							"",
							YarnApplicationMasterLauncher.this.applicationMasterName,
							"_AC_");
				}
				else {
					String classpath = YayaUtils.calculateClassPath(localResources);

					String containerArg = JSONObject.toJSONString(YarnApplicationMasterLauncher.this.containerLaunchArguments);
					String containerArgEncoded = new String(Base64.encodeBase64(containerArg.getBytes()));

					String applicationLauncherName = JavaApplicationContainerLauncher.class.getName();
					applicationContainerLaunchCommand = YayaUtils.generateExecutionCommand(
							YarnApplicationMasterLauncher.this.command + " -cp ",
							classpath,
							applicationLauncherName,
							containerArgEncoded,
							YarnApplicationMasterLauncher.this.applicationMasterName,
							"_AC_");
					YayaUtils.inJvmPrep(YarnApplicationMasterLauncher.this.containerType, containerLaunchContext,
							applicationLauncherName, containerArgEncoded);
				}
				return applicationContainerLaunchCommand;
			}
		});
	}

	/**
	 *
	 * @param container
	 */
	protected void signalContainerCompletion(ContainerStatus containerStatus) {
		if (containerStatus.getExitStatus() != 0){
			this.error = new IllegalStateException(containerStatus.getDiagnostics());
		}
		this.containerMonitor.countDown();
	}

	/**
	 *
	 */
	private Map<String, LocalResource> buildLocalResources() {
		try {
			Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
			String suffix = this.applicationMasterName + "_master/" + this.applicationMasterId + "/";
			FileSystem fs = FileSystem.get(this.yarnConfig);
			Path dst = new Path(fs.getHomeDirectory(), suffix);
			FileStatus[] deployedResources = fs.listStatus(dst);
			for (FileStatus fileStatus : deployedResources) {
				if (logger.isDebugEnabled()){
					logger.debug("Creating local resource for: " + fileStatus.getPath());
				}
				LocalResource scRsrc = LocalResource.newInstance(ConverterUtils.getYarnUrlFromURI(fileStatus.getPath().toUri()),
						LocalResourceType.FILE, LocalResourceVisibility.APPLICATION, fileStatus.getLen(), fileStatus.getModificationTime());
				localResources.put(fileStatus.getPath().getName(), scRsrc);
			}
			return localResources;
		}
		catch (Exception e) {
			throw new IllegalStateException("Failed to communicate with FileSystem", e);
		}
	}

	/**
	 *
	 */
	private void start() {
		try {
			this.startResourceManagerClient();
			this.startNodeManagerClient();
			for (int i = 0; i < this.containerCount; ++i) {
				ContainerRequest containerRequest = this.createConatinerRequest();
				this.resourceManagerClient.addContainerRequest(containerRequest);
				logger.info("Allocating container " + i + " - " + containerRequest);
			}
		}
		catch (Exception e) {
			logger.error("Failed to request container allocation", e);
			this.containerMonitor.countDown();
			this.error = e;
		}

		try {
			this.containerMonitor.await();
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RuntimeException("Current thread was interrupted", e);
		}
	}

	/**
	 *
	 */
	private void startResourceManagerClient(){
		this.resourceManagerClient.init(this.yarnConfig);
		this.resourceManagerClient.start();
		logger.info("Started AMRMClientAsync client");

		try {
			this.resourceManagerClient.registerApplicationMaster(this.yarnConfig.get("yarn.resourcemanager.hostname"), 0, "");
			logger.info("Registered Application Master with ResourceManager");
		}
		catch (Exception e) {
			throw new IllegalStateException("Failed to register ApplicationMaster with ResourceManager", e);
		}
	}

	/**
	 *
	 */
	private void startNodeManagerClient(){
		this.nodeManagerClient.init(this.yarnConfig);
		this.nodeManagerClient.start();
		logger.info("Started NMClientAsyncImpl client");
	}

	/**
	 *
	 */
	private void stop(){
		try {
			String suffix = this.applicationMasterName + "_master/" + this.applicationMasterId + "/";
			FileSystem fs = FileSystem.get(this.yarnConfig);
			Path dst = new Path(fs.getHomeDirectory(), suffix);
			fs.delete(dst, true);
			logger.info("Deleted application jars: " + dst.toString());

			logger.info("Shutting down executor");
			this.executor.shutdown();
			logger.info("Unregistering the Application Master");
			FinalApplicationStatus status = (this.error != null) ? FinalApplicationStatus.FAILED : FinalApplicationStatus.SUCCEEDED;
			this.resourceManagerClient.unregisterApplicationMaster(status, this.generateExitMessage(status) , null);
			logger.info("Shutting down Node Manager Client");
			this.nodeManagerClient.stop();
			logger.info("Shutting down Resource Manager Client");
			this.resourceManagerClient.stop();
			logger.info("Shut down " + this.getClass().getName());
		}
		catch (Exception e) {
			throw new IllegalStateException("Failed to shutdown " + this.getClass().getName(), e);
		}
	}

	/**
	 *
	 * @param status
	 * @return
	 */
	private String generateExitMessage(FinalApplicationStatus status){
		StringBuffer exitMessage = new StringBuffer();
		exitMessage.append("Application '");
		exitMessage.append(this.applicationMasterName);
		exitMessage.append("' launched by ");
		exitMessage.append(this.getClass().getName());
		exitMessage.append(" has finished");
		if (status == FinalApplicationStatus.FAILED){
			exitMessage.append(" with failure. Diagnostic information: " + this.error.getMessage());
		}
		else {
			exitMessage.append(" successfully.");
		}
		return exitMessage.toString();
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
