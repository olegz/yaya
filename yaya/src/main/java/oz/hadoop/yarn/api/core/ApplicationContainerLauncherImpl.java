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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.json.simple.JSONObject;

import oz.hadoop.yarn.api.YayaConstants;
import oz.hadoop.yarn.api.utils.PrimitiveImmutableTypeMap;

/**
 * @author Oleg Zhurakousky
 *
 */
public class ApplicationContainerLauncherImpl extends AbstractApplicationContainerLauncher {
	
	private final Log logger = LogFactory.getLog(ApplicationContainerLauncherImpl.class);
	
	private final ApplicationMasterCallbackSupport callbackSupport;
	
	private final AMRMClientAsync<ContainerRequest> resourceManagerClient;
	
	private final NMClientAsync.CallbackHandler nodeManagerCallbaclHandler;
	
	private final NMClientAsyncImpl nodeManagerClient;
	
	private final YarnConfiguration yarnConfig;

	public ApplicationContainerLauncherImpl(PrimitiveImmutableTypeMap applicationSpecification, PrimitiveImmutableTypeMap containerSpecification) {
		super(applicationSpecification, containerSpecification);
		this.callbackSupport = new ApplicationMasterCallbackSupport();
		this.resourceManagerClient = AMRMClientAsync.createAMRMClientAsync(100, this.callbackSupport.buildResourceManagerCallbackHandler(this));		
		this.nodeManagerCallbaclHandler = this.callbackSupport.buildNodeManagerCallbackHandler(this);
		this.nodeManagerClient = new NMClientAsyncImpl(this.nodeManagerCallbaclHandler);
		this.yarnConfig = new YarnConfiguration();
	}
	
	@Override
	void doLaunch() throws Exception {
		this.startResourceManagerClient();
		this.startNodeManagerClient();
		// Allocate containers. Containers will be launched when callback invokes launch(Container) method.
		int containerCount = this.containerSpecification.getInt(YayaConstants.CONTAINER_COUNT);
		for (int i = 0; i < containerCount; ++i) {
			ContainerRequest containerRequest = this.createConatinerRequest();
			this.resourceManagerClient.addContainerRequest(containerRequest);
			if (logger.isDebugEnabled()){
				logger.debug("Allocating container " + i + " - " + containerRequest);
			}
		}
	}

	@Override
	void doShutDown() throws Exception {
		String suffix = this.applicationSpecification.getString(YayaConstants.APPLICATION_NAME) + "_master/" + this.applicationSpecification.getInt(YayaConstants.APP_ID) + "/";
		FileSystem fs = FileSystem.get(this.yarnConfig);
		Path dst = new Path(fs.getHomeDirectory(), suffix);
		fs.delete(dst, true);
		if (logger.isInfoEnabled()){
			logger.info("Deleted application jars: " + dst.toString());
		}
		logger.info("Unregistering the Application Master");
		FinalApplicationStatus status = (this.error != null) ? FinalApplicationStatus.FAILED : FinalApplicationStatus.SUCCEEDED;
		//this.resourceManagerClient.getClusterNodeCount()
		//this.resourceManagerClient.getFailureCause()
		this.resourceManagerClient.unregisterApplicationMaster(status, this.generateExitMessage(status) , null);
		
		logger.info("Shutting down Node Manager Client");
		this.nodeManagerClient.stop();
		logger.info("Shutting down Resource Manager Client");
		this.resourceManagerClient.stop();
	}
	
	/**
	 *
	 * @param container
	 */
	void signalContainerCompletion(ContainerStatus containerStatus) {
		this.livelinessBarrier.countDown();
		if (containerStatus.getExitStatus() != 0){
			this.error = new IllegalStateException(containerStatus.getDiagnostics());
		}
	}
	
	/**
	 * 
	 */
	void initiateShutdown(List<Container> containers) {	
		logger.debug("Initiating shutdown");
		for (int i = 0; i < this.containerCount; i++) {
			this.livelinessBarrier.countDown();
		}
	}
	
	/**
	 * 
	 * @param allocatedContainer
	 */
	void launchContainer(Container allocatedContainer){
		try {
			ContainerLaunchContext containerLaunchContext = Records.newRecord(ContainerLaunchContext.class);
			Map<String, LocalResource> localResources = this.buildLocalResources();
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

			this.nodeManagerClient.startContainerAsync(allocatedContainer, containerLaunchContext);
		}
		catch (Exception e) {
			this.livelinessBarrier.countDown();
			logger.warn("Failed to launch container " + allocatedContainer.getId(), e);
			this.error = e;
		}
	}
	
	/**
	 *
	 */
	private Map<String, LocalResource> buildLocalResources() {
		try {
			Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
			String suffix = this.applicationSpecification.getString(YayaConstants.APPLICATION_NAME) + "_master/" + this.applicationSpecification.getInt(YayaConstants.APP_ID) + "/";
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
			throw new IllegalStateException("Failed to build LocalResources", e);
		}
	}
	
	/**
	 *
	 */
	private String buildApplicationCommand(ContainerLaunchContext containerLaunchContext, Map<String, LocalResource> localResources) {
		String classpath = YayaUtils.calculateClassPath(localResources);
		String containerArg = JSONObject.toJSONString(this.applicationSpecification);
		String containerArgEncoded = new String(Base64.encodeBase64(containerArg.getBytes()));

		String applicationLauncherName = ApplicationContainer.class.getName();
		
		String applicationContainerLaunchCommand = YayaUtils.generateExecutionCommand(
					this.containerSpecification.getString(YayaConstants.JAVA_COMMAND) + " -cp ",
					classpath,
					applicationLauncherName,
					containerArgEncoded,
					this.applicationSpecification.getString(YayaConstants.APPLICATION_NAME),
					"_AC_");
		
		YayaUtils.inJvmPrep("JAVA", containerLaunchContext, applicationLauncherName, containerArgEncoded);
		
		return applicationContainerLaunchCommand;
	}
	
	/**
	 *
	 */
	private void startResourceManagerClient(){
		this.resourceManagerClient.init(this.yarnConfig);
		this.resourceManagerClient.start();
		logger.debug("Started AMRMClientAsync client");

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
		logger.debug("Started NMClientAsyncImpl client");
	}
	
	/**
	 * Will create a {@link ContainerRequest} to the {@link ResourceManager}
	 * to obtain an application container
	 */
	private ContainerRequest createConatinerRequest() {
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
	 * @param status
	 * @return
	 */
	private String generateExitMessage(FinalApplicationStatus status){
		StringBuffer exitMessage = new StringBuffer();
		exitMessage.append("Application '");
		exitMessage.append(this.applicationSpecification.getString(YayaConstants.APPLICATION_NAME));
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
}
