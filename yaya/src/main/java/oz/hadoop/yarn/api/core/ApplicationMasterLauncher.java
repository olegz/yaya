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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
import org.springframework.util.StringUtils;

import oz.hadoop.yarn.api.YayaConstants;
import oz.hadoop.yarn.api.utils.PrimitiveImmutableTypeMap;


/**
 * @author Oleg Zhurakousky
 *
 */
class ApplicationMasterLauncher extends AbstractContainerLauncher {
	
	private final ApplicationMasterCallbackSupport callbackSupport;
	
	private final AMRMClientAsync<ContainerRequest> resourceManagerClient;
	
	private final NMClientAsync.CallbackHandler nodeManagerCallbaclHandler;

	private final NMClientAsyncImpl nodeManagerClient;
	
	private final CountDownLatch containerMonitor;
	
	private final boolean local;
	
	private final ExecutorService executor;
	
	private volatile Throwable error;

	/**
	 * 
	 * @param containerArguments
	 */
	public ApplicationMasterLauncher(PrimitiveImmutableTypeMap containerArguments) {
		super(containerArguments);
		this.callbackSupport = new ApplicationMasterCallbackSupport();
		this.resourceManagerClient = AMRMClientAsync.createAMRMClientAsync(100, this.callbackSupport.buildResourceManagerCallbackHandler(this));
		this.nodeManagerCallbaclHandler = this.callbackSupport.buildNodeManagerCallbackHandler(this);
		
		this.nodeManagerClient = new NMClientAsyncImpl(this.nodeManagerCallbaclHandler);
		this.containerMonitor = new CountDownLatch(this.containerSpec.getInt(YayaConstants.CONTAINER_COUNT));
		this.local = this.applicationSpecification.getBoolean(YayaConstants.LOCAL);
		if (this.local){
			this.executor = Executors.newCachedThreadPool();
		}
		else {
			this.executor = null;
		}
	}

	/**
	 * 
	 */
	@Override
	void run() {
		logger.info("###### Starting APPLICATION MASTER ######");
		
		if (this.local){
			this.doRunLocal();
		}
		else {
			this.doRunYarn();
			// Wait till all containers are finished. clean up and exit.
			try {
				this.containerMonitor.await();
				if (this.error != null){
					throw new IllegalStateException("Application Master for " + this.applicationSpecification.getString(YayaConstants.APPLICATION_NAME) + "_" + 
							this.applicationSpecification.get("appid") + " failed with error.", this.error);
				}
			} 
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				logger.error("APPLICATION MASTER's thread was interrupted", e);
			}
			finally {
				this.shutdown();
			}
		}
	}
	
	/**
	 *
	 * @param container
	 */
	protected void signalContainerCompletion(ContainerStatus containerStatus) {
		this.containerMonitor.countDown();
		if (containerStatus.getExitStatus() != 0){
			this.error = new IllegalStateException(containerStatus.getDiagnostics());
		}
	}
	
	/**
	 * 
	 * @param allocatedContainer
	 */
	protected void launchContainer(Container allocatedContainer){
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
			this.containerMonitor.countDown();
			logger.warn("Failed to launch container " + allocatedContainer.getId(), e);
			this.error = e;
		}
	}
	
	/**
	 * 
	 */
	void doRunYarn() {
		this.startResourceManagerClient();
		this.startNodeManagerClient();
		// Allocate containers. Containers will be launched when callback invokes launch(Container) method.
		int containerCount = this.containerSpec.getInt(YayaConstants.CONTAINER_COUNT);
		for (int i = 0; i < containerCount; ++i) {
			ContainerRequest containerRequest = this.createConatinerRequest();
			this.resourceManagerClient.addContainerRequest(containerRequest);
			if (logger.isInfoEnabled()){
				logger.info("Allocating container " + i + " - " + containerRequest);
			}
		}
	}
	
	/**
	 * 
	 */
	void doRunLocal() {
		int containerCount = containerSpec.getInt(YayaConstants.CONTAINER_COUNT);
		final CountDownLatch monitoringLatch = new CountDownLatch(containerCount);
		for (int i = 0; i < containerCount; i++) {
			this.executor.execute(new Runnable() {
				@Override
				public void run() {
					String command = containerSpec.getString(YayaConstants.COMMAND);
					try {
						if (StringUtils.hasText(command)){
							try {
								executeProcess(command);
							} 
							catch (Exception e) {
								logger.error(e);
								throw new IllegalStateException("Failed to execute command-based Application Container", e);
							}
						}
						else {
							ApplicationContainerLauncher launcher = new ApplicationContainerLauncher(ApplicationMasterLauncher.this.applicationSpecification);
							launcher.run();
							if (logger.isInfoEnabled()){
								logger.info("Container finished");
							}
						}
					} 
					finally {
						monitoringLatch.countDown();
					}					
				}
			});
		}
		this.executor.execute(new Monitor(monitoringLatch, executor));
	}
	
	/**
	 * This one is only called when execution is local. It will excute the process as Runtime.exec()
	 * and print its output to the console as [OUT] and [ERROR]. 
	 */
	private void executeProcess(String command) throws Exception {
		final Process p = Runtime.getRuntime().exec(command);
		final CountDownLatch latch = new CountDownLatch(2);
		this.executor.execute(new Runnable() {
			@Override
			public void run() {
				try {
					BufferedReader errorReader = new BufferedReader(new InputStreamReader(p.getErrorStream()));
					String line;
					while ((line = errorReader.readLine()) != null){
						logger.info("[ERROR]: " + line);
					}
				} 
				catch (Exception e) {
					throw new IllegalStateException("Process resulted in error", e);
				}
				finally {
					latch.countDown();
				}
			}
		});
		this.executor.execute(new Runnable() {
			@Override
			public void run() {
				try {
					BufferedReader outReader =  new BufferedReader(new InputStreamReader(p.getInputStream()));
					String line;
					while ((line = outReader.readLine()) != null){
						logger.info("[OUT]: " + line);
					}
				} 
				catch (Exception e) {
					throw new IllegalStateException("Process resulted in error", e);
				}
				finally {
					latch.countDown();
				}
			}
		});
		latch.await();
		logger.info("Finished executing command based Application Container");
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
	 * Will create a {@link ContainerRequest} to the {@link ResourceManager}
	 * to obtain an application container
	 */
	private ContainerRequest createConatinerRequest() {
		Priority priority = Records.newRecord(Priority.class);
		priority.setPriority(this.containerSpec.getInt(YayaConstants.PRIORITY));
		Resource capability = Records.newRecord(Resource.class);
		capability.setMemory(this.containerSpec.getInt(YayaConstants.MEMORY));
		capability.setVirtualCores(this.containerSpec.getInt(YayaConstants.VIRTUAL_CORES));

		//TODO support configuration to request resource containers on specific nodes
		ContainerRequest request = new ContainerRequest(capability, null, null, priority);
		logger.info("Created container request: " + request);
		return request;
	}
	
	/**
	 *
	 */
	private void shutdown(){
		try {
			if (!this.local){
				String suffix = this.applicationSpecification.getString(YayaConstants.APPLICATION_NAME) + "_master/" + this.applicationSpecification.getInt(YayaConstants.APP_ID) + "/";
				FileSystem fs = FileSystem.get(this.yarnConfig);
				Path dst = new Path(fs.getHomeDirectory(), suffix);
				fs.delete(dst, true);
				if (logger.isInfoEnabled()){
					logger.info("Deleted application jars: " + dst.toString());
				}
				logger.info("Unregistering the Application Master");
				FinalApplicationStatus status = (this.error != null) ? FinalApplicationStatus.FAILED : FinalApplicationStatus.SUCCEEDED;
				this.resourceManagerClient.unregisterApplicationMaster(status, this.generateExitMessage(status) , null);
				logger.info("Shutting down Node Manager Client");
				this.nodeManagerClient.stop();
				logger.info("Shutting down Resource Manager Client");
				this.resourceManagerClient.stop();
			}
			if (this.executor != null){
				this.executor.shutdown();
			}
			
			if (logger.isInfoEnabled()){
				logger.info("Shut down " + this.getClass().getName());
			}
		}
		catch (Exception e) {
			throw new IllegalStateException("Failed to shutdown " + this.getClass().getName(), e);
		}
		logger.info("###### Stopped APPLICATION MASTER ######");
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
	@SuppressWarnings("unchecked")
	private String buildApplicationCommand(ContainerLaunchContext containerLaunchContext, Map<String, LocalResource> localResources) {
		String applicationContainerLaunchCommand;

		String classpath = YayaUtils.calculateClassPath(localResources);
		String containerArg = JSONObject.toJSONString(this.applicationSpecification);
		String containerArgEncoded = new String(Base64.encodeBase64(containerArg.getBytes()));

		String applicationLauncherName = ApplicationContainerLauncher.class.getName();
		
		boolean isJavaContainer = !this.containerSpec.containsKey(YayaConstants.COMMAND);
		if (!isJavaContainer){
			applicationContainerLaunchCommand = YayaUtils.generateExecutionCommand(
					(String)((Map<String, Object>)this.applicationSpecification.get(YayaConstants.CONTAINER_SPEC)).get(YayaConstants.COMMAND),
					"",
					"",
					"",
					this.applicationSpecification.getString(YayaConstants.APPLICATION_NAME),
					"_AC_");
		}
		else {
			applicationContainerLaunchCommand = YayaUtils.generateExecutionCommand(
					this.containerSpec.getString(YayaConstants.JAVA_COMMAND) + " -cp ",
					classpath,
					applicationLauncherName,
					containerArgEncoded,
					this.applicationSpecification.getString(YayaConstants.APPLICATION_NAME),
					"_AC_");
			YayaUtils.inJvmPrep("JAVA", containerLaunchContext, applicationLauncherName, containerArgEncoded);
		}
		return applicationContainerLaunchCommand;
	}
	
	/**
	 * Container execution monitor task
	 */
	private class Monitor implements Runnable {
		private final CountDownLatch monitoringLatch;
		private final ExecutorService executor;
		public Monitor(CountDownLatch monitoringLatch, ExecutorService executor){
			this.monitoringLatch = monitoringLatch;
			this.executor = executor;
		}
		@Override
		public void run() {
			try {
				boolean finished = this.monitoringLatch.await(100, TimeUnit.MILLISECONDS);
				if (finished){
					ApplicationMasterLauncher.this.shutdown();
					if (logger.isInfoEnabled()){
						logger.info("All containers finished");
					}
				}
				else {
					this.executor.execute(this);
				}
			} 
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				logger.warn("Container execution monitoring task was interrupted", e);
			}
		}
	}
}
