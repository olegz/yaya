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

import java.io.File;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.json.simple.JSONObject;

import oz.hadoop.yarn.api.YayaConstants;
import oz.hadoop.yarn.api.utils.JarUtils;

/**
 * INTERNAL API
 * 
 * @author Oleg Zhurakousky
 *
 */
class ApplicationMasterLauncherImpl<T> extends AbstractApplicationMasterLauncher<T> {
	
	private final Log logger = LogFactory.getLog(ApplicationMasterLauncherImpl.class);
	
	private static final String AM_CLASS_NAME = ApplicationMaster.class.getName();
	
	private final YarnClient yarnClient;
	
	private ApplicationId applicationId;

	/**
	 * 
	 * @param applicationSpecification
	 */
	ApplicationMasterLauncherImpl(Map<String, Object> applicationSpecification) {
		super(applicationSpecification);
		this.yarnClient = YarnClient.createYarnClient();
	}

	/**
	 * 
	 */
	@Override
	ApplicationId doLaunch(int launchApplicationMaster) {
		this.startYarnClient();
		
		this.preCheck();
		
		// TODO see if these calls could be made ASYNC since they take time, but always succeed even if cluster is not running.
		
		YarnClientApplication yarnClientApplication = this.createYarnClientApplication();
		ApplicationSubmissionContext appContext = this.initApplicationContext(yarnClientApplication);
		logger.info("Deploying ApplicationMaster");
	    try {
	    	this.applicationId = this.yarnClient.submitApplication(appContext);
		}
	    catch (Exception e) {
			throw new IllegalStateException("Failed to launch Application Master: " + this.applicationName, e);
		}
		return this.applicationId;
	}
	
	/**
	 * 
	 */
	ApplicationId doShutDown() {
		try {
			this.yarnClient.stop();
		} 
		catch (Exception e) {
			logger.warn("Call to YarnClient.stop() resulted in Exception, probably due to the fact that applicaton is already stopped. Application: " + this.applicationId, e);
		}
		if (logger.isInfoEnabled()){
			logger.info("Shut down YarnClient");
		}
		return this.applicationId;
	}
	
	/**
	 * 
	 */
	private void startYarnClient() {
		this.yarnClient.init(this.yarnConfig);
		this.yarnClient.start();
		logger.debug("Started YarnClient");
	}
	
	/**
	 * Any type of pre-check you want to perform before launching Application Master
	 * mainly for the purpose of logging warning messages
	 */
	private void preCheck(){
		if (this.applicationContainerSpecification.getInt(YayaConstants.VIRTUAL_CORES) > 1){
			if (!this.yarnConfig.get(YarnConfiguration.RM_SCHEDULER).equals(FairScheduler.class.getName())){
				logger.warn("Based on current Hadoop implementation " +
						"'vcore' settings are ignored for schedulers other then FairScheduler");
			}
		}
		try {
			Iterator<QueueInfo> queues = this.yarnClient.getAllQueues().iterator();
			String identifiedQueueName = (String) this.applicationSpecification.get(YayaConstants.QUEUE_NAME);
			boolean queueExist = false;
			while (!queueExist && queues.hasNext()) {
				QueueInfo queueInfo = queues.next();
				if (queueInfo.getQueueName().equals(identifiedQueueName)){
					queueExist = true;
				}
			}
			if (!queueExist){
				throw new IllegalArgumentException("Queue with the name '" + identifiedQueueName + "' does not exist. Aborting application launch.");
			}
		} 
		catch (Exception e) {
			throw new IllegalStateException("Failed to validate queue.", e);
		}
	}
	
	/**
	 *
	 */
	private YarnClientApplication createYarnClientApplication(){
		try {
			// TODO put a log message about trying to establish the connection to RM
			// TODO could do a simple Connect test to the ResourceManager (e.g., 8055) and throw an exception
			YarnClientApplication yarnClientApplication = this.yarnClient.createApplication();
			logger.debug("Created YarnClientApplication");
			return yarnClientApplication;
		}
		catch (Exception e) {
			throw new IllegalStateException("Failed to create YarnClientApplication", e);
		}
	}
	
	/**
	 *
	 */
	private ApplicationSubmissionContext initApplicationContext(YarnClientApplication yarnClientApplication){
		ApplicationSubmissionContext appContext = yarnClientApplication.getApplicationSubmissionContext();
		
	    appContext.setApplicationName(this.applicationName);
	    this.applicationId = appContext.getApplicationId();
	    this.applicationSpecification.put(YayaConstants.APP_ID, this.applicationId.getId());
	  
	    ContainerLaunchContext containerLaunchContext = Records.newRecord(ContainerLaunchContext.class);

	    Map<String, LocalResource> localResources = this.createLocalResources();
	    if (logger.isDebugEnabled()){
	    	logger.debug("Created LocalResources: " + localResources);
	    }
	    containerLaunchContext.setLocalResources(localResources);
	    String jsonArguments = JSONObject.toJSONString(this.applicationSpecification);
		String encodedJsonArguments = new String(Base64.encodeBase64(jsonArguments.getBytes()));
		
		YayaUtils.inJvmPrep("JAVA", containerLaunchContext, AM_CLASS_NAME, encodedJsonArguments);

	    String applicationMasterLaunchCommand = this.createApplicationMasterLaunchCommand(localResources, encodedJsonArguments);
	    containerLaunchContext.setCommands(Collections.singletonList(applicationMasterLaunchCommand));

		Priority priority = Records.newRecord(Priority.class);
		priority.setPriority((int) this.applicationSpecification.get(YayaConstants.PRIORITY));
	    Resource capability = Records.newRecord(Resource.class);
	    capability.setMemory((int) this.applicationSpecification.get(YayaConstants.MEMORY));
	    capability.setVirtualCores((int) this.applicationSpecification.get(YayaConstants.VIRTUAL_CORES));
	    appContext.setResource(capability);
	    appContext.setMaxAppAttempts((int) this.applicationSpecification.get(YayaConstants.MAX_ATTEMPTS));
	    appContext.setAMContainerSpec(containerLaunchContext);
	    appContext.setPriority(priority);
	    appContext.setQueue((String) this.applicationSpecification.get(YayaConstants.QUEUE_NAME)); 

	    if (logger.isDebugEnabled()){
	    	logger.debug("Created ApplicationSubmissionContext: " + appContext);
	    }

		return appContext;
	}
	
	/**
	 * Will generate the final launch command for this ApplicationMaster
	 */
	private String createApplicationMasterLaunchCommand(Map<String, LocalResource> localResources, String containerSpecStr) {
		String classpath = YayaUtils.calculateClassPath(localResources);
		if (logger.isDebugEnabled()){
			logger.debug("Application master classpath: " + classpath);
		}

		String applicationMasterLaunchCommand = YayaUtils.generateExecutionCommand(
				this.applicationContainerSpecification.getString(YayaConstants.JAVA_COMMAND) + " -cp ",
				classpath,
				AM_CLASS_NAME,
				containerSpecStr,
				this.applicationName,
				"_AM_");

		if (logger.isDebugEnabled()){
			logger.debug("Application Master launch command: " + applicationMasterLaunchCommand);
		}

	    return applicationMasterLaunchCommand;
	}
	
	/**
	 * Will package this application JAR in {@link LocalResource}s.
	 * TODO make it more general to allow other resources
	 */
	private Map<String, LocalResource> createLocalResources() {
		Map<String, LocalResource> localResources = new LinkedHashMap<String, LocalResource>();

		try {
			FileSystem fs = FileSystem.get(this.yarnConfig);

			String[] cp = System.getProperty("java.class.path").split(":");
			for (String v : cp) {
				File f = new File(v);
				if (f.isDirectory()) {
					String jarFileName = YayaUtils.generateJarFileName(this.applicationName);
					if (logger.isDebugEnabled()){
						logger.debug("Creating JAR: " + jarFileName);
					}
					File jarFile = JarUtils.toJar(f, jarFileName);
					this.addToLocalResources(fs, jarFile.getAbsolutePath(),jarFile.getName(), this.applicationId.getId(), localResources);
					try {
						new File(jarFile.getAbsolutePath()).delete(); // will delete the generated JAR file
					}
					catch (Exception e) {
						logger.warn("Failed to delete generated JAR file: " + jarFile.getAbsolutePath(), e);
					}
				}
				else {
					this.addToLocalResources(fs, f.getAbsolutePath(), f.getName(), this.applicationId.getId(), localResources);
				}
			}
		}
	    catch (Exception e) {
			throw new IllegalStateException(e);
		}
		return localResources;
	}
	
	/**
	 *
	 */
	private void addToLocalResources(FileSystem fs, String fileSrcPath, String fileDstPath, int appId, Map<String, LocalResource> localResources) {
		String suffix = this.applicationName + "_master/" + appId + "/" + fileDstPath;
		Path dst = new Path(fs.getHomeDirectory(), suffix);

		try {
			fs.copyFromLocalFile(new Path(fileSrcPath), dst);
			FileStatus scFileStatus = fs.getFileStatus(dst);
			LocalResource scRsrc = LocalResource.newInstance(ConverterUtils.getYarnUrlFromURI(dst.toUri()),
					LocalResourceType.FILE, LocalResourceVisibility.APPLICATION, scFileStatus.getLen(), scFileStatus.getModificationTime());
			localResources.put(fileDstPath, scRsrc);
		}
		catch (Exception e) {
			throw new IllegalStateException("Failed to communicate with FileSystem: " + fs, e);
		}
	}
}
