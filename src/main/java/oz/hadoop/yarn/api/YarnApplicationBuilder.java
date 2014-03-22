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

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import oz.hadoop.yarn.api.utils.JarUtils;
import oz.hadoop.yarn.api.utils.NumberAssertUtils;
import oz.hadoop.yarn.api.utils.ObjectAssertUtils;
import oz.hadoop.yarn.api.utils.StringAssertUtils;

/**
 * Builder (see builder pattern) for building YARN based applications. The goal of this builder to
 * greatly simplify the the internals of the underlying YARN API.
 * It also provides a default implementation of the ApplicationMaster to manage application
 * containers while exposing {@link ApplicationMasterSpec} for variety of customizations.
 * Typical usage would look something like this:
 * <pre>
 *      ApplicationCommand applicationCommand = new ApplicationCommand("ls -all");
 *		applicationCommand.setMemory(32);
 * 		applicationCommand.setContainerCount(1);
 *
 * 		YarnApplication yarnApplication = YarnApplicationBuilder.forApplication("myCoolYarnApp", applicationCommand).
 *				setYarnConfiguration(yarnConfiguration).
 *				setMaxAttempts(1).
 *				setMemory(64).
 *				setVirtualCores(1).
 *				build();
 *
 *      yarnApplication.deploy();
 * <pre>
 * The returned {@link YarnApplication} will will be initialized with default values (see setters) while those values
 * could be overridden during the build process. In other words for bare minimum all you need is the following:
 * <pre>
 *      ApplicationCommand applicationCommand = new ApplicationCommand("ls -all");
 *
 * 		YarnApplication yarnApplication = YarnApplicationBuilder.forApplication("myCoolYarnApp", applicationCommand).build();
 *
 *      yarnApplication.deploy();
 * <pre>
 *
 * @author Oleg Zhurakousky
 *
 */
public class YarnApplicationBuilder {

	private static final Log logger = LogFactory.getLog(YarnApplicationBuilder.class);

	private final static String applicationMasterFqn = "oz.hadoop.yarn.api.YarnApplicationMaster";

	private final String applicationName;

	private YarnClient yarnClient;

	private YarnConfiguration yarnConfig;

	private final ApplicationCommand applicationCommand;

	private final Resource capability;

	private String queueName;

	private int maxAttempts;

	private int priority;

	private String classpath;

	/**
	 * Creates an instance of this builder initializing it with the application name and {@link ApplicationCommand}
	 *
	 * @param applicationName
	 * @param applicationCommand
	 * @return
	 */
	public static YarnApplicationBuilder forApplication(String applicationName, ApplicationCommand applicationCommand){
		StringAssertUtils.assertNotEmptyAndNoSpaces(applicationName);
		ObjectAssertUtils.assertNotNull(applicationCommand);

		YarnApplicationBuilder builder = new YarnApplicationBuilder(applicationName, applicationCommand);
		return builder;
	}
	/**
	 *
	 * @param applicationName
	 */
	private YarnApplicationBuilder(String applicationName, ApplicationCommand applicationCommand){
		this.applicationName = applicationName;
		this.applicationCommand = applicationCommand;
		this.yarnClient = YarnClient.createYarnClient();
		this.yarnConfig = new YarnConfiguration();

		this.capability = Records.newRecord(Resource.class);
		this.capability.setMemory(64);
		this.capability.setVirtualCores(1);
		this.maxAttempts = 1;
		this.queueName = "default";
		this.priority = 0;
	}

	/**
	 * Sets {@link YarnConfiguration}. The default is initialized from the yarn-site.xml
	 *
	 * @param yarnConfig
	 */
	public YarnApplicationBuilder setYarnConfiguration(YarnConfiguration yarnConfig){
		ObjectAssertUtils.assertNotNull(yarnConfig);
		this.yarnConfig = yarnConfig;
		return this;
	}

	/**
	 * Sets maximum retry attempts for starting Application Master before resulting in error. Default is 1.
	 *
	 * @param maxAttempts
	 * @return
	 */
	public YarnApplicationBuilder setMaxAttempts(int maxAttempts) {
		NumberAssertUtils.assertGreaterThenZero(maxAttempts);
		this.maxAttempts = maxAttempts;
		return this;
	}

	/**
	 * Sets the queue name. Default is 'default'
	 *
	 * @param queueName
	 * @return
	 */
	public YarnApplicationBuilder setQueueName(String queueName) {
		StringAssertUtils.assertNotEmptyAndNoSpaces(queueName);
		this.queueName = queueName;
		return this;
	}

	/**
	 * Sets priority. Default is 0.
	 *
	 * @param priority
	 * @return
	 */
	public YarnApplicationBuilder setPriority(int priority) {
		NumberAssertUtils.assertZeroOrPositive(priority);
		this.priority = priority;
		return this;
	}

	/**
	 * Sets memory. Default is 64Mb.
	 *
	 * @param memory
	 * @return
	 */
	public YarnApplicationBuilder setMemory(int memory) {
		NumberAssertUtils.assertGreaterThenZero(memory);
		this.capability.setMemory(memory);
		return this;
	}

	/**
	 * Sets virtual cores. Default is 1. Keep in mind that
	 * virtual cores are only applicable with {@link FairScheduler}
	 *
	 * @param virtualCores
	 * @return
	 */
	public YarnApplicationBuilder setVirtualCores(int virtualCores) {
		NumberAssertUtils.assertGreaterThenZero(virtualCores);
		this.capability.setVirtualCores(virtualCores);
		return this;
	}

	/**
	 * Sets {@link YarnClient} otherwise default is used.
	 *
	 * @param yarnClient
	 */
	public YarnApplicationBuilder setYarnClient(YarnClient yarnClient) {
		ObjectAssertUtils.assertNotNull(yarnClient);
		this.yarnClient = yarnClient;
		return this;
	}

	/**
	 * Will build {@link YarnApplication} from values provided in this builder.
	 * Any change to this builder's values after calling this method will not affect the
	 * newly created {@link YarnApplication}
	 *
	 * @return
	 */
	public YarnApplication build(){
		return new YarnApplication() {
			/**
			 *
			 */
			@Override
			public boolean start() {
				this.initAndStartYarnClient();
				if (YarnApplicationBuilder.this.capability.getVirtualCores() > 1){
					if (!YarnApplicationBuilder.this.yarnConfig.get(YarnConfiguration.RM_SCHEDULER).
							equals(FairScheduler.class.getName())){
						logger.warn("Based on current Hadoop implementation " +
								"'vcore' settings are ignored for schedulers other then FairScheduler");
					}
				}

				YarnClientApplication yarnClientApplication = null;
				try {
					yarnClientApplication = YarnApplicationBuilder.this.yarnClient.createApplication();
				}
				catch (Exception e) {
					throw new IllegalStateException("Failed to create YarnClientApplication", e);
				}

			    ApplicationSubmissionContext appContext = this.initApplicationContext(yarnClientApplication);

			    logger.info("Deploying ApplicationMaster");
			    try {
			    	YarnApplicationBuilder.this.yarnClient.submitApplication(appContext);
				}
			    catch (Exception e) {
					throw new IllegalStateException("Failed to deploy application: " + YarnApplicationBuilder.this.applicationName, e);
				}
			    return true;
			}

			/**
			 *
			 */
			private void initAndStartYarnClient() {
				logger.info("Starting YarnClient");
				yarnClient.init(YarnApplicationBuilder.this.yarnConfig);
				yarnClient.start();
				logger.info("Started YarnClient");
			}

			/**
			 *
			 */
			private ApplicationSubmissionContext initApplicationContext(YarnClientApplication yarnClientApplication){
				ApplicationSubmissionContext appContext = yarnClientApplication.getApplicationSubmissionContext();
			    appContext.setApplicationName(YarnApplicationBuilder.this.applicationName);
			    ContainerLaunchContext applicationMasterContainer = Records.newRecord(ContainerLaunchContext.class);

			    ApplicationId appId = appContext.getApplicationId();

			    Map<String, LocalResource> localResources = YarnApplicationBuilder.this.createLcalResources(appId);
			    applicationMasterContainer.setLocalResources(localResources);

			    List<String> launchCommand = YarnApplicationBuilder.this.createApplicationMasterLaunchCommand(appId, localResources);
				applicationMasterContainer.setCommands(launchCommand);

			    appContext.setResource(YarnApplicationBuilder.this.capability);
			    appContext.setMaxAppAttempts(YarnApplicationBuilder.this.maxAttempts);
			    appContext.setAMContainerSpec(applicationMasterContainer);
			    Priority priority = Records.newRecord(Priority.class);
			    priority.setPriority(YarnApplicationBuilder.this.priority);
			    appContext.setPriority(priority);
			    appContext.setQueue(YarnApplicationBuilder.this.queueName);
			    logger.info("Created Capability: " + capability);

				return appContext;
			}

			@Override
			public boolean stop() {
				throw new UnsupportedOperationException("This method is currently unimplemented. Check later");
			}
		};
	}

	/**
	 * Will package this application JAR in {@link LocalResource}s.
	 * TODO make it more general to allow other resources
	 */
	private Map<String, LocalResource> createLcalResources(ApplicationId appId) {
		Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
		try {
			FileSystem fs = FileSystem.get(YarnApplicationBuilder.this.yarnConfig);
			String[] cp = System.getProperty("java.class.path").split(":");
			for (String v : cp) {
				File f = new File(v);
				if (f.isDirectory()) {
					String jarFileName = YarnApplicationBuilder.this.generateJarFileName();
					logger.info("Creating JAR: " + jarFileName);
					File jarFile = JarUtils.toJar(f, jarFileName);
					addToLocalResources(fs, jarFile.getAbsolutePath(),jarFile.getName(), appId.getId(), localResources);
				}
				//TODO ensure the entire dev classpath is localized on the server
//				else {
//					addToLocalResources(fs, f.getAbsolutePath(), f.getName(), appId.getId(), localResources, null);
//				}
			}
		}
	    catch (Exception e) {
			throw new IllegalStateException(e);
		}
		return localResources;
	}

	/**
	 * Will generate the final launch command for thsi ApplicationMaster
	 */
	private List<String> createApplicationMasterLaunchCommand(ApplicationId appId, Map<String, LocalResource> localResources) {
		List<String> command = new ArrayList<String>();

		if ("true".equals(System.getProperty("local-cluster"))){
			this.classpath = "-cp " + System.getProperty("java.class.path");
		}
		else {
			String[] yarnClassPath = YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH;
			StringBuffer buffer = new StringBuffer();
			String delimiter = ":";
			for (String value : yarnClassPath) {
				buffer.append(value);
				buffer.append(delimiter);
			}
			for (String jar : localResources.keySet()) {
				buffer.append("./" + jar + ":");
			}

			this.classpath = "-cp " + buffer.toString();
		}

		command.add("java " + this.classpath);
		command.add(applicationMasterFqn);

		command.add(this.applicationCommand.build());

		command.add(" 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/" + this.applicationName + "_MasterStdOut");
		command.add(" 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/" + this.applicationName + "_MasterStdErr");

		if (logger.isInfoEnabled()){
			StringBuilder commandBuffer = new StringBuilder();
		    for (String str : command) {
		    	commandBuffer.append(str).append(" ");
		    }
		    logger.info("ApplicationMaster launch command: " + command.toString());
		}

	    return command;
	}

	/**
	 *
	 */
	private void addToLocalResources(FileSystem fs, String fileSrcPath, String fileDstPath, int appId, Map<String, LocalResource> localResources) {
		String suffix = this.applicationName + "/" + appId + "/" + fileDstPath;
		Path dst = new Path(fs.getHomeDirectory(), suffix);

		try {
			fs.copyFromLocalFile(new Path(fileSrcPath), dst);
			new File(fileSrcPath).delete();
			FileStatus scFileStatus = fs.getFileStatus(dst);
			LocalResource scRsrc = LocalResource.newInstance(
					ConverterUtils.getYarnUrlFromURI(dst.toUri()),
					LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
					scFileStatus.getLen(), scFileStatus.getModificationTime());
			localResources.put(fileDstPath, scRsrc);
		}
		catch (Exception e) {
			throw new IllegalStateException("Failed to communicate with FileSystem: " + fs, e);
		}
	}

	/**
	 *
	 * @return
	 */
	private String generateJarFileName(){
		StringBuffer nameBuffer = new StringBuffer();
		nameBuffer.append(this.applicationName);
		nameBuffer.append("_");
		nameBuffer.append(UUID.randomUUID().toString());
		nameBuffer.append(".jar");
		return nameBuffer.toString();
	}
}
