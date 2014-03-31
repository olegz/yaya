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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

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
 *      UnixApplicationContainerSpec unixContainer = new UnixApplicationContainerSpec("ls -all");
 *		unixContainer.setMemory(32);
 *
 * 		YarnApplication yarnApplication = YarnApplicationBuilder.forApplication("myCoolYarnApp", unixContainer).
 *				setYarnConfiguration(yarnConfiguration).
 *				setMaxAttempts(1).
 *				setMemory(1024).
 *				setVirtualCores(1).
 *				build();
 *
 *      yarnApplication.launch();
 * <pre>
 * The returned {@link YarnApplication} will will be initialized with default values (see setters) while those values
 * could be overridden during the build process. In other words for bare minimum all you need is the following:
 * <pre>
 *      UnixApplicationContainerSpec unixContainer = new UnixApplicationContainerSpec("ls -all");
 *
 * 		YarnApplication yarnApplication = YarnApplicationBuilder.forApplication("myCoolYarnApp", unixContainer).build();
 *
 *      yarnApplication.launch();
 * <pre>
 *
 * @author Oleg Zhurakousky
 *
 */
public class YarnApplicationBuilder {

	private static final Log logger = LogFactory.getLog(YarnApplicationBuilder.class);

	private final static String applicationMasterFqn = YarnApplicationMasterLauncher.class.getName();

	private final String applicationName;

	private YarnConfiguration yarnConfig;

	private final AbstractApplicationContainerSpec applicationContainerSpec;

	private final Resource capability;

	private String javaCommand;

	private String queueName;

	private int maxAttempts;

	private int priority;

	/**
	 * Creates an instance of this builder initializing it with the application name and {@link AbstractApplicationContainerSpec}
	 *
	 * @param applicationName
	 * @param applicationCommand
	 * @return
	 */
	public static YarnApplicationBuilder forApplication(String applicationName, AbstractApplicationContainerSpec applicationContainerSpec){
		StringAssertUtils.assertNotEmptyAndNoSpaces(applicationName);
		ObjectAssertUtils.assertNotNull(applicationContainerSpec);

		YarnApplicationBuilder builder = new YarnApplicationBuilder(applicationName, applicationContainerSpec);
		return builder;
	}
	/**
	 *
	 * @param applicationName
	 */
	private YarnApplicationBuilder(String applicationName, AbstractApplicationContainerSpec applicationContainerSpec){
		this.applicationName = applicationName;
		this.applicationContainerSpec = applicationContainerSpec;
		this.yarnConfig = new YarnConfiguration();
		this.javaCommand = "java";

		this.capability = Records.newRecord(Resource.class);
		this.capability.setMemory(2048);
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
	 * Absolute path to a java command. By default it will simply use 'java', but in the cases where you
	 * have multiple JVMs installed you may want to invoke this setter to point to a specific one.
	 * This setting has no effect if using local mini-cluster with oz.hadoop.yarn.test.cluster.InJvmContainerExecutor
	 * configured as 'yarn.nodemanager.container-executor.class' since execution of Application Master will
	 * happen in the same JVM as the server.
	 *
	 * @param javaCommand
	 * @return
	 */
	public YarnApplicationBuilder setJavaCommandAbsolutePath(String javaCommand) {
		StringAssertUtils.assertNotEmptyAndNoSpaces(javaCommand);
		this.javaCommand = javaCommand;
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
	 * Will build {@link YarnApplication} from values provided in this builder.
	 * Any change to this builder's values after calling this method will not affect the
	 * newly created {@link YarnApplication}
	 *
	 * @return
	 */
	public YarnApplication build(){
		return new YarnApplication() {
			private final YarnClient yarnClient = YarnClient.createYarnClient();

			/**
			 *
			 */
			@Override
			public boolean launch() {
				this.preCheck();
				this.startYarnClient();

				YarnClientApplication yarnClientApplication = this.createYarnClientApplication();

			    ApplicationSubmissionContext appContext = this.initApplicationContext(yarnClientApplication);

			    logger.info("Deploying ApplicationMaster");
			    try {
			    	this.yarnClient.submitApplication(appContext);
				}
			    catch (Exception e) {
					throw new IllegalStateException("Failed to deploy application: " + YarnApplicationBuilder.this.applicationName, e);
				}
			    return true;
			}

			/**
			 *
			 */
			@Override
			public boolean terminate() {
				throw new UnsupportedOperationException("This method is currently unimplemented. Check later");
			}

			/**
			 * Any type of pre-check you want to perform before launching Application Master
			 * mainly for the purpose of logging warning messages
			 */
			private void preCheck(){
				if (YarnApplicationBuilder.this.capability.getVirtualCores() > 1){
					if (!YarnApplicationBuilder.this.yarnConfig.get(YarnConfiguration.RM_SCHEDULER).equals(FairScheduler.class.getName())){
						logger.warn("Based on current Hadoop implementation " +
								"'vcore' settings are ignored for schedulers other then FairScheduler");
					}
				}
			}

			/**
			 *
			 * @return
			 */
			private YarnClientApplication createYarnClientApplication(){
				try {
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
			    appContext.setApplicationName(YarnApplicationBuilder.this.applicationName);
			    ApplicationId appId = appContext.getApplicationId();

			    ContainerLaunchContext containerLaunchContext = this.createContainerLaunchContext(appId.getId());

			    Map<String, LocalResource> localResources = this.createLocalResources(appId);
			    if (logger.isDebugEnabled()){
			    	logger.debug("Created LocalResources: " + localResources);
			    }
			    containerLaunchContext.setLocalResources(localResources);

			    String encodedArguments = YarnApplicationBuilder.this.applicationContainerSpec.toBase64EncodedJsonString();
			    String applicationMasterLaunchCommand = this.createApplicationMasterLaunchCommand(localResources, encodedArguments);
			    containerLaunchContext.setCommands(Collections.singletonList(applicationMasterLaunchCommand));

				Priority priority = Records.newRecord(Priority.class);
			    priority.setPriority(YarnApplicationBuilder.this.priority);
			    appContext.setResource(YarnApplicationBuilder.this.capability);
			    appContext.setMaxAppAttempts(YarnApplicationBuilder.this.maxAttempts);
			    appContext.setAMContainerSpec(containerLaunchContext);
			    appContext.setPriority(priority);
			    appContext.setQueue(YarnApplicationBuilder.this.queueName);

			    if (logger.isInfoEnabled()){
			    	logger.info("Created ApplicationSubmissionContext: " + appContext);
			    }

				return appContext;
			}

			/**
			 *
			 */
			private ContainerLaunchContext createContainerLaunchContext(int applicationId) {
				ContainerLaunchContext containerLaunchContext = Records.newRecord(ContainerLaunchContext.class);
				YarnApplicationBuilder.this.applicationContainerSpec.addToContainerSpec(AbstractApplicationContainerSpec.APPLICATION_NAME, YarnApplicationBuilder.this.applicationName);
				YarnApplicationBuilder.this.applicationContainerSpec.addToContainerSpec(AbstractApplicationContainerSpec.APPLICATION_ID, applicationId);

				if (YarnApplicationBuilder.this.applicationContainerSpec instanceof JavaApplicationContainerSpec){
					YarnApplicationBuilder.this.applicationContainerSpec.addToContainerSpec(AbstractApplicationContainerSpec.CONTAINER_TYPE, "JAVA");
				}

				if (logger.isInfoEnabled()) {
					logger.info("Application container spec: " + YarnApplicationBuilder.this.applicationContainerSpec.toJsonString());
				}

				YayaUtils.inJvmPrep("JAVA", containerLaunchContext, YarnApplicationBuilder.applicationMasterFqn, YarnApplicationBuilder.this.applicationContainerSpec.toBase64EncodedJsonString());

			    return containerLaunchContext;
			}

			/**
			 * Will package this application JAR in {@link LocalResource}s.
			 * TODO make it more general to allow other resources
			 */
			private Map<String, LocalResource> createLocalResources(ApplicationId appId) {
				Map<String, LocalResource> localResources = new LinkedHashMap<String, LocalResource>();

				try {
					FileSystem fs = FileSystem.get(YarnApplicationBuilder.this.yarnConfig);

					String[] cp = System.getProperty("java.class.path").split(":");
					for (String v : cp) {
						File f = new File(v);
						if (f.isDirectory()) {
							String jarFileName = YayaUtils.generateJarFileName(YarnApplicationBuilder.this.applicationName);
							if (logger.isDebugEnabled()){
								logger.debug("Creating JAR: " + jarFileName);
							}
							File jarFile = JarUtils.toJar(f, jarFileName);
							this.addToLocalResources(fs, jarFile.getAbsolutePath(),jarFile.getName(), appId.getId(), localResources);
							try {
								new File(jarFile.getAbsolutePath()).delete(); // will delete the generated JAR file
							}
							catch (Exception e) {
								logger.warn("Failed to delete generated JAR file: " + jarFile.getAbsolutePath(), e);
							}
						}
						else {
							this.addToLocalResources(fs, f.getAbsolutePath(), f.getName(), appId.getId(), localResources);
						}
					}
				}
			    catch (Exception e) {
					throw new IllegalStateException(e);
				}
				return localResources;
			}

			/**
			 * Will generate the final launch command for this ApplicationMaster
			 */
			private String createApplicationMasterLaunchCommand(Map<String, LocalResource> localResources, String containerSpecStr) {
				String classpath = YayaUtils.calculateClassPath(localResources);
				if (logger.isInfoEnabled()){
					logger.info("Application master classpath: " + classpath);
				}

				String applicationMasterLaunchCommand = YayaUtils.generateExecutionCommand(
						YarnApplicationBuilder.this.javaCommand + " -cp ",
						classpath,
						YarnApplicationBuilder.applicationMasterFqn,
						containerSpecStr,
						YarnApplicationBuilder.this.applicationName,
						"_AM_");

				if (logger.isInfoEnabled()){
					logger.info("Application Master launch command: " + applicationMasterLaunchCommand);
				}

			    return applicationMasterLaunchCommand;
			}

			/**
			 *
			 */
			private void startYarnClient() {
				this.yarnClient.init(YarnApplicationBuilder.this.yarnConfig);
				this.yarnClient.start();
				logger.info("Started YarnClient");
			}

			/**
			 *
			 */
			private void addToLocalResources(FileSystem fs, String fileSrcPath, String fileDstPath, int appId, Map<String, LocalResource> localResources) {
				String suffix = YarnApplicationBuilder.this.applicationName + "_master/" + appId + "/" + fileDstPath;
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
		};
	}
}
