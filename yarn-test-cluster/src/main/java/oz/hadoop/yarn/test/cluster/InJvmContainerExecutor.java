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
package oz.hadoop.yarn.test.cluster;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.util.ConverterUtils;

/**
 * !!!!! FOR TESTING WITH MINI CLUSTER ONLY !!!!!
 *
 * Container launcher which will launch Java container in the same JVM.
 * Non JAVA containers (e.g., unix command) launch requests will be delegated to
 * its super class {@link DefaultContainerExecutor}.
 *
 * In order to use it you must override 'yarn.nodemanager.container-executor.class' property
 * in the server configuration (e.g., mini cluster) and set it to the fully qualified name of this
 * class
 *
 * @author Oleg Zhurakousky
 *
 */
public class InJvmContainerExecutor extends DefaultContainerExecutor {

	private static final Log logger = LogFactory.getLog(InJvmContainerExecutor.class);

	private static final String[] additionalClassPathExclusions = new String[]{
		"junit",
		"hemcrest",
		"mockito",
		"easymock"
	};

	/**
	 * Copied from super class
	 * Permissions for user app dir. $local.dir/usercache/$user/appcache/$appId
	 */
	static final short APPDIR_PERM = (short) 0777;
	/**
	 * Copied from super class
	 * Permissions for user log dir. $logdir/$user/$appId
	 */
	static final short LOGDIR_PERM = (short) 0777;

	private final FileContext fc;

	/**
	 *
	 */
	public InJvmContainerExecutor() {
		try {
			this.fc = FileContext.getLocalFSFileContext();
		}
		catch (UnsupportedFileSystemException e) {
			throw new IllegalStateException(e);
		}
	}

	/**
	 *
	 */
	@Override
	public int launchContainer(final Container container,
			Path nmPrivateContainerScriptPath, Path nmPrivateTokensPath,
			String userName, String appId, final Path containerWorkDir,
			List<String> localDirs, List<String> logDirs) throws IOException {

		if ("JAVA".equalsIgnoreCase(container.getLaunchContext().getEnvironment().get("CONTAINER_TYPE"))){
			this.prepareContainerDirectories(container, nmPrivateContainerScriptPath, nmPrivateTokensPath,
					userName, appId, containerWorkDir, localDirs, logDirs);
			return this.launchJavaContainer(container, containerWorkDir);
		}
		else {
			return super.launchContainer(container, nmPrivateContainerScriptPath, nmPrivateTokensPath, userName, appId, containerWorkDir, localDirs, logDirs);
		}
	}

	/**
	 *
	 */
	private int launchJavaContainer(final Container container, final Path containerWorkDir){
		UserGroupInformation ugi = this.buildUgiForContainerLaunching(container, containerWorkDir);
		return ugi.doAs(new PrivilegedAction<Integer>() {
			@Override
			public Integer run() {
				return InJvmContainerExecutor.this.doLaunch(container, containerWorkDir);
			}
		});
	}

	/**
	 *
	 */
	private int doLaunch(Container container, Path containerWorkDir) {
		Set<Path> paths = this.getIncomingClassPathEntries(container);
		String currentClassPath = System.getProperty("java.class.path");
		final Set<URL> additionalClassPathUrls = new HashSet<>();
		if (logger.isDebugEnabled()){
			logger.debug("Building additional classpath for the container: " + container);
		}
		List<String> ae = Arrays.asList(additionalClassPathExclusions); // for logging purposes
		for (Path path : paths) {
			String resourceName = path.getName();
			if (currentClassPath.contains(resourceName)) {
				if (logger.isDebugEnabled()){
					logger.debug("\t skipping " + resourceName + ". Already in the classpath.");
				}
			}
			else {
				if (!this.shouldExclude(path.getName())){
					if (logger.isDebugEnabled()){
						logger.debug("\t adding " + resourceName + " to the classpath");
					}
					try {
						additionalClassPathUrls.add(path.toUri().toURL());
					} catch (Exception e) {
						throw new IllegalArgumentException(e);
					}
				}
				else {
					if (logger.isDebugEnabled()){
						logger.debug("Excluding " + path.getName() + " based on 'additionalClassPathExclusions': " + ae);
					}
				}
			}
		}

		Map<String, String> environment = container.getLaunchContext().getEnvironment();
		try {
			URLClassLoader cl = new URLClassLoader(additionalClassPathUrls.toArray(new URL[] {}));
			String main = environment.get("MAIN");
			Class<?> amClass = Class.forName(main, true, cl);
			Method mainMethod = amClass.getMethod("main", new Class[] {String[].class});
			mainMethod.setAccessible(true);
			String mainArgs = environment.get("MAIN_ARG");
			//containerCount, memory, virtualCores, priority, amSpec, command
			String[] arguments = mainArgs.split("\t");

			mainMethod.invoke(null, (Object) arguments);
			System.out.println();
		}
		catch (Exception e) {
			logger.error("Failed to laumch container " + container, e);
			return -9;
		}
		return 0;
	}

	/**
	 *
	 */
	private boolean shouldExclude(String jar){
		for (String value : additionalClassPathExclusions) {
			if (jar.contains(value)){
				return true;
			}
		}
		return false;
	}

	/**
	 *
	 * @param container
	 * @param containerWorkDir
	 * @return
	 */
	private UserGroupInformation buildUgiForContainerLaunching(Container container, final Path containerWorkDir) {
		UserGroupInformation ugi;
		try {
			ugi = UserGroupInformation.getCurrentUser();
		    ugi.setAuthenticationMethod(AuthMethod.TOKEN);
			String filePath = new Path(containerWorkDir,ContainerLaunch.FINAL_CONTAINER_TOKENS_FILE).toString();
			Credentials credentials = Credentials.readTokenStorageFile(new File(filePath), this.getConf());
			Collection<Token<? extends TokenIdentifier>> tokens = credentials.getAllTokens();
			for (Token<? extends TokenIdentifier> token : tokens) {
				ugi.addToken(token);
			}
		}
		catch (Exception e) {
			throw new IllegalArgumentException("Failed to build UserGroupInformation to launch container " + container, e);
		}
		return ugi;
	}

	/**
	 * Most of this code is copied from the super class's launchContainer method (unfortunately), since directory
	 * and other preparation logic is tightly coupled with the actual container launch.
	 * Would be nice if it was broken apart where launch method would be invoked when
	 * everything is prepared
	 */
	private void prepareContainerDirectories(Container container,
			Path nmPrivateContainerScriptPath, Path nmPrivateTokensPath,
			String userName, String appId, Path containerWorkDir,
			List<String> localDirs, List<String> logDirs) {

		FsPermission dirPerm = new FsPermission(APPDIR_PERM);
		ContainerId containerId = container.getContainerId();
		String containerIdStr = ConverterUtils.toString(containerId);
		String appIdStr = ConverterUtils.toString(containerId.getApplicationAttemptId().getApplicationId());

		try {
			for (String sLocalDir : localDirs) {
				Path usersdir = new Path(sLocalDir, ContainerLocalizer.USERCACHE);
				Path userdir = new Path(usersdir, userName);
				Path appCacheDir = new Path(userdir, ContainerLocalizer.APPCACHE);
				Path appDir = new Path(appCacheDir, appIdStr);
				Path containerDir = new Path(appDir, containerIdStr);
				createDir(containerDir, dirPerm, true);
			}

			// Create the container log-dirs on all disks
			this.createLogDirs(appIdStr, containerIdStr, logDirs);

			Path tmpDir = new Path(containerWorkDir,YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR);
			createDir(tmpDir, dirPerm, false);

			// copy launch script to work dir
			Path launchDst = new Path(containerWorkDir,ContainerLaunch.CONTAINER_SCRIPT);
			fc.util().copy(nmPrivateContainerScriptPath, launchDst);

			// copy container tokens to work dir
			Path tokenDst = new Path(containerWorkDir,ContainerLaunch.FINAL_CONTAINER_TOKENS_FILE);
			fc.util().copy(nmPrivateTokensPath, tokenDst);
		}
		catch (Exception e) {
			throw new IllegalStateException("Failed to prepare container directories for container " + container, e);
		}
	}

	/**
	 * Copied from super class
	 */
	private void createDir(Path dirPath, FsPermission perms, boolean createParent) throws IOException {
		fc.mkdir(dirPath, perms, createParent);
		if (!perms.equals(perms.applyUMask(fc.getUMask()))) {
			fc.setPermission(dirPath, perms);
		}
	}


	/**
	 * Copied from super class
	 * Create application log directories on all disks.
	 */
	private void createLogDirs(String appId, String containerId,List<String> logDirs) throws IOException {

		boolean containerLogDirStatus = false;
		FsPermission containerLogDirPerms = new FsPermission(LOGDIR_PERM);
		for (String rootLogDir : logDirs) {
			// create $log.dir/$appid/$containerid
			Path appLogDir = new Path(rootLogDir, appId);
			Path containerLogDir = new Path(appLogDir, containerId);
			try {
				createDir(containerLogDir, containerLogDirPerms, true);
			}
			catch (IOException e) {
				logger.warn("Unable to create the container-log directory : " + appLogDir, e);
				continue;
			}
			containerLogDirStatus = true;
		}
		if (!containerLogDirStatus) {
			throw new IOException("Not able to initialize container-log directories "
							+ "in any of the configured local directories for container " + containerId);
		}
	}

	/**
	 *
	 * @param container
	 * @return
	 */
	private Set<Path> getIncomingClassPathEntries(Container container) {
		Map<Path, List<String>> localizedResources = this.getLocalResources(container);
		Set<Path> paths = localizedResources.keySet();
		return paths;
	}

	/**
	 *
	 * @param container
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private Map<Path, List<String>> getLocalResources(Container container) {
		Map<Path, List<String>> localizedResources;
		try {
			Field lf = container.getClass().getDeclaredField("localizedResources");
			lf.setAccessible(true);
			localizedResources = (Map<Path, List<String>>) lf.get(container);
			return localizedResources;
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private void setEnv(Map<String, String> newenv) {
		try {
			Class<?> processEnvironmentClass = Class
					.forName("java.lang.ProcessEnvironment");
			Field theEnvironmentField = processEnvironmentClass
					.getDeclaredField("theEnvironment");
			theEnvironmentField.setAccessible(true);
			Map<String, String> env = (Map<String, String>) theEnvironmentField
					.get(null);
			env.putAll(newenv);
			Field theCaseInsensitiveEnvironmentField = processEnvironmentClass
					.getDeclaredField("theCaseInsensitiveEnvironment");
			theCaseInsensitiveEnvironmentField.setAccessible(true);
			Map<String, String> cienv = (Map<String, String>) theCaseInsensitiveEnvironmentField
					.get(null);
			cienv.putAll(newenv);
		} catch (NoSuchFieldException e) {
			try {
				Class[] classes = Collections.class.getDeclaredClasses();
				Map<String, String> env = System.getenv();
				for (Class cl : classes) {
					if ("java.util.Collections$UnmodifiableMap".equals(cl
							.getName())) {
						Field field = cl.getDeclaredField("m");
						field.setAccessible(true);
						Object obj = field.get(env);
						Map<String, String> map = (Map<String, String>) obj;
						map.clear();
						map.putAll(newenv);
					}
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		} catch (Exception e1) {
			e1.printStackTrace();
		}
	}
}
