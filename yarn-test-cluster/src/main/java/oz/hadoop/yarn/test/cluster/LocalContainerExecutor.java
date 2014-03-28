/*
 * Copyright 2013 the original author or authors.
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

import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.CreateFlag.OVERWRITE;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerDiagnosticsUpdateEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.util.ConverterUtils;

/**
 * @author Oleg Zhurakousky
 *
 */
public class LocalContainerExecutor extends DefaultContainerExecutor {
	private static final Log LOG = LogFactory
		      .getLog(DefaultContainerExecutor.class);

	private final FileContext fc;
	static final short APPDIR_PERM = (short)0710;

	public LocalContainerExecutor(){
		try {
			this.fc = FileContext.getLocalFSFileContext();
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	@Override
	public int launchContainer(Container container,
			Path nmPrivateContainerScriptPath, Path nmPrivateTokensPath,
			String userName, String appId, Path containerWorkDir,
			List<String> localDirs, List<String> logDirs) throws IOException {

		//HADOOP_TOKEN_FILE_LOCATION
		///Users/oleg/HADOOP_DEV/git/yarn-tutorial/yarn-test-cluster/target/LOCAL_YARN_CLUSTER/LOCAL_YARN_CLUSTER-localDir-nm-0_0/usercache/oleg/appcache/application_1395848295806_0001/container_1395848295806_0001_01_000001
//		containerWorkDir = new Path(System.getProperty("user.dir"));
		this.prep(container, nmPrivateContainerScriptPath, nmPrivateTokensPath, userName, appId, containerWorkDir, localDirs, logDirs);

		System.out.println(System.getProperty("user.dir"));

		System.out.println(containerWorkDir.toString());
		System.setProperty("user.dir", containerWorkDir.toString());
		System.out.println(System.getProperty("user.dir"));

		Set<Path> paths = this.getIncomingClassPathEntries(container);
		String currentClassPath = System.getProperty("java.class.path");
		Set<URL> additionalClassPathUrls = new HashSet<>();
		for (Path path : paths) {
			String resourceName = path.getName();
			if (currentClassPath.contains(resourceName)) {
				System.out.println(resourceName
						+ " is already in the classpath. Skipping");
			} else {
				System.out.println(resourceName
						+ " is not in the classpath. Adding");
				additionalClassPathUrls.add(path.toUri().toURL());
			}
		}
		System.out.println(additionalClassPathUrls);

		URLClassLoader cl = new URLClassLoader(additionalClassPathUrls.toArray(new URL[] {}));

		try {
			Class amClass = Class.forName("oz.hadoop.yarn.api.YarnApplicationMaster", true, cl);
			Method mainMethod = amClass.getMethod("main",new Class[] { String[].class });
			mainMethod.setAccessible(true);
			String[] arguments = "containerCount 1 memory 128 virtualCores 1 priority 0 amSpec oz.hadoop.yarn.api.ApplicationMasterSpec command cal".split(" ");
			mainMethod.invoke(null, (Object) arguments);
			System.out.println();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}

	private void prep(Container container, Path nmPrivateContainerScriptPath,
			Path nmPrivateTokensPath, String userName, String appId,
			Path containerWorkDir, List<String> localDirs, List<String> logDirs) {
		try {
			FsPermission dirPerm = new FsPermission(APPDIR_PERM);
		    ContainerId containerId = container.getContainerId();

		    // create container dirs on all disks
		    String containerIdStr = ConverterUtils.toString(containerId);
		    String appIdStr =
		        ConverterUtils.toString(
		            containerId.getApplicationAttemptId().
		                getApplicationId());
		    for (String sLocalDir : localDirs) {
		      Path usersdir = new Path(sLocalDir, ContainerLocalizer.USERCACHE);
		      Path userdir = new Path(usersdir, userName);
		      Path appCacheDir = new Path(userdir, ContainerLocalizer.APPCACHE);
		      Path appDir = new Path(appCacheDir, appIdStr);
		      Path containerDir = new Path(appDir, containerIdStr);
		      createDir(containerDir, dirPerm, true);
		    }

		    // Create the container log-dirs on all disks
		    //createContainerLogDirs(appIdStr, containerIdStr, logDirs);

		    Path tmpDir = new Path(containerWorkDir,
		        YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR);
		    createDir(tmpDir, dirPerm, false);

		    // copy launch script to work dir
		    Path launchDst =
		        new Path(containerWorkDir, ContainerLaunch.CONTAINER_SCRIPT);
		    fc.util().copy(nmPrivateContainerScriptPath, launchDst);

		    // copy container tokens to work dir
		    Path tokenDst =
		      new Path(containerWorkDir, ContainerLaunch.FINAL_CONTAINER_TOKENS_FILE);
		    fc.util().copy(nmPrivateTokensPath, tokenDst);

		    // Create new local launch wrapper script
		    LocalWrapperScriptBuilder sb = Shell.WINDOWS ?
		      new WindowsLocalWrapperScriptBuilder(containerIdStr, containerWorkDir) :
		      new UnixLocalWrapperScriptBuilder(containerWorkDir);

		    // Fail fast if attempting to launch the wrapper script would fail due to
		    // Windows path length limitation.
//		    if (Shell.WINDOWS &&
//		        sb.getWrapperScriptPath().toString().length() > WIN_MAX_PATH) {
//		      throw new IOException(String.format(
//		        "Cannot launch container using script at path %s, because it exceeds " +
//		        "the maximum supported path length of %d characters.  Consider " +
//		        "configuring shorter directories in %s.", sb.getWrapperScriptPath(),
//		        WIN_MAX_PATH, YarnConfiguration.NM_LOCAL_DIRS));
//		    }

		    Path pidFile = getPidFilePath(containerId);
		    if (pidFile != null) {
		      sb.writeLocalWrapperScript(launchDst, pidFile);
		    } else {
		      LOG.info("Container " + containerIdStr
		          + " was marked as inactive. Returning terminated error");
		      throw new RuntimeException("Booooo");
		    }

		    // create log dir under app
		    // fork script
		    ShellCommandExecutor shExec = null;
		    try {
		      fc.setPermission(launchDst,
		          ContainerExecutor.TASK_LAUNCH_SCRIPT_PERMISSION);
		      fc.setPermission(sb.getWrapperScriptPath(),
		          ContainerExecutor.TASK_LAUNCH_SCRIPT_PERMISSION);

		      // Setup command to run
		      String[] command = getRunCommand(sb.getWrapperScriptPath().toString(),
		        containerIdStr, this.getConf());

		      LOG.info("launchContainer: " + Arrays.toString(command));
		      shExec = new ShellCommandExecutor(
		          command,
		          new File(containerWorkDir.toUri().getPath()),
		          container.getLaunchContext().getEnvironment());      // sanitized env
		      if (isContainerActive(containerId)) {
		        shExec.execute();
		      }
		      else {
		        LOG.info("Container " + containerIdStr +
		            " was marked as inactive. Returning terminated error");
		        throw new RuntimeException("Booooo1");
		      }
		    } catch (IOException e) {
		      if (null == shExec) {
		    	  throw new RuntimeException("Booooo2");
		      }
		      int exitCode = shExec.getExitCode();
		      LOG.warn("Exit code from container " + containerId + " is : " + exitCode);
		      // 143 (SIGTERM) and 137 (SIGKILL) exit codes means the container was
		      // terminated/killed forcefully. In all other cases, log the
		      // container-executor's output
		      if (exitCode != ExitCode.FORCE_KILLED.getExitCode()
		          && exitCode != ExitCode.TERMINATED.getExitCode()) {
		        LOG.warn("Exception from container-launch with container ID: "
		            + containerId + " and exit code: " + exitCode , e);
		        logOutput(shExec.getOutput());
		        String diagnostics = "Exception from container-launch: "
		            + e + "\n"
		            + StringUtils.stringifyException(e) + "\n" + shExec.getOutput();
		        container.handle(new ContainerDiagnosticsUpdateEvent(containerId,
		            diagnostics));
		      } else {
		        container.handle(new ContainerDiagnosticsUpdateEvent(containerId,
		            "Container killed on request. Exit code is " + exitCode));
		      }
		      return;
		    } finally {
		      ; //
		    }
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	private Set<Path> getIncomingClassPathEntries(Container container) {
		try {
			Field lf = container.getClass().getDeclaredField(
					"localizedResources");
			lf.setAccessible(true);
			Map<Path, List<String>> localizedResources = (Map<Path, List<String>>) lf
					.get(container);
			Set<Path> paths = localizedResources.keySet();
			return paths;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private void createDir(Path dirPath, FsPermission perms, boolean createParent) throws IOException {
		fc.mkdir(dirPath, perms, createParent);
		if (!perms.equals(perms.applyUMask(fc.getUMask()))) {
			fc.setPermission(dirPath, perms);
		}
	}

	 private abstract class LocalWrapperScriptBuilder {

		    private final Path wrapperScriptPath;

		    public Path getWrapperScriptPath() {
		      return wrapperScriptPath;
		    }

		    public void writeLocalWrapperScript(Path launchDst, Path pidFile) throws IOException {
		      DataOutputStream out = null;
		      PrintStream pout = null;

		      try {
		        out = fc.create(wrapperScriptPath, EnumSet.of(CREATE, OVERWRITE));
		        pout = new PrintStream(out);
		        writeLocalWrapperScript(launchDst, pidFile, pout);
		      } finally {
		        IOUtils.cleanup(LOG, pout, out);
		      }
		    }

		    protected abstract void writeLocalWrapperScript(Path launchDst, Path pidFile,
		        PrintStream pout);

		    protected LocalWrapperScriptBuilder(Path containerWorkDir) {
		      this.wrapperScriptPath = new Path(containerWorkDir,
		        Shell.appendScriptExtension("default_container_executor"));
		    }
		  }

		  private final class UnixLocalWrapperScriptBuilder
		      extends LocalWrapperScriptBuilder {

		    public UnixLocalWrapperScriptBuilder(Path containerWorkDir) {
		      super(containerWorkDir);
		    }

		    @Override
		    public void writeLocalWrapperScript(Path launchDst, Path pidFile,
		        PrintStream pout) {

		      // We need to do a move as writing to a file is not atomic
		      // Process reading a file being written to may get garbled data
		      // hence write pid to tmp file first followed by a mv
		      pout.println("#!/bin/bash");
		      pout.println();
		      pout.println("echo $$ > " + pidFile.toString() + ".tmp");
		      pout.println("/bin/mv -f " + pidFile.toString() + ".tmp " + pidFile);
		      String exec = Shell.isSetsidAvailable? "exec setsid" : "exec";
		      pout.println(exec + " /bin/bash \"" +
		        launchDst.toUri().getPath().toString() + "\"");
		    }
		  }

		  private final class WindowsLocalWrapperScriptBuilder
		      extends LocalWrapperScriptBuilder {

		    private final String containerIdStr;

		    public WindowsLocalWrapperScriptBuilder(String containerIdStr,
		        Path containerWorkDir) {

		      super(containerWorkDir);
		      this.containerIdStr = containerIdStr;
		    }

		    @Override
		    public void writeLocalWrapperScript(Path launchDst, Path pidFile,
		        PrintStream pout) {

		      // On Windows, the pid is the container ID, so that it can also serve as
		      // the name of the job object created by winutils for task management.
		      // Write to temp file followed by atomic move.
		      String normalizedPidFile = new File(pidFile.toString()).getPath();
		      pout.println("@echo " + containerIdStr + " > " + normalizedPidFile +
		        ".tmp");
		      pout.println("@move /Y " + normalizedPidFile + ".tmp " +
		        normalizedPidFile);
		      pout.println("@call " + launchDst.toString());
		    }
		  }
}
