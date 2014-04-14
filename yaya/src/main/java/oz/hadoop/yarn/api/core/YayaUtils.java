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

import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;

import oz.hadoop.yarn.api.YayaConstants;

/**
 * Internal utilities used only by the framework. Not for public use.
 *
 * @author Oleg Zhurakousky
 *
 */
public class YayaUtils {

	/**
	 *
	 * @param localResources
	 * @return
	 */
	public static String calculateClassPath(Map<String, LocalResource> localResources) {
		StringBuffer buffer = new StringBuffer();
		for (String resource : localResources.keySet()) {
			buffer.append("./" + resource + ":");
		}
		String classpath = buffer.toString();
		return classpath;
	}
	/**
	 *
	 * @param applicationName
	 * @return
	 */
	public static String generateJarFileName(String applicationName){
		StringBuffer nameBuffer = new StringBuffer();
		nameBuffer.append(applicationName);
		nameBuffer.append("_");
		nameBuffer.append(UUID.randomUUID().toString());
		nameBuffer.append(".jar");
		return nameBuffer.toString();
	}

	/**
	 *
	 * @param containerType
	 * @param containerLaunchContext
	 * @param containerLauncherName
	 * @param containerArguments
	 */
	public static void inJvmPrep(String containerType, ContainerLaunchContext containerLaunchContext, String containerLauncherName, String containerArguments){
		Map<String, String> environment = containerLaunchContext.getEnvironment();
		environment.put(YayaConstants.CONTAINER_TYPE, "JAVA");
		environment.put(YayaConstants.CONTAINER_LAUNCHER, containerLauncherName);
		environment.put(YayaConstants.CONTAINER_ARG, containerArguments);
	}

	/**
	 * TODO !!!!! Looks pretty ugly. Needs work
	 *
	 * @param javaCommand
	 * @param main
	 * @param arguments
	 * @param classpath
	 * @param applicationName
	 * @param qualifier
	 * @return
	 */
	public static String generateExecutionCommand(String shellCommand, String classpath, String main, String arguments, String applicationName, String qualifier){
		StringBuffer commandBuffer = new StringBuffer();
		commandBuffer.append(shellCommand);
		commandBuffer.append(classpath);
		commandBuffer.append(" ");
		commandBuffer.append(main);
		commandBuffer.append(" ");
		commandBuffer.append(arguments);
		commandBuffer.append(" ");
		commandBuffer.append(main);
		commandBuffer.append(" ");
		commandBuffer.append(" 1>");
		commandBuffer.append(ApplicationConstants.LOG_DIR_EXPANSION_VAR);
		commandBuffer.append("/");
		commandBuffer.append(applicationName);
		commandBuffer.append(qualifier);
		commandBuffer.append("_out");
		commandBuffer.append(" 2>");
		commandBuffer.append(ApplicationConstants.LOG_DIR_EXPANSION_VAR);
		commandBuffer.append("/");
		commandBuffer.append(applicationName);
		commandBuffer.append(qualifier);
		commandBuffer.append("_err");
		return commandBuffer.toString();
	}
}
