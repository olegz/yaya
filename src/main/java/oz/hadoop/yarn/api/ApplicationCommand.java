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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import oz.hadoop.yarn.api.utils.NumberAssertUtils;
import oz.hadoop.yarn.api.utils.ObjectAssertUtils;
import oz.hadoop.yarn.api.utils.StringAssertUtils;

/**
 *
 * @author Oleg Zhurakousky
 *
 */
public class ApplicationCommand {
	public static final String COMMAND = "command";
	public static final String CONTAINER_COUNT = "containerCount";
	public static final String MEMORY = "memory";
	public static final String VIRTUAL_CORES = "virtualCores";
	public static final String PRIORITY = "priority";
	public static final String AM_SPEC = "amSpec";

	private static final Log logger = LogFactory.getLog(ApplicationCommand.class);

	private final HashMap<String, Object> varArgs;

	/**
	 * If command contains multiple tokens (e.g., "java -cp foo.har HelloWorld"), it
	 * will be wrapped in single quotes.
	 *
	 * @param command
	 */
	public ApplicationCommand(String command){
		StringAssertUtils.assertNotEmpty(command);
		this.varArgs = new LinkedHashMap<String, Object>();

		if (command.contains(" ")){
			this.varArgs.put(COMMAND, "'" + command + "'");
		}
		else {
			this.varArgs.put(COMMAND, command);
		}

		this.varArgs.put(CONTAINER_COUNT, 1);
		this.varArgs.put(MEMORY, 128);
		this.varArgs.put(VIRTUAL_CORES, 1);
		this.varArgs.put(PRIORITY, 0);
		this.varArgs.put(AM_SPEC, ApplicationMasterSpec.class.getName());
	}

	public void setContainerCount(int containerCount) {
		NumberAssertUtils.assertZeroOrPositive(containerCount);
		this.varArgs.put(CONTAINER_COUNT, containerCount);
	}

	public void setMemory(int memory){
		NumberAssertUtils.assertZeroOrPositive(memory);
		this.varArgs.put(MEMORY, memory);
	}

	public void setVirtualCores(int virtualCores){
		NumberAssertUtils.assertZeroOrPositive(virtualCores);
		this.varArgs.put(VIRTUAL_CORES, virtualCores);
	}

	public void setPriority(int priority){
		NumberAssertUtils.assertZeroOrPositive(priority);
		this.varArgs.put(PRIORITY, priority);
	}

	public void setApplicationMasterSpectClass(Class<ApplicationMasterSpec> applicationMasterSpecClass){
		ObjectAssertUtils.assertNotNull(applicationMasterSpecClass);
		this.varArgs.put(AM_SPEC, applicationMasterSpecClass.getClass().getName());
	}

	public String build(){
		StringBuffer commandBuffer = new StringBuffer();
		for (Entry<String, Object> entry : this.varArgs.entrySet()) {
			commandBuffer.append(entry.getKey());
			commandBuffer.append(" ");
			commandBuffer.append(entry.getValue());
			commandBuffer.append(" ");
		}
		String finalCommand = commandBuffer.toString();
		logger.info("Built command: " + finalCommand);
		return finalCommand;
	}
}
