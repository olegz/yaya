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

/**
 *
 * @author Oleg Zhurakousky
 *
 */
public abstract class AbstractApplicationCommand {
	public static final String COMMAND = "command";
	public static final String CONTAINER_COUNT = "containerCount";
	public static final String MEMORY = "memory";
	public static final String VIRTUAL_CORES = "virtualCores";
	public static final String PRIORITY = "priority";
	public static final String AM_SPEC = "amSpec";

	private static final Log logger = LogFactory.getLog(AbstractApplicationCommand.class);

	private final HashMap<String, Object> containerArguments;


	public AbstractApplicationCommand(){
		this.containerArguments = new LinkedHashMap<String, Object>();
		this.containerArguments.put(CONTAINER_COUNT, 1);
		this.containerArguments.put(MEMORY, 128);
		this.containerArguments.put(VIRTUAL_CORES, 1);
		this.containerArguments.put(PRIORITY, 0);
		this.containerArguments.put(AM_SPEC, ApplicationMasterSpec.class.getName());
	}

	public void setContainerCount(int containerCount) {
		NumberAssertUtils.assertZeroOrPositive(containerCount);
		this.containerArguments.put(CONTAINER_COUNT, containerCount);
	}

	public void setMemory(int memory){
		NumberAssertUtils.assertZeroOrPositive(memory);
		this.containerArguments.put(MEMORY, memory);
	}

	public void setVirtualCores(int virtualCores){
		NumberAssertUtils.assertZeroOrPositive(virtualCores);
		this.containerArguments.put(VIRTUAL_CORES, virtualCores);
	}

	public void setPriority(int priority){
		NumberAssertUtils.assertZeroOrPositive(priority);
		this.containerArguments.put(PRIORITY, priority);
	}

	public void setApplicationMasterSpecClass(Class<ApplicationMasterSpec> applicationMasterSpecClass){
		ObjectAssertUtils.assertNotNull(applicationMasterSpecClass);
		this.containerArguments.put(AM_SPEC, applicationMasterSpecClass.getClass().getName());
	}

	protected void setFinalCommand(String command){
		if (command.contains(" ")){
			this.setContainerArgument(COMMAND, "'" + command + "'");
		}
		else {
			this.setContainerArgument(COMMAND, command);
		}
	}

	protected void setContainerArgument(String name, String value) {
		this.containerArguments.put(name, value);
	}

	protected String build(){
		StringBuffer commandBuffer = new StringBuffer();
		for (Entry<String, Object> entry : this.containerArguments.entrySet()) {
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
