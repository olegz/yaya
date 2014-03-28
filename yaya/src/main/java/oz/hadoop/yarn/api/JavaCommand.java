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

import java.util.ArrayList;
import java.util.List;

import oz.hadoop.yarn.api.utils.ObjectAssertUtils;
import oz.hadoop.yarn.api.utils.StringAssertUtils;

/**
 * @author Oleg Zhurakousky
 *
 */
public class JavaCommand extends AbstractApplicationCommand {

	private final String mainClassName;

	private String javaCommand;

	private String classpath;

	private final List<String> commandArguments;

	/**
	 * @param command
	 */
	public JavaCommand(Class<?> mainClass) {
		super();
		ObjectAssertUtils.assertIsMain(mainClass);
		this.mainClassName = mainClass.getName();
		this.javaCommand = "java";
		this.commandArguments = new ArrayList<String>();
	}

	/**
	 *
	 * @param javaCommand
	 */
	public void setJavaCommand(String javaCommand) {
		StringAssertUtils.assertNotEmpty(javaCommand);
		this.javaCommand = javaCommand;
	}

	/**
	 *
	 * @param commandArgument
	 */
	public void addCommandArgument(String commandArgument){
		StringAssertUtils.assertNotEmpty(commandArgument);
		this.commandArguments.add(commandArgument);
	}

	/**
	 *
	 * @return
	 */
	@Override
	protected String build(String applicationMasterName, int id) {
		StringBuffer commandBuffer = new StringBuffer();
		commandBuffer.append(this.javaCommand);
		commandBuffer.append(" ");
		if (!StringAssertUtils.isEmpty(this.classpath)){
			commandBuffer.append("-cp ");
			commandBuffer.append(this.classpath);
			commandBuffer.append(" ");
		}

		commandBuffer.append(this.mainClassName);

		for (String commandArgument : this.commandArguments) {
			commandBuffer.append(" ");
			commandBuffer.append(commandArgument);
		}
		this.setFinalCommand(commandBuffer.toString());

		return super.build(applicationMasterName, id);
	}

	protected void setClasspath(String classpath) {
		this.classpath = classpath;
	}
}
