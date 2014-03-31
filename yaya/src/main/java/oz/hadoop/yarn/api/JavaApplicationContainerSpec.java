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

import oz.hadoop.yarn.api.utils.ObjectAssertUtils;
import oz.hadoop.yarn.api.utils.StringAssertUtils;

/**
 * A specification class for defining a Java based YARN Application Container
 *
 * @author Oleg Zhurakousky
 *
 */
public class JavaApplicationContainerSpec extends UnixApplicationContainerSpec {

	/**
	 * @param command
	 */
	public JavaApplicationContainerSpec(Class<? extends JavaApplicationContainer> javaApplicationContainer) {
		super("java");
		ObjectAssertUtils.assertNotNull(javaApplicationContainer);
		this.addToContainerSpec(CONTAINER_IMPL, javaApplicationContainer.getName());
	}

	/**
	 * Sets the the command to invoke Java program. Default is 'java', but
	 * could be overridden with the full path to java executable in the environments
	 * where more then one JVM present.
	 *
	 * @param javaCommand
	 */
	public void setJavaLaunchCommand(String javaLaunchCommand) {
		StringAssertUtils.assertNotEmptyAndNoSpaces(javaLaunchCommand);
		this.addToContainerSpec(COMMAND, javaLaunchCommand);
	}
}
