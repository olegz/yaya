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

import oz.hadoop.yarn.api.utils.StringAssertUtils;

/**
 * @author Oleg Zhurakousky
 *
 */
public class UnixApplicationContainerSpec extends AbstractApplicationContainerSpec {

	public final static String COMMAND = "command";

	/**
	 *
	 * @param command
	 */
	public UnixApplicationContainerSpec(String command) {
		super();
		StringAssertUtils.assertNotEmpty(command);
		StringAssertUtils.assertNotEmpty(command.trim());
		this.addToContainerSpec(COMMAND, command);
	}

	public String getCommand() {
		return (String) this.containerSpec.get(COMMAND);
	}
}