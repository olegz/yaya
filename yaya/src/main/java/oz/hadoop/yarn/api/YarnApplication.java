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



/**
 * Strategy for providing implementation for YARN-based applications.
 * Such implementation must encapsulate everything required (e.g., configuration, Application Master implementation etc.)
 * in order to call {@link #launch()} method and have the underlying application be successfully deployed.
 *
 * YarnApplication encapsulates YARNs Application Master container which internally encapsulates
 * client's application containers (applications).
 * See {@link AbstractApplicationContainerSpec} {@link YarnApplicationMasterLauncher} and {@link YarnApplicationBuilder} for more details.
 *
 * @author Oleg Zhurakousky
 *
 */
public interface YarnApplication {

	/**
	 * Starts YARN application
	 * @return
	 */
	public boolean launch();

	/**
	 * Stops YARN application
	 * @return
	 */
	public boolean terminate();
}
