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
package yarn.demo;

import oz.hadoop.yarn.api.YarnApplication;
import oz.hadoop.yarn.api.YarnAssembly;

/**
 * Demo of Application Container(s) implemented as non-Java process.
 * 
 * There is an identical demo that runs in YARN Cluster. Please see 
 * CommandBasedYarnApplicationClusterDemo.java in this package.
 * 
 * @author Oleg Zhurakousky
 *
 */
public class CommandBasedYarnApplicationEmulatorDemo {
	
	/**
	 * This demo will run in the YARN Emulator and requires no preparation. 
	 * Just hit run and see your output in the console.
	 */
	public static void main(String[] args) throws Exception {
		YarnApplication<Void> yarnApplication = YarnAssembly.forApplicationContainer("ping -c 4 google.com").
								containerCount(4).
								withApplicationMaster().
									build("CommandBasedYarnApplicationDemo");
		
		yarnApplication.launch();
		/*
		 * Unlike its cluster based counterpart (see CommandBasedYarnApplicationClusterDemo),
		 * this demo demonstrates self-shutdown where application will exit
		 * upon completion of tasks by all containers.
		 */
	}
}
