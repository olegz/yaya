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

import org.apache.hadoop.yarn.conf.YarnConfiguration;

import oz.hadoop.yarn.api.YarnAssembly;
import oz.hadoop.yarn.api.YarnApplication;

/**
 * Demo of Application Container(s) implemented as non-Java process.
 * 
 * It is setup to run in the valid cluster
 * 
 * There is an identical demo that runs in YARN Emulator. Please see 
 * CommandBasedYarnApplicationEmulatorDemo.java in this package.
 * 
 * @author Oleg Zhurakousky
 *
 */
public class CommandBasedYarnApplicationClusterDemo {
	
	/**
	 * Before running ensure that properly configured yarn-site.xml are copied
	 * into src/main/resources. You can use the yarn-site.xml from local-config
	 * directory of this project. The newly checkout out project is already
	 * setup for this.
	 * Examples for remote configurations are located in remote-config directory,
	 * but you might as well use the ones from your installed cluster.
	 *
	 * Also, make sure you start local YARN cluster by executing
	 * StartMiniCluster.java first.
	 */
	public static void main(String[] args) throws Exception {
		YarnConfiguration yarnConfiguration = new YarnConfiguration();
		YarnApplication<Void> yarnApplication = YarnAssembly.forApplicationContainer("ping -c 4 yahoo.com").
								containerCount(2).
								memory(512).withApplicationMaster(yarnConfiguration).
									maxAttempts(2).
									priority(2).
									build("CommandBasedYarnApplicationDemo");
		
		yarnApplication.launch();
		yarnApplication.shutDown();
		/*
		 * If running in the local mini-cluster check target/LOCAL_YARN_CLUSTER directory of mini-cluster project 
		 * for application logs
		 */
	}
	
}
