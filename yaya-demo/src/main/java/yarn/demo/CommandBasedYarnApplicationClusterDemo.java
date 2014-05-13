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

import java.io.File;

import org.apache.hadoop.yarn.conf.YarnConfiguration;

import oz.hadoop.yarn.api.YarnApplication;
import oz.hadoop.yarn.api.YarnAssembly;
import oz.hadoop.yarn.api.utils.ConfigUtils;
import demo.utils.MiniClusterUtils;

/**
 * Demo of Application Container(s) implemented as non-Java process.
 * 
 * It is setup to run in the Mini Cluster which will be built (if needed), 
 * started and stopped as part of this demo.
 * To test this demo with a real cluster comment/remove DemoUtils.start*stop* methods
 * and call ConfigUtils.setConfig(..) with valid configuration file or directory with configuration files 
 * pointing to your cluster. Sample configuration files for remote cluster are located in 
 * remote-config directory of this project.
 * 
 * There is an identical demo that runs in YARN Emulator. Please see 
 * CommandBasedYarnApplicationEmulatorDemo.java in this package.
 * 
 * @author Oleg Zhurakousky
 *
 */
public class CommandBasedYarnApplicationClusterDemo {
	
	/**
	 * If running in Mini-Cluster (see yarn-test-cluster project), make sure you start it
	 * by executing StartMiniCluster.java first.
	 */
	public static void main(String[] args) throws Exception {
		MiniClusterUtils.startMiniCluster();

		ConfigUtils.addToClasspath(new File("mini-cluster-config"));

		YarnApplication<Void> yarnApplication = YarnAssembly.forApplicationContainer("ping -c 4 google.com").
								containerCount(4).
								withApplicationMaster(new YarnConfiguration()).
									build("CommandBasedYarnApplicationDemo");
		
		yarnApplication.launch();
		yarnApplication.awaitFinish();
		
		MiniClusterUtils.stoptMiniCluster();
	}
}
