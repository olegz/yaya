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

import java.nio.ByteBuffer;

import org.apache.hadoop.yarn.conf.YarnConfiguration;

import oz.hadoop.yarn.api.ApplicationContainerProcessor;
import oz.hadoop.yarn.api.YarnApplication;
import oz.hadoop.yarn.api.YarnAssembly;

/**
 * Demo of Application Container(s) implemented as Java process and runs in 
 * YARN Cluster
 * 
 * There is an identical demo that runs in YARN Cluster. Please see 
 * JavaBasedYarnApplicationEmulatorDemo.java in this package.
 * 
 * @author Oleg Zhurakousky
 *
 */
public class JavaBasedYarnApplicationClusterDemo {
	
	/**
	 * Before running ensure that properly configured yarn-site.xml are copied
	 * into src/main/resources. You can use the yarn-site.xml from local-config
	 * directory of this project. The newly checkout out project is already
	 * setup for this.
	 * Examples for remote configurations are located in remote-config directory,
	 * but you might as well use the ones from your installed cluster.
	 *
	 * If running in Mini-Cluster (see yarn-test-cluster project), make sure you start it
	 * by executing StartMiniCluster.java first.
	 */
	public static void main(String[] args) throws Exception {
		YarnApplication<Void> yarnApplication = YarnAssembly.forApplicationContainer(ReverseMessageContainer.class, ByteBuffer.wrap("Hello Yarn!".getBytes())).
								containerCount(4).
								withApplicationMaster(new YarnConfiguration()).
									maxAttempts(2).
									build("JavaBasedYarnApplicationDemo");
		
		yarnApplication.launch();
		System.out.println();
		/*
		 * This demo demonstrates self-shutdown where application will exit
		 * upon completion of tasks by all containers.
		 */
	}
	
	/**
	 * As name suggests this ApplicationContainerProcessor will reverse the input message printing it to 
	 * the logs.
	 */
	public static class ReverseMessageContainer implements ApplicationContainerProcessor {
		@Override
		public ByteBuffer process(ByteBuffer inputMessage) {
			inputMessage.rewind();
			byte[] inputBytes = new byte[inputMessage.limit()];
			inputMessage.get(inputBytes);
			String strMessage = new String(inputBytes);
			strMessage = new StringBuilder(strMessage).reverse().toString();
			System.out.println("Processing input: " + strMessage);
			try {
				Thread.sleep(5000);
			} catch (Exception e) {
				// TODO: handle exception
			}
			return null;
			// You can also return ByteBuffer, but since its a finite container
			// the contents of the returned ByteBuffer will be logged (see JavaBasedYarnApplicationEmulatorDemo)
			
			//return ByteBuffer.wrap(strMessage.getBytes());
		}
	}
	
}
