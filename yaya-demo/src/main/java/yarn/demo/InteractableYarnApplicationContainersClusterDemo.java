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
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.yarn.conf.YarnConfiguration;

import oz.hadoop.yarn.api.ApplicationContainerProcessor;
import oz.hadoop.yarn.api.DataProcessor;
import oz.hadoop.yarn.api.YarnApplication;
import oz.hadoop.yarn.api.YarnAssembly;

/**
 * This demo showcases long-running reusable containers you can interact with
 * by exchanging messages. This one (while trivial) demonstrates a simple YARN
 * application which echoes back the message it receives and the calling client
 * prints the echoed message.
 * 
 * This demo requires a valid YARN cluster (mini-cluster or full cluster) provided
 * through YarnConfiguration. 
 * 
 * There is an identical demo that runs in YARN Emulator. Please see 
 * InteractableYarnApplicationContainersEmulatorDemo.java in this package.
 * 
 * @author Oleg Zhurakousky
 * 
 */
public class InteractableYarnApplicationContainersClusterDemo {

	/**
	 * Ensure valid YarnConfiguration is available in the classpath, then run.
	 */
	public static void main(String[] args) throws Exception {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		
		YarnApplication<DataProcessor> yarnApplication = YarnAssembly.forApplicationContainer(DemoEchoContainer.class).
				containerCount(8).
				withApplicationMaster(new YarnConfiguration()).
					maxAttempts(2).
					build("InteractableYarnApplicationContainersClusterDemo");

		/*
		 * DataProcessor essentially is a proxy over all Application Containers running in YARN.
		 * It is aware of which Application Containers are available and will 
		 * delegate its process(..) invocation to the first available Application Container.
		 * So essentially DataProcessor is a gateway to the YARN Distributed Computing Grid.
		 * 
		 * Additionally you can register oz.hadoop.yarn.api.DataProcessorReplyListener with 
		 * DataProcessor if interested in receiving a reply from the distributed process.
		 */
		final DataProcessor dataProcessor = yarnApplication.launch();
		executor.execute(new Runnable() {
			@Override
			public void run() {
				for (int i = 0; i < 30; i++) {
					/*
					 * Actual processing will be delegated to the first available (out of 8 deployed) 
					 * remote Application Container
					 */
					dataProcessor.process(ByteBuffer.wrap(("Hello Yarn Grid!" + i).getBytes()));
				}
			}
		});
		
		Thread.sleep(2000); //let it run for a bit and then shutdown
		/*
		 * NOTE: This is a graceful shutdown, letting 
		 * currently running Application Containers to finish, while
		 * not accepting any more. So you may see a "Rejecting submission..." message in the logs.
		 */
		yarnApplication.shutDown();
		System.out.println("Processes completed since launch: " + dataProcessor.completedSinceStart());
		executor.shutdown();
	}
	
	/**
	 * 
	 */
	public static class DemoEchoContainer implements ApplicationContainerProcessor {
		@Override
		public ByteBuffer process(ByteBuffer inputMessage) {
			try {
				Thread.sleep(new Random().nextInt(3000));
				System.out.println("Echoing. . .");
			} catch (Exception e) {
				// TODO: handle exception
			}
			return inputMessage;
		}
	}
}
