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
import java.util.concurrent.Future;

import org.apache.hadoop.yarn.conf.YarnConfiguration;

import oz.hadoop.yarn.api.ApplicationContainer;
import oz.hadoop.yarn.api.YarnApplication;
import oz.hadoop.yarn.api.YarnAssembly;
import oz.hadoop.yarn.api.net.ContainerDelegate;

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
		YarnConfiguration yarnConfiguration = new YarnConfiguration();
		YarnApplication<ContainerDelegate[]> yarnApplication = YarnAssembly.forApplicationContainer(DemoEchoContainer.class).
				containerCount(2).
				memory(256).withApplicationMaster(yarnConfiguration).
					maxAttempts(2).
					memory(512).
					build("InteractableYarnApplicationContainersDemo");

		ContainerDelegate[] containerDelegates = yarnApplication.launch();
		for (int i = 0; i < 5; i++) {
			for (ContainerDelegate containerDelegate : containerDelegates) {
				Future<ByteBuffer> reply = containerDelegate.exchange(ByteBuffer.wrap(("Hello Yarn!-" + i).getBytes()));
				ByteBuffer r = reply.get();
				byte[] replyBytes = new byte[r.limit()];
				r.get(replyBytes);
				System.out.println("Reply: " + new String(replyBytes));
			}
		}
		yarnApplication.shutDown();
	}
	
	/**
	 * 
	 */
	public static class DemoEchoContainer implements ApplicationContainer {
		@Override
		public ByteBuffer process(ByteBuffer inputMessage) {
			return inputMessage;
		}
	}
}
