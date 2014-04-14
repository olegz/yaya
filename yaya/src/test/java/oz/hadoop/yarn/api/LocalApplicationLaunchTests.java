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

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.Future;

import org.junit.Test;

import oz.hadoop.yarn.api.net.ContainerDelegate;

/**
 * @author Oleg Zhurakousky
 *
 */
public class LocalApplicationLaunchTests {

	@Test(timeout=5000)
	public void validatehLongLivedJavaContainerLaunch() throws Exception {
		YarnApplication<ContainerDelegate[]> yarnApplication = YarnAssembly.forApplicationContainer(SimpleEchoContainer.class).
												containerCount(2).
												memory(512).withApplicationMaster().
													maxAttempts(2).
													priority(2).
													build("sample-yarn-application");
		
		ContainerDelegate[] containerDelegates = yarnApplication.launch();
		assertEquals(2, containerDelegates.length);
		
		for (ContainerDelegate containerDelegate : containerDelegates) {
			InetAddress containersAddress = InetAddress.getByName(containerDelegate.getHost());
			assertTrue(containersAddress.isReachable(1000));
		}
		for (int i = 0; i < 5; i++) {
			for (ContainerDelegate containerDelegate : containerDelegates) {
				Future<ByteBuffer> reply = containerDelegate.exchange(ByteBuffer.wrap(("Hello Yarn!-" + i).getBytes()));
				ByteBuffer r = reply.get();
				byte[] replyBytes = new byte[r.limit()];
				r.get(replyBytes);
				assertEquals("Hello Yarn!-" + i, new String(replyBytes));
			}
		}
		yarnApplication.shutDown();
	}
	
	@Test(timeout=2000)
	public void validateJavaContainerLaunchWithArguments() throws Exception {
		YarnApplication<Void> yarnApplication = YarnAssembly.forApplicationContainer(SimpleEchoContainer.class, ByteBuffer.wrap("Hello".getBytes())).
												containerCount(2).
												memory(512).withApplicationMaster().
													maxAttempts(2).
													priority(2).
													build("sample-yarn-application");
		yarnApplication.launch();
		yarnApplication.shutDown();
	}
	
	@Test//(timeout=2000)
	public void validateContainerLaunchWithCommand() throws Exception {
		YarnApplication<Void> yarnApplication = YarnAssembly.forApplicationContainer("ping -c 4 yahoo.com").
												containerCount(2).
												memory(512).withApplicationMaster().
													maxAttempts(2).
													priority(2).
													build("sample-yarn-application");
		
		yarnApplication.launch();
		Thread.sleep(5000);
		yarnApplication.shutDown();
	}
	
	/**
	 * 
	 */
	public static class SimpleEchoContainer implements ApplicationContainer {
		@Override
		public ByteBuffer process(ByteBuffer inputMessage) {
			return inputMessage;
		}
	}
}
