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
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.junit.Test;
import org.springframework.core.io.ClassPathResource;

import oz.hadoop.yarn.api.net.ContainerDelegate;

/**
 * @author Oleg Zhurakousky
 *
 */
public class LocalApplicationLaunchTests {
	
	private static AtomicInteger flag = new AtomicInteger();
	
	@Test(timeout=5000)
	public void validateLongLivedJavaContainerLaunch() throws Exception {
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
		assertTrue(yarnApplication.isRunning());
		yarnApplication.shutDown();
		assertFalse(yarnApplication.isRunning());
	}
	
	@Test(timeout=2000)
	public void validateJavaContainerLaunchWithArgumentsImediateShutdown() throws Exception {
		YarnApplication<Void> yarnApplication = YarnAssembly.forApplicationContainer(SimpleEchoContainer.class, ByteBuffer.wrap("Hello".getBytes())).
												containerCount(2).
												memory(512).withApplicationMaster().
													maxAttempts(2).
													priority(2).
													build("sample-yarn-application");
		assertFalse(yarnApplication.isRunning());
		yarnApplication.launch();
		assertTrue(yarnApplication.isRunning());
		yarnApplication.shutDown();
		assertFalse(yarnApplication.isRunning());
	}
	
	@Test(timeout=2000)
	public void validateJavaContainerLaunchWithArgumentsLettingFinish() throws Exception {
		YarnApplication<Void> yarnApplication = YarnAssembly.forApplicationContainer(SimpleEchoContainer.class, ByteBuffer.wrap("Hello".getBytes())).
												containerCount(2).
												memory(512).withApplicationMaster().
													maxAttempts(2).
													priority(2).
													build("sample-yarn-application");
		assertFalse(yarnApplication.isRunning());
		yarnApplication.launch();
		assertTrue(yarnApplication.isRunning());
		Thread.sleep(500);
		// note its going to assert true to not running without explicit shutdown 
		assertFalse(yarnApplication.isRunning());
	}
	
	@Test(timeout=5000)
	public void validateJavaContainerLaunchWithArgumentsAndForcedShutdown() throws Exception {
		YarnApplication<Void> yarnApplication = YarnAssembly.forApplicationContainer(InfiniteContainer.class, ByteBuffer.wrap("Hello".getBytes())).
												containerCount(2).
												memory(512).withApplicationMaster().
													maxAttempts(2).
													priority(2).
													build("sample-yarn-application");
		yarnApplication.launch();
		Thread.sleep(4000);
		// may be add is PartiallyRunning or somethig liek that to see which containers exited
		assertTrue(yarnApplication.isRunning());
		yarnApplication.shutDown();
		assertFalse(yarnApplication.isRunning());
	}
	
	@Test(timeout=5000)
	public void validateJavaContainerLaunchWithArgumentsAndVariableProcessTimeWithForcedShutdown() throws Exception {
		YarnApplication<Void> yarnApplication = YarnAssembly.forApplicationContainer(VariableProcessingTime.class, ByteBuffer.wrap("Hello".getBytes())).
												containerCount(6).
												memory(512).withApplicationMaster().
													maxAttempts(2).
													priority(2).
													build("sample-yarn-application");
		yarnApplication.launch();
		assertEquals(6, yarnApplication.liveContainers());
		while (yarnApplication.liveContainers() == 6){
			LockSupport.parkNanos(10000);
		}
		assertTrue(yarnApplication.liveContainers() < 6);
		assertTrue(yarnApplication.isRunning());
		yarnApplication.shutDown();
		assertEquals(0, yarnApplication.liveContainers());
		assertFalse(yarnApplication.isRunning());
	}
	
	@Test(timeout=3000)
	public void validateContainerLaunchWithCommand() throws Exception {
		YarnApplication<Void> yarnApplication = YarnAssembly.forApplicationContainer("date").
												containerCount(2).
												memory(512).withApplicationMaster().
													maxAttempts(2).
													priority(2).
													build("sample-yarn-application");
		
		assertEquals(0, yarnApplication.liveContainers());
		yarnApplication.launch();
		assertTrue(yarnApplication.isRunning());
		assertEquals(2, yarnApplication.liveContainers());
		Thread.sleep(1000);
		assertFalse(yarnApplication.isRunning());
		yarnApplication.shutDown();
	}
	
	@Test(timeout=5000)
	public void validateContainerLaunchWithInfiniteCommand() throws Exception {
		ClassPathResource resource = new ClassPathResource("infinite", this.getClass());
		YarnApplication<Void> yarnApplication = YarnAssembly.forApplicationContainer(resource.getFile().getAbsolutePath()).
												containerCount(3).
												memory(512).withApplicationMaster().
													maxAttempts(2).
													priority(2).
													build("sample-yarn-application");
		
		assertEquals(0, yarnApplication.liveContainers());
		assertFalse(yarnApplication.isRunning());
		yarnApplication.launch();
		assertTrue(yarnApplication.isRunning());
		assertEquals(3, yarnApplication.liveContainers());
		Thread.sleep(3000);
		assertEquals(3, yarnApplication.liveContainers());
		yarnApplication.shutDown();
		assertEquals(0, yarnApplication.liveContainers());
	}
	
	@Test(timeout=20000)
	public void validateContainerLaunchWithVariableLengthCommandsUntillDone() throws Exception {
		ClassPathResource resource = new ClassPathResource("variable", this.getClass());
		YarnApplication<Void> yarnApplication = YarnAssembly.forApplicationContainer(resource.getFile().getAbsolutePath()).
												containerCount(6).
												withApplicationMaster().
													build("sample-yarn-application");
		
		assertEquals(0, yarnApplication.liveContainers());
		assertFalse(yarnApplication.isRunning());
		yarnApplication.launch();
		assertTrue(yarnApplication.isRunning());
		int liveContainers = yarnApplication.liveContainers();
		assertEquals(6, liveContainers);
		do {
			Thread.sleep(100);
			int currentliveContainers = yarnApplication.liveContainers();
			if (currentliveContainers < liveContainers){
				System.out.println("Current live containers = " + currentliveContainers);
				liveContainers = currentliveContainers;
			}
		} while (liveContainers > 0);
		assertEquals(0, yarnApplication.liveContainers());
		assertFalse(yarnApplication.isRunning());
		yarnApplication.shutDown();
	}
	
	/**
	 * 
	 */
	public static class SimpleEchoContainer implements ApplicationContainerProcessor {
		@Override
		public ByteBuffer process(ByteBuffer inputMessage) {
			return inputMessage;
		}
	}
	
	public static class InfiniteContainer implements ApplicationContainerProcessor {
		@Override
		public ByteBuffer process(ByteBuffer inputMessage) {
			try {
				while (true){
					Thread.sleep(1000);
				}
			} catch (Exception e) {
				// ignore
			}
			return inputMessage;
		}
	}
	
	public static class VariableProcessingTime implements ApplicationContainerProcessor {
		int loopCount = flag.incrementAndGet() * 5;
		
		@Override
		public ByteBuffer process(ByteBuffer inputMessage) {
			try {
				System.out.println("Will loop for " + loopCount);
				for (int i = 0; i < loopCount; i++) {
					Thread.sleep(500);
				}
			} catch (Exception e) {
				// ignore
			}
			return inputMessage;
		}
	}
}
