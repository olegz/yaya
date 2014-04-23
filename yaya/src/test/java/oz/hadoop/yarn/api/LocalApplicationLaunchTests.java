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

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.junit.Test;
import org.springframework.core.io.ClassPathResource;

/**
 * @author Oleg Zhurakousky
 *
 */
public class LocalApplicationLaunchTests {
	
	private static AtomicInteger flag = new AtomicInteger();
	
	@Test(timeout=10000)
	public void validateWithReplyListener() throws Exception {
		YarnApplication<DataProcessor> yarnApplication = YarnAssembly.forApplicationContainer(SimpleEchoContainer.class).
												containerCount(2).
												memory(512).withApplicationMaster().
													maxAttempts(2).
													priority(2).
													build("sample-yarn-application");
		
		DataProcessor dataProcessor = yarnApplication.launch();
		final AtomicInteger repliesCounter = new AtomicInteger();
		dataProcessor.registerReplyListener(new DataProcessorReplyListener() {
			@Override
			public void onReply(ByteBuffer replyData) {
				repliesCounter.incrementAndGet();
			}
		});
		assertEquals(2, dataProcessor.containers());
		
		for (int i = 0; i < 2; i++) {
			for (int j = 0; j < dataProcessor.containers(); j++) {
				dataProcessor.process(ByteBuffer.wrap(("Hello Yarn!-" + i).getBytes()));
			}
		}
		assertTrue(yarnApplication.isRunning());
		yarnApplication.shutDown();
		assertEquals(repliesCounter.get(), dataProcessor.completedSinceStart());
		assertFalse(yarnApplication.isRunning());
	}
	
	@Test(timeout=60000)
	public void validateLongLivedJavaContainerLaunch() throws Exception {
		YarnApplication<DataProcessor> yarnApplication = YarnAssembly.forApplicationContainer(SimpleRandomDelayContainer.class).
												containerCount(2).
												memory(512).withApplicationMaster().
													maxAttempts(2).
													priority(2).
													build("sample-yarn-application");
		
		DataProcessor dataProcessor = yarnApplication.launch();
		assertEquals(2, dataProcessor.containers());
		
		for (int i = 0; i < 5; i++) {
			for (int j = 0; j < dataProcessor.containers(); j++) {
				dataProcessor.process(ByteBuffer.wrap(("Hello Yarn!-" + i).getBytes()));
			}
		}
		assertTrue(yarnApplication.isRunning());
		yarnApplication.shutDown();
		assertFalse(yarnApplication.isRunning());
	}
	
	@Test(timeout=60000)
	public void validateLongLivedJavaContainerLaunchWithGracefullShutdown() throws Exception {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		YarnApplication<DataProcessor> yarnApplication = YarnAssembly.forApplicationContainer(SimpleRandomDelayContainer.class).
												containerCount(4).
												withApplicationMaster().
													build("sample-yarn-application");
		
		final DataProcessor dataProcessor = yarnApplication.launch();
		assertEquals(4, dataProcessor.containers());
		executor.execute(new Runnable() {	
			@Override
			public void run() {
				for (int i = 0; i < 1000000; i++) {
					for (int j = 0; j < dataProcessor.containers(); j++) {
						try {
							dataProcessor.process(ByteBuffer.wrap(("Hello Yarn!-" + i).getBytes()));
						} 
						catch (Exception e) {
							e.printStackTrace();
							throw new IllegalStateException("Failed to submit data for processing", e);
						}
					}
				}
			}
		});
		
		assertTrue(yarnApplication.isRunning());
		Thread.sleep(new Random().nextInt(5000));
		yarnApplication.shutDown();
		assertFalse(yarnApplication.isRunning());
	}
	
	@Test(timeout=30000)
	public void validateLongLivedJavaContainerLaunchWithTermination() throws Exception {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		YarnApplication<DataProcessor> yarnApplication = YarnAssembly.forApplicationContainer(SimpleRandomDelayContainer.class).
												containerCount(4).
												withApplicationMaster().
													build("sample-yarn-application");
		
		final DataProcessor dataProcessor = yarnApplication.launch();
		assertEquals(4, dataProcessor.containers());
		executor.execute(new Runnable() {	
			@Override
			public void run() {
				for (int i = 0; i < 1000000; i++) {
					for (int j = 0; j < dataProcessor.containers(); j++) {
						try {
							dataProcessor.process(ByteBuffer.wrap(("Hello Yarn!-" + i).getBytes()));
						} 
						catch (Exception e) {
							e.printStackTrace();
							throw new IllegalStateException("Failed to submit data for processing", e);
						}
					}
				}
			}
		});
		
		assertTrue(yarnApplication.isRunning());
		Thread.sleep(new Random().nextInt(5000));
		yarnApplication.terminate();
		assertFalse(yarnApplication.isRunning());
	}
	
	@Test(timeout=20000)
	public void validateJavaContainerLaunchImmediateShutdown() throws Exception {
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
	
	@Test(timeout=20000)
	public void validateJavaContainerLaunchSelfShutdown() throws Exception {
		YarnApplication<Void> yarnApplication = YarnAssembly.forApplicationContainer(SimpleEchoContainer.class, ByteBuffer.wrap("Hello".getBytes())).
												containerCount(2).
												memory(512).withApplicationMaster().
													maxAttempts(2).
													priority(2).
													build("sample-yarn-application");
		assertFalse(yarnApplication.isRunning());
		yarnApplication.launch();
		assertTrue(yarnApplication.isRunning());
		Thread.sleep(10000);
		assertEquals(0, yarnApplication.liveContainers());
		// note, its going to assert true to not running without explicit shutdown 
		assertFalse(yarnApplication.isRunning());
	}
	
	@Test(timeout=5000)
	public void validateInfiniteJavaContainerLaunchForcedShutdown() throws Exception {
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
		yarnApplication.terminate();
		assertFalse(yarnApplication.isRunning());
	}
	
	@Test(timeout=5000)
	public void validateJavaContainerLaunchAndVariableProcessTimeWithForcedShutdown() throws Exception {
		YarnApplication<Void> yarnApplication = YarnAssembly.forApplicationContainer(VariableProcessingTime.class, ByteBuffer.wrap("Hello".getBytes())).
												containerCount(6).
												memory(512).withApplicationMaster().
													maxAttempts(2).
													priority(2).
													build("sample-yarn-application");
		yarnApplication.launch();
		assertEquals(6, yarnApplication.liveContainers());
		while (yarnApplication.liveContainers() == 6){
			LockSupport.parkNanos(100000);
		}
		assertTrue(yarnApplication.liveContainers() < 6);
		assertTrue(yarnApplication.isRunning());
		yarnApplication.terminate();
		assertEquals(0, yarnApplication.liveContainers());
		assertFalse(yarnApplication.isRunning());
	}
	
	@Test(timeout=3000)
	public void validateContainerLaunchWithCommandSelfShutdown() throws Exception {
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
	}
	
	@Test(timeout=5000)
	public void validateContainerLaunchWithInfiniteCommandForcedShutdown() throws Exception {
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
		yarnApplication.terminate();
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
	
	/**
	 * 
	 */
	public static class SimpleRandomDelayContainer implements ApplicationContainerProcessor {
		@Override
		public ByteBuffer process(ByteBuffer inputMessage) {
			try {
				Thread.sleep(new Random().nextInt(10000));
			} catch (Exception e) {
				e.printStackTrace();
			}
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
					if (loopCount == 30){
						System.out.println("LOOPING: " + i);
						Thread.sleep(5000);
					}
					else {
						Thread.sleep(500);
					}
				}
			} catch (Exception e) {
				// ignore
			}
			return inputMessage;
		}
	}
}
