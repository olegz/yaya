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
package oz.hadoop.yarn.api.core;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.junit.Ignore;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;

import oz.hadoop.yarn.api.ApplicationContainerProcessor;
import oz.hadoop.yarn.api.DataProcessor;
import oz.hadoop.yarn.api.ContainerReplyListener;
import oz.hadoop.yarn.api.YarnApplication;
import oz.hadoop.yarn.api.YarnAssembly;

/**
 * @author Oleg Zhurakousky
 *
 */
public class LocalApplicationLaunchTests {
	
	private static AtomicInteger flag = new AtomicInteger();
	
	//@Test//(timeout=10000)
	public static void main(String[] args) throws Exception {
		YarnApplication<DataProcessor> yarnApplication = YarnAssembly.forApplicationContainer(SimpleEchoContainer.class).
												containerCount(2).
												withApplicationMaster().
													build("sample-yarn-application");
		
		DataProcessor dataProcessor = yarnApplication.launch();
	
		
		yarnApplication.shutDown();
		System.out.println();
		
	}
	
	@Test//(timeout=10000)
	public void validateWithReplyListener() throws Exception {
		final AtomicInteger repliesCounter = new AtomicInteger();
		YarnApplication<DataProcessor> yarnApplication = YarnAssembly.forApplicationContainer(SimpleEchoContainer.class).
												containerCount(2).
												memory(512).withApplicationMaster().
													maxAttempts(2).
													priority(2).
													build("sample-yarn-application");
		yarnApplication.registerReplyListener(new ContainerReplyListener() {
			@Override
			public void onReply(ByteBuffer replyData) {
				repliesCounter.incrementAndGet();
			}
		});
		
		DataProcessor dataProcessor = yarnApplication.launch();
	
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
	
	@Test(timeout=2000)
	public void validateJavaContainerLaunchImmediateTermination() throws Exception {
		final YarnApplication<Void> yarnApplication = YarnAssembly.forApplicationContainer(SimpleRandomDelayContainer.class, ByteBuffer.wrap("Hello".getBytes())).
												containerCount(2).
												memory(512).withApplicationMaster().
													maxAttempts(2).
													priority(2).
													build("sample-yarn-application");
		assertFalse(yarnApplication.isRunning());
		ExecutorService executor = Executors.newCachedThreadPool();
		executor.execute(new Runnable() {	
			@Override
			public void run() {
				yarnApplication.launch();
			}
		});
		assertFalse(yarnApplication.isRunning());
		yarnApplication.terminate();
		assertEquals(0, yarnApplication.liveContainers());
		assertFalse(yarnApplication.isRunning());
		executor.shutdown();
	}
	
	@Test(timeout=2000)
	public void validateJavaContainerLaunchSelfShutdown() throws Exception {
		YarnApplication<Void> yarnApplication = YarnAssembly.forApplicationContainer(SimpleEchoContainer.class, ByteBuffer.wrap("Hello".getBytes())).
												containerCount(2).
												memory(512).withApplicationMaster().
													maxAttempts(2).
													priority(2).
													build("sample-yarn-application");
		assertFalse(yarnApplication.isRunning());
		yarnApplication.launch();
		while (yarnApplication.isRunning()){
			LockSupport.parkNanos(1000000);
		}
		assertEquals(0, yarnApplication.liveContainers());
		assertFalse(yarnApplication.isRunning());
	}
	
	@Test(timeout=5000)
	public void validateInfiniteJavaContainerLaunchForcedShutdown() throws Exception {
		final YarnApplication<Void> yarnApplication = YarnAssembly.forApplicationContainer(InfiniteContainer.class, ByteBuffer.wrap("Hello".getBytes())).
												containerCount(4).
												memory(512).withApplicationMaster().
													maxAttempts(2).
													priority(2).
													build("sample-yarn-application");
		ExecutorService executor = Executors.newCachedThreadPool();
		executor.execute(new Runnable() {	
			@Override
			public void run() {
				yarnApplication.launch();
			}
		});
		Thread.sleep(4000);
		assertTrue(yarnApplication.isRunning());
		yarnApplication.terminate();
		assertFalse(yarnApplication.isRunning());
		executor.shutdown();
	}
	
	@Test(timeout=5000)
	@Ignore // fix to adgust for API changes
	public void validateJavaContainerLaunchAndVariableProcessTimeWithForcedShutdown() throws Exception {
		final YarnApplication<Void> yarnApplication = YarnAssembly.forApplicationContainer(VariableProcessingTime.class, ByteBuffer.wrap("Hello".getBytes())).
												containerCount(6).
												memory(512).withApplicationMaster().
													maxAttempts(2).
													priority(2).
													build("sample-yarn-application");
		ExecutorService executor = Executors.newCachedThreadPool();
		executor.execute(new Runnable() {	
			@Override
			public void run() {
				yarnApplication.launch();
			}
		});
		// wait till all 6 are active
		while (yarnApplication.liveContainers() != 6){
			LockSupport.parkNanos(10000);
		}
		System.out.println("Running: " + yarnApplication.isRunning());
		// wait till some begin to shutdown
		while (yarnApplication.liveContainers() == 6){
			LockSupport.parkNanos(10000);
		}
		System.out.println("Running: " + yarnApplication.isRunning());
		assertTrue(yarnApplication.isRunning());
		yarnApplication.terminate();
		assertEquals(0, yarnApplication.liveContainers());
		assertFalse(yarnApplication.isRunning());
	}
	
	@Test(timeout=2000)
	public void validateContainerLaunchWithCommandSelfShutdown() throws Exception {
		YarnApplication<Void> yarnApplication = YarnAssembly.forApplicationContainer("date").
												containerCount(2).
												memory(512).withApplicationMaster().
													maxAttempts(2).
													priority(2).
													build("sample-yarn-application");
		
		assertEquals(0, yarnApplication.liveContainers());
		yarnApplication.launch();
		while (yarnApplication.isRunning()){
			LockSupport.parkNanos(1000000);
		}
		assertFalse(yarnApplication.isRunning());
	}
	
	@Test(timeout=2000)
	public void validateContainerLaunchWithInfiniteCommandForcedShutdown() throws Exception {
		ClassPathResource resource = new ClassPathResource("infinite", this.getClass());
		final YarnApplication<Void> yarnApplication = YarnAssembly.forApplicationContainer(resource.getFile().getAbsolutePath()).
												containerCount(3).
												memory(512).withApplicationMaster().
													maxAttempts(2).
													priority(2).
													build("sample-yarn-application");
		
		assertEquals(0, yarnApplication.liveContainers());
		assertFalse(yarnApplication.isRunning());
		
		ExecutorService executor = Executors.newCachedThreadPool();
		executor.execute(new Runnable() {	
			@Override
			public void run() {
				yarnApplication.launch();
			}
		});
		while (yarnApplication.liveContainers() != 3){
			LockSupport.parkNanos(1000);
		}
		assertTrue(yarnApplication.isRunning());
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
		while (yarnApplication.isRunning()){
			LockSupport.parkNanos(1000000);
		}
		assertFalse(yarnApplication.isRunning());
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
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new IllegalStateException("Interrupted");
			}
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
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new IllegalStateException("Interrupted");
			}
			return inputMessage;
		}
	}
}
