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

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import oz.hadoop.yarn.api.ApplicationContainerProcessor;
import oz.hadoop.yarn.api.ContainerReplyListener;
import oz.hadoop.yarn.api.DataProcessor;
import oz.hadoop.yarn.api.YarnApplication;
import oz.hadoop.yarn.api.YarnAssembly;
import oz.hadoop.yarn.api.YayaConstants;
import oz.hadoop.yarn.api.utils.ConfigUtils;
import oz.hadoop.yarn.api.utils.MiniClusterUtils;
import oz.hadoop.yarn.api.utils.ReflectionUtils;

/**
 * @author Oleg Zhurakousky
 *
 */
//@Ignore
public class Clustered_ApplicationMasterTests {
	
	@Before
	public void before(){
		MiniClusterUtils.startMiniCluster();
	}
	
	@After
	public void after(){
		MiniClusterUtils.stoptMiniCluster();
	}
	
	@Test
	public void validateFiniteContainerSelfShutdown() throws Exception {
		YarnApplication<Void> yarnApplication = YarnAssembly.forApplicationContainer(ContainerWithRandomDelay.class, ByteBuffer.wrap("hello".getBytes())).
							containerCount(4).
							withApplicationMaster(new YarnConfiguration()).build("validateFiniteContainerSelfShutdown");
		yarnApplication.registerReplyListener(new ContainerReplyListener() {
			@Override
			public void onReply(ByteBuffer replyData) {
				byte[] replyBytes = new byte[replyData.limit()];
				replyData.rewind();
				replyData.get(replyBytes);
				replyData.rewind();
				String reply = new String(replyBytes);
				System.out.println("REPLY: " + reply);
			}
		});
		yarnApplication.launch();
		assertTrue(yarnApplication.isRunning());
		yarnApplication.awaitFinish();
		assertFalse(yarnApplication.isRunning());
	}
	
	//========
	@Test
	public void validateFiniteContainerSelfShutdownWithProcessException() throws Exception {
//		ConfigUtils.addToClasspath(new File("/Users/oleg/HADOOP_DEV/yaya/yarn-test-cluster/src/main/resources"));
		YarnApplication<Void> yarnApplication = YarnAssembly.forApplicationContainer(ContainerThrowingExceptionRandomly.class, ByteBuffer.wrap("hello".getBytes())).
							containerCount(4).
							withApplicationMaster(new YarnConfiguration()).build("validateFiniteContainerSelfShutdownWithProcessException");
		yarnApplication.registerReplyListener(new ContainerReplyListener() {
			@Override
			public void onReply(ByteBuffer replyData) {
				byte[] replyBytes = new byte[replyData.limit()];
				replyData.rewind();
				replyData.get(replyBytes);
				replyData.rewind();
				String reply = new String(replyBytes);
				System.out.println("REPLY: " + reply);
			}
		});
		yarnApplication.launch();
		assertTrue(yarnApplication.isRunning());
		yarnApplication.awaitFinish();
		assertFalse(yarnApplication.isRunning());
	}
	
	@Test
	public void validateReusableContainerGracefulShutdownNoTaskSubmitted() throws Exception {
//		ConfigUtils.addToClasspath(new File("/Users/oleg/HADOOP_DEV/yaya/yarn-test-cluster/src/main/resources"));
		YarnApplication<DataProcessor> yarnApplication = YarnAssembly.forApplicationContainer(ContainerWithRandomDelay.class).
				containerCount(4).
				withApplicationMaster(new YarnConfiguration()).build("validateReusableContainerGracefulShutdownNoTaskSubmitted");
		yarnApplication.launch();
		assertTrue(yarnApplication.isRunning());
		yarnApplication.shutDown();
		assertFalse(yarnApplication.isRunning());
	}
	
	/**
	 * Validates that application with long running containers can be shut down gracefully (via shutdown method)
	 * after all currently submitted tasks have finished
	 */
	@Test
	public void validateReusableContainerGracefulShutdownAfterAllTasksSubmitted() throws Exception {
//		ConfigUtils.addToClasspath(new File("/Users/oleg/HADOOP_DEV/yaya/yarn-test-cluster/src/main/resources"));
		YarnApplication<DataProcessor> yarnApplication = YarnAssembly.forApplicationContainer(ContainerWithRandomDelay.class).
				containerCount(4).
				withApplicationMaster(new YarnConfiguration()).build("validateReusableContainerGracefulShutdownAfterAllTasksSubmitted");
		yarnApplication.registerReplyListener(new ContainerReplyListener() {
			@Override
			public void onReply(ByteBuffer replyData) {
				byte[] replyBytes = new byte[replyData.limit()];
				replyData.rewind();
				replyData.get(replyBytes);
				replyData.rewind();
				String reply = new String(replyBytes);
				System.out.println("REPLY: " + reply);
			}
		});
		DataProcessor dataProcessor = yarnApplication.launch();
		assertTrue(yarnApplication.isRunning());
		for (int i = 0; i < 10; i++) {
			dataProcessor.process(ByteBuffer.wrap(("hello-" + i).getBytes()));
		}
		yarnApplication.shutDown();
		assertFalse(yarnApplication.isRunning());
	}
	
	/**
	 * Validates ability for long running containers with long running tasks to terminate.
	 * In this test there should see no REPLY messages coming in.
	 */
	@Test
	public void validateReusableContainerTermination() throws Exception {
//		ConfigUtils.addToClasspath(new File("/Users/oleg/HADOOP_DEV/yaya/yarn-test-cluster/src/main/resources"));
		final YarnApplication<DataProcessor> yarnApplication = YarnAssembly.forApplicationContainer(ContainerWithLongRunningLoop.class).
				containerCount(4).
				withApplicationMaster(new YarnConfiguration()).build("validateReusableContainerTermination");
		final AtomicBoolean replies = new AtomicBoolean();
		yarnApplication.registerReplyListener(new ContainerReplyListener() {
			@Override
			public void onReply(ByteBuffer replyData) {
				replies.set(true);
			}
		});
		final DataProcessor dataProcessor = yarnApplication.launch();
		assertTrue(yarnApplication.isRunning());
		ExecutorService executor = Executors.newCachedThreadPool();
		executor.execute(new Runnable() {		
			@Override
			public void run() {
				for (int i = 0; i < 10; i++) {
					dataProcessor.process(ByteBuffer.wrap(("hello-" + i).getBytes()));
				}
			}
		});
		Thread.sleep(2000);// let it run for a while and then terminate
		executor.execute(new Runnable() {	
			@Override
			public void run() {
				yarnApplication.shutDown();
			}
		});
		Thread.sleep(2000);
		assertTrue(yarnApplication.isRunning());
		yarnApplication.terminate();
		assertFalse(yarnApplication.isRunning());
		assertFalse(replies.get());
		executor.shutdownNow();
	}
	
	/**
	 * Validates that a finite container with a long running task can be terminated
	 */
	@Test
	public void validateFiniteContainerTermination() throws Exception {
//		ConfigUtils.addToClasspath(new File("/Users/oleg/HADOOP_DEV/yaya/yarn-test-cluster/src/main/resources"));
		final YarnApplication<Void> yarnApplication = YarnAssembly.forApplicationContainer(ContainerWithLongRunningLoop.class, ByteBuffer.wrap("hello".getBytes())).
							containerCount(2).
							withApplicationMaster(new YarnConfiguration()).build("validateFiniteContainerTermination");
		yarnApplication.registerReplyListener(new ContainerReplyListener() {
			@Override
			public void onReply(ByteBuffer replyData) {
				byte[] replyBytes = new byte[replyData.limit()];
				replyData.rewind();
				replyData.get(replyBytes);
				replyData.rewind();
				String reply = new String(replyBytes);
				System.out.println("REPLY: " + reply);
			}
		});
		ExecutorService executor = Executors.newCachedThreadPool();
		executor.execute(new Runnable() {
			@Override
			public void run() {
				yarnApplication.launch();
				System.out.println("STARTED");
			}
		});
		yarnApplication.awaitLaunch();
		assertTrue(yarnApplication.isRunning());

		Thread.sleep(4000);// let it run for a while and then terminate
		executor.execute(new Runnable() {	
			@Override
			public void run() {
				yarnApplication.shutDown();
			}
		});

		Thread.sleep(4000); // ensure that it is still running
		assertTrue(yarnApplication.isRunning());
		yarnApplication.terminate();
		assertFalse(yarnApplication.isRunning());
	}
	
	/**
	 * This test validates that finite application exits immediately upon container startup failure
	 * 
	 * @throws Exception
	 */
	@Test
	public void validateApplicationSelfExitAfterContaierStartupFailure() throws Exception {
//		ConfigUtils.addToClasspath(new File("/Users/oleg/HADOOP_DEV/yaya/yarn-test-cluster/src/main/resources"));
		
		YarnApplication<Void> yarnApplication = YarnAssembly.forApplicationContainer(ContainerWithLongRunningLoop.class, ByteBuffer.wrap("hello".getBytes())).
				containerCount(8).
				withApplicationMaster(new YarnConfiguration()).build("validateApplicationSelfExitAfterContaierStartupFailure");
		
		Map<String, Object> specificationMap = new HashMap<>(yarnApplication.getApplicationSpecification());
		specificationMap.put("FORCE_CONTAINER_ERROR", true);
		specificationMap.put(YayaConstants.YARN_CONFIG, new YarnConfiguration());
		
		Constructor<?> launcherCtr = ReflectionUtils.getInvocableConstructor("oz.hadoop.yarn.api.core.ApplicationMasterLauncherImpl", Map.class);
		Object launcher = launcherCtr.newInstance(specificationMap);
		Method launchMethod = ReflectionUtils.getMethodAndMakeAccessible(launcher.getClass(), "launch");
		launchMethod.invoke(launcher);
		//Thread.sleep(2000);
		assertFalse(yarnApplication.isRunning());
	}
	
	/**
	 * 
	 */
	public static class ContainerWithRandomDelay implements ApplicationContainerProcessor {
		@Override
		public ByteBuffer process(ByteBuffer inputMessage) {
			System.out.println("In ContainerWithRandomDelay");
			try {
				Thread.sleep(new Random().nextInt(5000));
			} catch (Exception e) {
				// TODO: handle exception
			}
			return inputMessage;
		}
	}
	
	/**
	 * 
	 */
	public static class ContainerWithLongRunningLoop implements ApplicationContainerProcessor {
		@Override
		public ByteBuffer process(ByteBuffer inputMessage) {
			System.out.println("In ContainerWithLongRunningLoop");
			for (int i = 0; i < 1000000; i++) {
				try {
					System.out.println("Blah Blah");
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new IllegalStateException("Interrupted");
				}
			}
			return inputMessage;
		}
	}

	/**
	 * 
	 */
	public static class ContainerThrowingExceptionRandomly implements ApplicationContainerProcessor {
		@Override
		public ByteBuffer process(ByteBuffer inputMessage) {
			if (new Random().nextInt(4) % 2 == 0){
				System.out.println("THROWING EXCEPTION");
				throw new RuntimeException("Intentional");
			}
			else {
				System.out.println("SUCCESS");
				return inputMessage;
			}
		}
	}
}
