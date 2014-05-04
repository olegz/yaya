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
import static junit.framework.Assert.fail;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import oz.hadoop.yarn.api.ApplicationContainerProcessor;
import oz.hadoop.yarn.api.ContainerReplyListener;
import oz.hadoop.yarn.api.DataProcessor;
import oz.hadoop.yarn.api.YarnApplication;
import oz.hadoop.yarn.api.YarnAssembly;
import oz.hadoop.yarn.api.YayaConstants;
import oz.hadoop.yarn.api.utils.MiniClusterUtils;
import oz.hadoop.yarn.api.utils.ReflectionUtils;

/**
 * @author Oleg Zhurakousky
 *
 */
@Ignore
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
	
	@Test
	public void validateFiniteContainerSelfShutdownWithProcessException() throws Exception {
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
		YarnApplication<DataProcessor> yarnApplication = YarnAssembly.forApplicationContainer(ContainerWithRandomDelay.class).
				containerCount(4).
				withApplicationMaster(new YarnConfiguration()).build("validateReusableContainerGracefulShutdownNoTaskSubmitted");
		yarnApplication.launch();
		assertTrue(yarnApplication.isRunning());
		yarnApplication.shutDown();
		assertFalse(yarnApplication.isRunning());
	}
	
	@Test
	public void validateReusableContainerGracefulShutdownAfterAllTasksSubmitted() throws Exception {
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
	
	@Test
	public void validateReusableContainerShuttingDownBeforeAllTasksFinished() throws Exception {
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
		yarnApplication.shutDown();
		assertFalse(yarnApplication.isRunning());
	}
	
	/**
	 * Should see no REPLY messages coming in.
	 * @throws Exception
	 */
	@Test
	public void validateReusableContainerTerminated() throws Exception {
		YarnApplication<DataProcessor> yarnApplication = YarnAssembly.forApplicationContainer(ContainerWithLongRunningLoop.class).
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
		yarnApplication.terminate();
		assertFalse(yarnApplication.isRunning());
		executor.shutdown();
	}
	
	@Test
	public void validateFiniteContainerWithInfiniteLoopForceShutdown() throws Exception {
		final YarnApplication<Void> yarnApplication = YarnAssembly.forApplicationContainer(ContainerWithLongRunningLoop.class, ByteBuffer.wrap("hello".getBytes())).
							containerCount(2).
							withApplicationMaster(new YarnConfiguration()).build("validateFiniteContainerWithInfiniteLoopForceShutdown");
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
		Thread.sleep(5000);// let it run for a while and then terminate
		yarnApplication.terminate();
		assertFalse(yarnApplication.isRunning());
	}
	
	/**
	 */
	@Test
	public void validateForcedContainerStartupFailure() throws Exception {
		YarnApplication<Void> yarnApplication = YarnAssembly.forApplicationContainer(ContainerWithLongRunningLoop.class, ByteBuffer.wrap("hello".getBytes())).
				containerCount(4).
				withApplicationMaster(new YarnConfiguration()).build("validateForcedContainerStartupFailure");
		
		Map<String, Object> specificationMap = new HashMap<>(yarnApplication.getApplicationSpecification());
		specificationMap.put("FORCE_CONTAINER_ERROR", true);
		specificationMap.put(YayaConstants.CLIENTS_JOIN_TIMEOUT, "10");
		specificationMap.put(YayaConstants.YARN_CONFIG, new YarnConfiguration());
		
		Constructor<?> launcherCtr = ReflectionUtils.getInvocableConstructor("oz.hadoop.yarn.api.core.ApplicationMasterLauncherImpl", Map.class);
		Object launcher = launcherCtr.newInstance(specificationMap);
		try {
			Method launchMethod = ReflectionUtils.getMethodAndMakeAccessible(launcher.getClass(), "launch");
			launchMethod.invoke(launcher);
			fail();
		} catch (Exception e) {
			// ignore
		}
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
