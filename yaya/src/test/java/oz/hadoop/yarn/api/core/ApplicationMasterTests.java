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
import static junit.framework.Assert.fail;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import org.junit.Ignore;
import org.junit.Test;

import oz.hadoop.yarn.api.ApplicationContainerProcessor;
import oz.hadoop.yarn.api.ContainerReplyListener;
import oz.hadoop.yarn.api.YayaConstants;
import oz.hadoop.yarn.api.net.ApplicationContainerServer;
import oz.hadoop.yarn.api.net.ContainerDelegate;
import oz.hadoop.yarn.api.net.ReplyPostProcessor;
import oz.hadoop.yarn.api.utils.PrimitiveImmutableTypeMap;
import oz.hadoop.yarn.api.utils.ReflectionUtils;

/**
 * @author Oleg Zhurakousky
 *
 */
public class ApplicationMasterTests {
	
	@Test
	public void validateFiniteContainerSelfShutdown() throws Exception {
		int expectedContainers = 5;
		Map<String, Object> applicationSpec = this.buildApplicationSpec(null, ContainerWithDelay.class, expectedContainers, true);
	
		ApplicationMasterLauncher<Void> amLauncher = new ApplicationMasterLauncherEmulatorImpl<>(applicationSpec);
		amLauncher.launch();
		while (amLauncher.isRunning()){
			LockSupport.parkNanos(1000000);
		}
		assertFalse(amLauncher.isRunning());
	}
	
	@Test
	public void validateReusableContainerGracefulShutdownNoTaskSubmitted() throws Exception {
		int expectedContainers = 5;
		Map<String, Object> applicationSpec = this.buildApplicationSpec(null, ContainerWithDelay.class, expectedContainers, false);
	
		ApplicationMasterLauncher<Void> amLauncher = new ApplicationMasterLauncherEmulatorImpl<>(applicationSpec);
		amLauncher.launch();
		assertTrue(amLauncher.isRunning());
		amLauncher.shutDown();
		assertFalse(amLauncher.isRunning());
	}
	
	@Test
	public void validateReusableContainerGracefulShutdown() throws Exception {
		int expectedContainers = 5;
		Map<String, Object> applicationSpec = this.buildApplicationSpec(null, ContainerWithDelay.class, expectedContainers, false);
	
		ApplicationMasterLauncher<Void> amLauncher = new ApplicationMasterLauncherEmulatorImpl<>(applicationSpec);
		amLauncher.launch();
		assertTrue(amLauncher.isRunning());
		ApplicationContainerServer clientServer = (ApplicationContainerServer) ReflectionUtils.getFieldAndMakeAccessible(amLauncher.getClass(), "clientServer").get(amLauncher);
		ContainerDelegate[] containerDelegates = clientServer.getContainerDelegates();
		for (final ContainerDelegate containerDelegate : containerDelegates) {
			containerDelegate.process(ByteBuffer.wrap("hello".getBytes()), new ReplyPostProcessor(){
				@Override
				public void doProcess(ByteBuffer reply) {
				}
			});
		}
		amLauncher.shutDown();
		assertFalse(amLauncher.isRunning());
	}
	
	@Test
	public void validateReusableContainerForceShutdownWithInfiniteTasks() throws Exception {
		int expectedContainers = 5;
		Map<String, Object> applicationSpec = this.buildApplicationSpec(null, ContainerWithLongRunningLoop.class, expectedContainers, false);
	
		ApplicationMasterLauncher<Void> amLauncher = new ApplicationMasterLauncherEmulatorImpl<>(applicationSpec);
		amLauncher.launch();
		assertTrue(amLauncher.isRunning());
		ApplicationContainerServer clientServer = (ApplicationContainerServer) ReflectionUtils.getFieldAndMakeAccessible(amLauncher.getClass(), "clientServer").get(amLauncher);
		ContainerDelegate[] containerDelegates = clientServer.getContainerDelegates();
		for (final ContainerDelegate containerDelegate : containerDelegates) {
			containerDelegate.process(ByteBuffer.wrap("hello".getBytes()), new ReplyPostProcessor(){
				@Override
				public void doProcess(ByteBuffer reply) {
				}
			});
		}
		Thread.sleep(3000); // letting it run for a bit
		amLauncher.terminate();
		assertFalse(amLauncher.isRunning());
	}
	
	@Test
	public void validateFiniteContainerWithInfiniteLoopForceShutdown() throws Exception {
		ExecutorService executor = Executors.newCachedThreadPool();
		int expectedContainers = 4;
		Map<String, Object> applicationSpec = this.buildApplicationSpec(null, ContainerWithLongRunningLoop.class, expectedContainers, true);
		final ApplicationMasterLauncher<Void> amLauncher = new ApplicationMasterLauncherEmulatorImpl<>(applicationSpec);
		final AtomicReference<ApplicationContainerServer> clientServerRef = new AtomicReference<>();
		executor.execute(new Runnable() {
			@Override
			public void run() {
				amLauncher.launch();
			}
		});
		
		ApplicationContainerServer clientServer = null;
		while (clientServer == null){
			clientServer = (ApplicationContainerServer) ReflectionUtils.getFieldAndMakeAccessible(amLauncher.getClass(), "clientServer").get(amLauncher);
		}		
		clientServerRef.set(clientServer);
		clientServer.awaitAllClients(5);
		
		assertTrue(amLauncher.isRunning());
		Thread.sleep(3000);
		assertTrue(amLauncher.isRunning());
		System.out.println("Didn't shut down on its onw. Good");
		
		executor.execute(new Runnable() {	
			@Override
			public void run() {
				clientServerRef.get().stop(false);
			}
		});
		Thread.sleep(3000);
		assertTrue(amLauncher.isRunning());
		System.out.println("Didn't shut down on graceful attempt. Good");
		
		clientServerRef.get().stop(true);
		clientServerRef.get().awaitShutdown();
		assertFalse(amLauncher.isRunning());
		System.out.println("Shut down with force. Good");
		executor.shutdownNow();
	}
	
	@SuppressWarnings("unchecked")
	@Test
	@Ignore // need to fix to adjust for API changes
	public void validateFiniteContainerSelfShutdownWithContainerThrowingException() throws Exception {
		ExecutorService executor = Executors.newCachedThreadPool();
		int expectedContainers = 2;
		ApplicationContainerServer clientServer = (ApplicationContainerServer) ReflectionUtils.
				getInvocableConstructor("oz.hadoop.yarn.api.net.ApplicationContainerServerImpl", int.class, boolean.class, Runnable.class).
				newInstance(expectedContainers, true, new Runnable() {
					
					@Override
					public void run() {
						System.out.println("Server onDisconnect");
					}
				});
		InetSocketAddress address = clientServer.start();
		
		Map<String, Object> applicationSpec = this.buildApplicationSpec(address, ContainerThrowingException.class, expectedContainers, true);
		
		final ApplicationContainerLauncher acLauncher = 
				new ApplicationContainerLauncherEmulatorImpl(new PrimitiveImmutableTypeMap(applicationSpec), 
						new PrimitiveImmutableTypeMap((Map<String, Object>) applicationSpec.get(YayaConstants.CONTAINER_SPEC)));
		executor.execute(new Runnable() {
			@Override
			public void run() {
				acLauncher.launch();
			}
		});
		
		for (int i = 0; i < expectedContainers; i++) {
			clientServer.getContainerDelegates()[i].process(ByteBuffer.wrap("START".getBytes()), new ReplyPostProcessor() {
				@Override
				public void doProcess(ByteBuffer reply) {
					byte[] replyBytes = new byte[reply.limit()];
					reply.get(replyBytes);
					System.out.println("Processing reply\n" + new String(replyBytes));
				}
			});
		}
		clientServer.awaitShutdown();
		assertFalse(clientServer.isRunning());
		executor.shutdown();
	}
	
	@Test
	@Ignore // need to fix to adjust for API changes
	public void validateFiniteContainerSelfShutdownWithSomeContainersThrowingException() throws Exception {
		int expectedContainers = 4;
		Map<String, Object> applicationSpec = this.buildApplicationSpec(null, ContainerThrowingExceptionRandomly.class, expectedContainers, true);
		final AtomicInteger replySucceses = new AtomicInteger();
		ApplicationMasterLauncher<Void> amLauncher = new ApplicationMasterLauncherEmulatorImpl<>(applicationSpec);
		amLauncher.registerReplyListener(new ContainerReplyListener(){

			@Override
			public void onReply(ByteBuffer replyData) {
				byte[] replyBytes = new byte[replyData.limit()];
				replyData.rewind();
				replyData.get(replyBytes);
				replyData.rewind();
				String reply = new String(replyBytes);
				assertTrue(reply.startsWith("OK") || reply.startsWith("FAIL"));
				replySucceses.incrementAndGet();
			}
		});
		amLauncher.launch();
		while (amLauncher.isRunning()){
			LockSupport.parkNanos(1000000);
		}
		assertFalse(amLauncher.isRunning());
		assertEquals(expectedContainers, replySucceses.get());
	}
	
	@Test
	public void validateReusableContainerWithSomeContainersThrowingException() throws Exception {
		int expectedContainers = 4;
		final AtomicInteger replySucceses = new AtomicInteger();
		Map<String, Object> applicationSpec = this.buildApplicationSpec(null, ContainerThrowingExceptionRandomly.class, expectedContainers, false);
		ApplicationMasterLauncher<Void> amLauncher = new ApplicationMasterLauncherEmulatorImpl<>(applicationSpec);
		amLauncher.launch();
		ApplicationContainerServer clientServer = (ApplicationContainerServer) ReflectionUtils.getFieldAndMakeAccessible(amLauncher.getClass(), "clientServer").get(amLauncher);
		ContainerDelegate[] containerDelegates = clientServer.getContainerDelegates();
		for (final ContainerDelegate containerDelegate : containerDelegates) {
			containerDelegate.process(ByteBuffer.wrap("hello".getBytes()), new ReplyPostProcessor(){
				@Override
				public void doProcess(ByteBuffer replyBuffer) {
					byte[] replyBytes = new byte[replyBuffer.limit()];
					replyBuffer.rewind();
					replyBuffer.get(replyBytes);
					replyBuffer.rewind();
					String reply = new String(replyBytes);
					assertTrue(reply.startsWith("OK") || reply.startsWith("FAIL"));
					replySucceses.incrementAndGet();
				}
			});
		}
		amLauncher.shutDown();
		assertFalse(amLauncher.isRunning());
		assertEquals(expectedContainers, replySucceses.get());
	}
	
	/**
	 * Different from the previous test in such way that Application Container
	 * doesn't even get a chance to start.
	 */
	@Test
	@Ignore // need to fix to adjust for API changes
	public void validateFiniteContainerSelfShutdownWithContainerStartupException() throws Exception {
		int expectedContainers = 5;
		Map<String, Object> applicationSpec = this.buildApplicationSpec(null, ContainerThrowingException.class, expectedContainers, true);
		applicationSpec.put("FORCE_CONTAINER_ERROR", true);
		applicationSpec.put(YayaConstants.CLIENTS_JOIN_TIMEOUT, "5");
		
		ApplicationMasterLauncher<Void> amLauncher = new ApplicationMasterLauncherEmulatorImpl<>(applicationSpec);
		
		try {
			amLauncher.launch();
			fail();
		} catch (Exception e) {
			// ignore
		}
		assertFalse(amLauncher.isRunning());
	}
	
	/**
	 * 
	 */
	private Map<String, Object> buildApplicationSpec(InetSocketAddress address, Class<? extends ApplicationContainerProcessor> containerProcessor, int expectedContainers, boolean finite){
		Map<String, Object> applicationSpec = new HashMap<String, Object>();
		if (address != null){
			applicationSpec.put(YayaConstants.CLIENT_HOST, address.getAddress().getHostAddress());
			applicationSpec.put(YayaConstants.CLIENT_PORT, address.getPort());
		}
		applicationSpec.put(YayaConstants.YARN_EMULATOR, true);
		Map<String, Object> containerSpec = new HashMap<String, Object>();
		containerSpec.put(YayaConstants.CONTAINER_IMPL, containerProcessor.getName());
		if (finite){
			containerSpec.put(YayaConstants.CONTAINER_ARG, "Hello");
		}
		containerSpec.put(YayaConstants.CONTAINER_COUNT, expectedContainers);
		containerSpec.put(YayaConstants.PRIORITY, 0);
		containerSpec.put(YayaConstants.VIRTUAL_CORES, 1);
		containerSpec.put(YayaConstants.MEMORY, 256);
		applicationSpec.put(YayaConstants.CONTAINER_SPEC, containerSpec);
		return applicationSpec;
	}
	
	/**
	 * 
	 */
	public static class ContainerWithDelay implements ApplicationContainerProcessor {
		@Override
		public ByteBuffer process(ByteBuffer inputMessage) {
			System.out.println("In ContainerWithDelay");
			try {
				Thread.sleep(5000);
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
	public static class ContainerThrowingException implements ApplicationContainerProcessor {
		@Override
		public ByteBuffer process(ByteBuffer inputMessage) {
			throw new RuntimeException("Intentional");
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
