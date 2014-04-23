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
package oz.hadoop.yarn.api.net;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

/**
 * @author Oleg Zhurakousky
 *
 */
public class ClientServerTests {
	
	@Test
	public void testMoreThenExpectedClients() throws Exception {
		for (int y = 0; y < 10; y++) {
			int expectedClients = 2;
			InetSocketAddress sa = new InetSocketAddress(InetAddress.getLocalHost().getHostAddress(), 0);
			ApplicationContainerServerImpl clientServer = new ApplicationContainerServerImpl(sa, expectedClients, false);
			InetSocketAddress address = clientServer.start();
			
			for (int i = 0; i < 40; i++) {
				ApplicationContainerClientImpl containerClientOne = new ApplicationContainerClientImpl(address, new EchoMessageHandler());
				containerClientOne.start();
				System.out.println("I" + i);
			}
			
			clientServer.awaitAllClients(2);
			
			ContainerDelegate[] containerDelegates = clientServer.getContainerDelegates();
			
			assertEquals(expectedClients, containerDelegates.length);
			clientServer.stop(false);
		}
	}
	
	@Test(timeout=5000)
	public void testSimpleInterruction() throws Exception {
		int expectedClients = 2;
		InetSocketAddress sa = new InetSocketAddress(InetAddress.getLocalHost().getHostAddress(), 0);
		ApplicationContainerServerImpl clientServer = new ApplicationContainerServerImpl(sa, expectedClients, false);
		InetSocketAddress address = clientServer.start();
		
		for (int i = 0; i < expectedClients; i++) {
			ApplicationContainerClientImpl containerClientOne = new ApplicationContainerClientImpl(address, new EchoMessageHandler());
			containerClientOne.start();
		}
		
		clientServer.awaitAllClients(2);
		
		ContainerDelegate[] containerDelegates = clientServer.getContainerDelegates();
		
		assertEquals(expectedClients, containerDelegates.length);
		for (int i = 0; i < 2; i++) {
			final int I = i;
			for (final ContainerDelegate containerDelegate : containerDelegates) {
				containerDelegate.process(ByteBuffer.wrap(new String("Hello" + i).getBytes()), new ReplyPostProcessor() {
					@Override
					public void doProcess(ByteBuffer reply) {
						String replyString = new String(reply.array()).trim();
						assertEquals("Hello" + I, replyString);
					}
				});
			}
		}
		clientServer.stop(false);
	}

	@Test//(timeout=5000)
	public void testServerWithMixedMessages() throws Exception {
		InetSocketAddress sa = new InetSocketAddress(InetAddress.getLocalHost().getHostAddress(), 0);
		ApplicationContainerServerImpl clientServer = new ApplicationContainerServerImpl(sa, 1, false);
		InetSocketAddress address = clientServer.start();
		StringBuffer buffer = new StringBuffer();
		String s = "Hello World";
		int size = 0;
		while (size < 16384){
			buffer.append(s);
			size += s.length();
		}
		buffer.append("\r\n");
		String message = buffer.toString().substring(0, 16384);
		
		ApplicationContainerClientImpl containerClient = new ApplicationContainerClientImpl(address, new SampleMessageHandler());
		containerClient.start();
		
		final ByteBuffer b1 = ByteBuffer.wrap(message.getBytes());
		
		ContainerDelegate[] containerDelegates = clientServer.getContainerDelegates();
		
		// Large message 
		containerDelegates[0].process(b1, new ReplyPostProcessor() {
			@Override
			public void doProcess(ByteBuffer reply) {
				int reportedMessageSize = reply.getInt(0); 
				assertEquals(b1.limit(), reportedMessageSize);
			}
		});
		
		// Small Message
		final ByteBuffer b2 = ByteBuffer.wrap("Hello NIO\r\n".getBytes());
		containerDelegates[0].process(b2, new ReplyPostProcessor() {
			@Override
			public void doProcess(ByteBuffer reply) {
				int reportedMessageSize = reply.getInt(0); 
				assertEquals(b2.limit(), reportedMessageSize);
			}
		});
	}
	
	@Test(timeout=5000)
	public void testWithExpectedClients() throws Exception {
		ExecutorService executor = Executors.newCachedThreadPool();
		final int expectedClients = 4;
		
		InetSocketAddress sa = new InetSocketAddress(InetAddress.getLocalHost().getHostAddress(), 0);
		ApplicationContainerServerImpl clientServer = new ApplicationContainerServerImpl(sa, expectedClients, false);
		final InetSocketAddress actualServerAddress = clientServer.start();
		@SuppressWarnings("unchecked")
		Future<ApplicationContainerClientImpl>[] clients = new Future[expectedClients];
		
		for (int i = 0; i < expectedClients; i++) {
			clients[i] = executor.submit(new Callable<ApplicationContainerClientImpl>() {
				@Override
				public ApplicationContainerClientImpl call() throws Exception {
					try {
						Thread.sleep(new Random().nextInt(200));
						ApplicationContainerClientImpl containerClient = new ApplicationContainerClientImpl(actualServerAddress, new SampleMessageHandler());
						containerClient.start();
						return containerClient;
					} 
					catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
			});
		}
		
		clientServer.awaitAllClients(30);
		assertEquals(clientServer.getContainerDelegates().length, expectedClients);
		for (Future<ApplicationContainerClientImpl> future : clients) {
			ApplicationContainerClient client = future.get();
			assertNotNull(client);
		}
		clientServer.stop(false);
	}
	
	@Test(timeout=30000)
	public void testWithMixedMessagesAndCorrectResultMultiClient() throws Exception {
		ExecutorService executor = Executors.newFixedThreadPool(20);
		final int expectedClients = 40;
		
		InetSocketAddress sa = new InetSocketAddress(InetAddress.getLocalHost().getHostAddress(), 0);
		ApplicationContainerServerImpl clientServer = new ApplicationContainerServerImpl(sa, expectedClients, false);
		InetSocketAddress actualServerAddress = clientServer.start();
		for (int i = 0; i < expectedClients; i++) {
			ApplicationContainerClientImpl containerClient = new ApplicationContainerClientImpl(actualServerAddress, new EchoMessageHandler());
			containerClient.start();
		}
		
		ContainerDelegate[] containerDelegates = clientServer.getContainerDelegates();
		int iterations = 100;
		final CountDownLatch latch = new CountDownLatch(containerDelegates.length * iterations);
		final AtomicReference<Exception> error = new AtomicReference<>();
		for (int i = 0; i < iterations; i++) {
			for (final ContainerDelegate containerDelegate : containerDelegates) {
				executor.execute(new Runnable() {	
					@Override
					public void run() {
						try {
							int loopCount = new Random().nextInt(1000);
							StringBuffer buffer = new StringBuffer();
							for (int y = 0; y < loopCount; y++) {
								buffer.append(UUID.randomUUID().toString());
							}
							final String inputMessage = buffer.toString();
							inputMessage.length();
							ByteBuffer inputBuffer = ByteBuffer.wrap(inputMessage.getBytes());
							containerDelegate.process(inputBuffer, new ReplyPostProcessor() {	
								@Override
								public void doProcess(ByteBuffer reply) {
									assertEquals(inputMessage.length(), reply.limit());
								}
							});
						} 
						catch (Exception e) {
							e.printStackTrace();
							error.set(e);
						}
						finally {
							latch.countDown();
						}
					}
				});
			}
		}
		
		latch.await();
		assertNull(error.get());
		
		clientServer.stop(false);
		executor.shutdown();
		System.out.println("Stopped");
	}
	
	private static class EchoMessageHandler implements ApplicationContainerMessageHandler {

		@Override
		public ByteBuffer handle(ByteBuffer messageBuffer) {
			return messageBuffer;
		}

		@Override
		public void onDisconnect() {
			
		}
		
	}
	
	private static class SampleMessageHandler implements ApplicationContainerMessageHandler {
		@Override
		public ByteBuffer handle(ByteBuffer messageBuffer) {
			ByteBuffer replyBuffer = ByteBuffer.allocate(6).putInt(messageBuffer.limit());
			replyBuffer.flip();
			return replyBuffer;
		}

		@Override
		public void onDisconnect() {
			// TODO Auto-generated method stub
			
		}
	}
}
