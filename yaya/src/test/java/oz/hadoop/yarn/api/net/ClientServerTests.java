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
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import org.junit.Test;

/**
 * @author Oleg Zhurakousky
 *
 */
public class ClientServerTests {
	
	@Test
	public void validateNetworkStackShutdownAndOnDisconnectListenerInvocationByClosingServer() throws Exception {
		int expectedClients = 2;
		Runnable onDisconnectTaskServer = mock(Runnable.class);
		Runnable onDisconnectTaskClient = mock(Runnable.class);
		
		ApplicationContainerServerImpl clientServer = new ApplicationContainerServerImpl(expectedClients, false, onDisconnectTaskServer);
		InetSocketAddress address = clientServer.start();
		assertTrue(clientServer.isRunning());
		
		ApplicationContainerClientImpl containerClientOne = new ApplicationContainerClientImpl(address, new EchoMessageHandler(), onDisconnectTaskClient);
		containerClientOne.start();
		assertTrue(containerClientOne.isRunning());
		
		ApplicationContainerClientImpl containerClientTwo = new ApplicationContainerClientImpl(address, new EchoMessageHandler(), onDisconnectTaskClient);
		containerClientTwo.start();
		assertTrue(containerClientTwo.isRunning());
		
		clientServer.awaitAllClients(2);
		
		verify(onDisconnectTaskClient, times(0)).run();
		verify(onDisconnectTaskServer, times(0)).run();
		
		clientServer.stop(false);
		clientServer.awaitShutdown();	
		
		assertFalse(clientServer.isRunning());
		assertFalse(containerClientOne.isRunning());
		assertFalse(containerClientTwo.isRunning());
		verify(onDisconnectTaskClient, times(2)).run();
		verify(onDisconnectTaskServer, times(1)).run();
	}
	
	@Test
	public void validateNetworkStackShutdownAndOnDisconnectListenerInvocationByClosingClients() throws Exception {
		int expectedClients = 2;
		Runnable onDisconnectTaskServer = mock(Runnable.class);
		Runnable onDisconnectTaskClient = mock(Runnable.class);
		
		ApplicationContainerServerImpl clientServer = new ApplicationContainerServerImpl(expectedClients, false, onDisconnectTaskServer);
		InetSocketAddress address = clientServer.start();
		assertTrue(clientServer.isRunning());
		
		ApplicationContainerClientImpl containerClientOne = new ApplicationContainerClientImpl(address, new EchoMessageHandler(), onDisconnectTaskClient);
		containerClientOne.start();
		assertTrue(containerClientOne.isRunning());
		
		ApplicationContainerClientImpl containerClientTwo = new ApplicationContainerClientImpl(address, new EchoMessageHandler(), onDisconnectTaskClient);
		containerClientTwo.start();
		assertTrue(containerClientTwo.isRunning());
		
		clientServer.awaitAllClients(2);
		
		verify(onDisconnectTaskClient, times(0)).run();
		verify(onDisconnectTaskServer, times(0)).run();
		
		containerClientOne.stop(false);
		assertTrue(clientServer.isRunning());
		assertFalse(containerClientOne.isRunning());

		verify(onDisconnectTaskClient, times(1)).run();
		verify(onDisconnectTaskServer, times(0)).run();
		
		containerClientTwo.stop(false);
		
		clientServer.awaitShutdown();	
		assertFalse(clientServer.isRunning());
		assertFalse(containerClientTwo.isRunning());
		
		verify(onDisconnectTaskClient, times(2)).run();
		verify(onDisconnectTaskServer, times(1)).run();
	}
	
	@Test
	public void validateForcedShutdown() throws Exception {
		ExecutorService executor = Executors.newCachedThreadPool();
		int expectedClients = 2;
		final ApplicationContainerServer clientServer = new ApplicationContainerServerImpl(expectedClients, false, mock(Runnable.class));
		InetSocketAddress address = clientServer.start();
		
		ApplicationContainerClient containerClientOne = new ApplicationContainerClientImpl(address, new BlockingMessageHandler(), mock(Runnable.class));
		containerClientOne.start();
		
		ApplicationContainerClient containerClientTwo = new ApplicationContainerClientImpl(address, new BlockingMessageHandler(), mock(Runnable.class));
		containerClientTwo.start();
		
		clientServer.awaitAllClients(1);
		
		ContainerDelegate[] containerDelegates = clientServer.getContainerDelegates();
		for (ContainerDelegate containerDelegate : containerDelegates) {
			containerDelegate.process(ByteBuffer.wrap("foo".getBytes()), mock(ReplyPostProcessor.class));
		}
		// attempt to shutdown without force. it will block without success since the container tasks are infinite
		executor.execute(new Runnable() {	
			@Override
			public void run() {
				clientServer.stop(false);
			}
		});
		
		Thread.sleep(2000);
		assertTrue(clientServer.isRunning());
		clientServer.stop(true);
		clientServer.awaitShutdown();	
		
		assertFalse(clientServer.isRunning());
	}
	
	@Test
	public void validateMoreThenExpectedClients() throws Exception {
		for (int y = 0; y < 10; y++) {
			int expectedClients = 2;
			ApplicationContainerServerImpl clientServer = new ApplicationContainerServerImpl(expectedClients, false, mock(Runnable.class));
			InetSocketAddress address = clientServer.start();
			
			for (int i = 0; i < 40; i++) {
				ApplicationContainerClientImpl containerClientOne = new ApplicationContainerClientImpl(address, new EchoMessageHandler(), mock(Runnable.class));
				containerClientOne.start();
			}
			
			clientServer.awaitAllClients(2);
			
			ContainerDelegate[] containerDelegates = clientServer.getContainerDelegates();
			
			assertEquals(expectedClients, containerDelegates.length);
			clientServer.stop(false);
		}
	}
	
	@Test(timeout=5000)
	public void validateSimpleInterruction() throws Exception {
		int expectedClients = 2;
		InetSocketAddress sa = new InetSocketAddress(InetAddress.getLocalHost().getHostAddress(), 0);
		ApplicationContainerServerImpl clientServer = new ApplicationContainerServerImpl(sa, expectedClients, false, mock(Runnable.class));
		InetSocketAddress address = clientServer.start();
		
		for (int i = 0; i < expectedClients; i++) {
			ApplicationContainerClientImpl containerClientOne = new ApplicationContainerClientImpl(address, new EchoMessageHandler(), mock(Runnable.class));
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
		clientServer.awaitShutdown();	
	}

	@Test(timeout=5000)
	public void validateMixedMessageSizeExchange() throws Exception {
		ApplicationContainerServerImpl clientServer = new ApplicationContainerServerImpl(1, false, mock(Runnable.class));
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
		
		ApplicationContainerClientImpl containerClient = new ApplicationContainerClientImpl(address, new SampleMessageHandler(), mock(Runnable.class));
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
	
	@Test(timeout=30000)
	public void validateWithMixedMessageSizesAndCorrectResultMultiClient() throws Exception {
		ExecutorService executor = Executors.newFixedThreadPool(20);
		final int expectedClients = 40;
		
		InetSocketAddress sa = new InetSocketAddress(InetAddress.getLocalHost().getHostAddress(), 0);
		ApplicationContainerServerImpl clientServer = new ApplicationContainerServerImpl(sa, expectedClients, false, mock(Runnable.class));
		InetSocketAddress actualServerAddress = clientServer.start();
		for (int i = 0; i < expectedClients; i++) {
			ApplicationContainerClientImpl containerClient = new ApplicationContainerClientImpl(actualServerAddress, new EchoMessageHandler(), mock(Runnable.class));
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
	
	private static class BlockingMessageHandler implements ApplicationContainerMessageHandler {
		@Override
		public ByteBuffer handle(ByteBuffer messageBuffer) {
			for (int i = 0; i < 1000000; i++) {
				LockSupport.parkNanos(1000000000);
				boolean interrupted = Thread.interrupted();
				System.out.println(this +  " - running: " + interrupted);
				if (interrupted){
					break;
				}
			}
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
