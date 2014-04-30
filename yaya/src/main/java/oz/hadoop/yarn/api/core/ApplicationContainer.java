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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Random;

import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import oz.hadoop.yarn.api.ApplicationContainerProcessor;
import oz.hadoop.yarn.api.YayaConstants;
import oz.hadoop.yarn.api.net.ApplicationContainerClient;
import oz.hadoop.yarn.api.net.ApplicationContainerMessageHandler;
import oz.hadoop.yarn.api.utils.ByteBufferUtils;
import oz.hadoop.yarn.api.utils.PrimitiveImmutableTypeMap;
import oz.hadoop.yarn.api.utils.ReflectionUtils;

/**
 * INTERNAL API
 * 
 * @author Oleg Zhurakousky
 *
 */
class ApplicationContainer extends AbstractContainer {

	private volatile ApplicationContainerClient client;
	
	private volatile InetSocketAddress listeningAddress;
	
	private volatile String successReplyMessage;
	
	private volatile String failureReplyMessage;
	
	/**
	 * 
	 * @param containerArguments
	 */
	ApplicationContainer(PrimitiveImmutableTypeMap containerArguments) {
		super(containerArguments);
	}

	/**
	 * 
	 */
	@Override
	void launch() {
		this.preLaunch();
		logger.info("###### Starting APPLICATION CONTAINER ######");
		try {
			this.doLaunch();
		} 
		catch (Exception e) {
			logger.error("Failed to launch an Application Container.", e);
			throw new IllegalStateException("Failed to launch an Application Container.", e);
		}
		finally {
			logger.info("###### Stopped APPLICATION CONTAINER ######");
		}
	}
	
	/**
	 * 
	 */
	private void doLaunch(){
		ApplicationContainerProcessor applicationContainer = null;
		
		String command = this.containerSpec.getString(YayaConstants.COMMAND);
		if (StringUtils.hasText(command)){
			applicationContainer = new ProcessLaunchingApplicationContainer(new CommandProcessLauncher(command));
		}
		else {
			String appContainerImplClass = this.containerSpec.getString(YayaConstants.CONTAINER_IMPL);
			Assert.hasText(appContainerImplClass, "Invalid condition: 'appContainerImplClass' must not be null or empty. " +
					"Since this is coming from internal API it must be a bug. Please REPORT.");
			applicationContainer = (ApplicationContainerProcessor) ReflectionUtils.newDefaultInstance(appContainerImplClass);
			String containerArguments = this.containerSpec.getString(YayaConstants.CONTAINER_ARG);
			if (StringUtils.hasText(containerArguments)){
				applicationContainer = new ProcessLaunchingApplicationContainer(new JavaProcessLauncher<ByteBuffer>(applicationContainer, containerArguments));
			}
		}
		applicationContainer = new ExceptionHandlingApplicationContainer(applicationContainer);
		this.connectWithApplicationMaster(applicationContainer);	
				
		logger.info("Awaiting Application Container's process to finish or termination signal from the client");
		/*
		 * Upon receiving a reply Server will check if application if finite and if so
		 * it will close the connection which will force client to exit
		 */
		this.client.awaitShutdown();

		logger.info("ApplicationContainerClient has been stopped");
	}
	
	/**
	 * 
	 * @param applicationContainer
	 * @return
	 */
	private void connectWithApplicationMaster(ApplicationContainerProcessor applicationContainer){
		MessageDispatchingHandler messageHandler = new MessageDispatchingHandler(applicationContainer);
		
		InetSocketAddress address = new InetSocketAddress(this.applicationSpecification.getString(YayaConstants.CLIENT_HOST), 
				                                          this.applicationSpecification.getInt(YayaConstants.CLIENT_PORT));
		this.client = this.buildApplicationContainerClient(address, messageHandler);
		this.listeningAddress = this.client.start();
		this.successReplyMessage = "OK:" + listeningAddress.getAddress().getHostAddress() + ":" + listeningAddress.getPort() + ":";
		this.failureReplyMessage = "FAILED:" + listeningAddress.getAddress().getHostAddress() + ":" + listeningAddress.getPort();
		if (logger.isInfoEnabled()){
			logger.info("Started ApplicationContainerClient on " + listeningAddress);
		}
	}
	
	/**
	 * 
	 */
	private ApplicationContainerClient buildApplicationContainerClient(InetSocketAddress address, ApplicationContainerMessageHandler messageHandler){
		try {
			Constructor<ApplicationContainerClient> acCtr = ReflectionUtils.getInvocableConstructor(
					ApplicationContainerClient.class.getPackage().getName() + ".ApplicationContainerClientImpl", 
					InetSocketAddress.class, ApplicationContainerMessageHandler.class, Runnable.class);
			ApplicationContainerClient ac = acCtr.newInstance(address, messageHandler, new Runnable() {
				@Override
				public void run() {
					// noop
				}
			});
			return ac;
		} 
		catch (Exception e) {
			throw new IllegalStateException("Failed to create ApplicationContainerClient instance", e);
		}
	}
	
	/**
	 * 
	 */
	private void preLaunch(){
		if (applicationSpecification.getBoolean("FORCE_CONTAINER_ERROR")){ // strictly for testing
			if (new Random().nextInt(2) == 1){
				throw new RuntimeException("INTENTIONALLY FORCING CONTAINER STARTUP FAILURE DUE TO 'FORCE_CONTAINER_ERROR' PROPERTY SET TO TRUE");
			}
		}
	}
	
	/**
	 * 
	 */
	private class ProcessLaunchingApplicationContainer implements ApplicationContainerProcessor {
		private final ProcessLauncher<?> processLauncher;
		
		ProcessLaunchingApplicationContainer(ProcessLauncher<?> processLauncher){
			this.processLauncher = processLauncher;
		}
		
		@Override
		public ByteBuffer process(ByteBuffer input) {
			return (ByteBuffer)this.processLauncher.launch();
		}
	}
	
	/**
	 * 
	 */
	private class ExceptionHandlingApplicationContainer implements ApplicationContainerProcessor {
		private final ApplicationContainerProcessor targetApplicationContainer;
		
		ExceptionHandlingApplicationContainer(ApplicationContainerProcessor targetApplicationContainer){
			this.targetApplicationContainer = targetApplicationContainer;
		}
		
		@Override
		public ByteBuffer process(ByteBuffer input) {
			try {
				ByteBuffer reply = this.targetApplicationContainer.process(input);
				if (reply == null){
					reply = ByteBuffer.wrap(successReplyMessage.getBytes());
				}
				else {
					ByteBuffer source = ByteBuffer.wrap(successReplyMessage.getBytes());
					source.position(source.limit());
					reply = ByteBufferUtils.merge(source, reply);
				}
				reply.rewind();
				return reply;
			} 
			catch (Exception e) {
				logger.error("Process failed in " + listeningAddress.getAddress().getHostAddress() + ":" + listeningAddress.getPort(), e);
				StringBuffer replyMessageBuffer = new StringBuffer();
				replyMessageBuffer.append(failureReplyMessage);
				replyMessageBuffer.append(":{\n");
				
				StringWriter sw = new StringWriter();
				PrintWriter pw = new PrintWriter(sw);
				e.printStackTrace(pw);
				
				replyMessageBuffer.append(sw.toString());
				replyMessageBuffer.append("\n}");

				return ByteBuffer.wrap(replyMessageBuffer.toString().getBytes());
			}
		}
	}
}