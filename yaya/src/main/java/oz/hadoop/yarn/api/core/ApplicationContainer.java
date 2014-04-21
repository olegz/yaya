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

import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

import org.springframework.util.StringUtils;

import oz.hadoop.yarn.api.ApplicationContainerProcessor;
import oz.hadoop.yarn.api.YayaConstants;
import oz.hadoop.yarn.api.net.ApplicationContainerClient;
import oz.hadoop.yarn.api.net.ApplicationContainerMessageHandler;
import oz.hadoop.yarn.api.utils.PrimitiveImmutableTypeMap;
import oz.hadoop.yarn.api.utils.ReflectionUtils;

/**
 * INTERNAL API
 * 
 * @author Oleg Zhurakousky
 *
 */
class ApplicationContainer extends AbstractContainer {
	
	private volatile CountDownLatch containerLivelinesBarrier;
	
	/**
	 * 
	 * @param containerArguments
	 */
	public ApplicationContainer(PrimitiveImmutableTypeMap containerArguments) {
		super(containerArguments);
		this.containerLivelinesBarrier = new CountDownLatch(1);
	}

	/**
	 * 
	 */
	@Override
	void run() {
		logger.info("###### Starting Application Container ######");
		
		final String appContainerImplClass = this.containerSpec.getString(YayaConstants.CONTAINER_IMPL);
		String command = this.containerSpec.getString(YayaConstants.COMMAND);
		final String containerArguments = this.containerSpec.getString(YayaConstants.CONTAINER_ARG);
		
		final ApplicationContainerProcessor applicationContainer = StringUtils.hasText(appContainerImplClass) ?
				(ApplicationContainerProcessor) ReflectionUtils.newDefaultInstance(appContainerImplClass) : null;
			
		ProcessLauncher processLauncher = null;
		if (StringUtils.hasText(command)){
			processLauncher = new CommandProcessLauncher(command, this.containerLivelinesBarrier);
		}
		else if (StringUtils.hasText(containerArguments)){
			processLauncher = new JavaProcessLauncher(applicationContainer, containerArguments, this.containerLivelinesBarrier);
		}
		
		ApplicationContainerClient client = processLauncher != null ? 
				this.connectWithApplicationMaster(new ProcessLaunchingApplicationContainer(processLauncher)) :
					this.connectWithApplicationMaster(applicationContainer);
				
		try {
			if (logger.isInfoEnabled()){
				logger.info("Awaiting Application Container's process to finish or termination signal from the client");
			}
			this.containerLivelinesBarrier.await();
			
			if (logger.isInfoEnabled()){
				logger.info("Stopping Application Container");
			}
		} 
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			logger.warn("Interrupted while awaiting Application Container to finish");
		}
		
		if (processLauncher != null && !processLauncher.isFinished()){
			processLauncher.finish();
			if (logger.isInfoEnabled()){
				logger.info("Force-terminated Application Container based on the request from the client");
			}
		}

		client.awaitShutdown();
		logger.info("ApplicationContainerClient has been stopped");
		
		logger.info("###### Application Container Stopped ######");
	}
	
	
	
	private ApplicationContainerClient connectWithApplicationMaster(ApplicationContainerProcessor applicationContainer){
		InetSocketAddress address = new InetSocketAddress(this.applicationSpecification.getString(YayaConstants.CLIENT_HOST), 
				this.applicationSpecification.getInt(YayaConstants.CLIENT_PORT));
		MessageDispatchingHandler messageHandler = new MessageDispatchingHandler(applicationContainer, this.containerLivelinesBarrier);
		
		ApplicationContainerClient client = this.buildApplicationContainerClient(address, messageHandler);
		InetSocketAddress listeningAddress = client.start();
		if (logger.isInfoEnabled()){
			logger.info("Started ApplicationContainerClient on " + listeningAddress);
		}
		
		return client;
	}
	
	/**
	 * 
	 */
	private ApplicationContainerClient buildApplicationContainerClient(InetSocketAddress address, MessageDispatchingHandler messageHandler){
		try {
			Constructor<ApplicationContainerClient> acCtr = ReflectionUtils.getInvocableConstructor(
					ApplicationContainerClient.class.getPackage().getName() + ".ApplicationContainerClientImpl", 
					InetSocketAddress.class, ApplicationContainerMessageHandler.class);
			ApplicationContainerClient ac = acCtr.newInstance(address, messageHandler);
			return ac;
		} 
		catch (Exception e) {
			throw new IllegalStateException("Failed to create ApplicationContainerClient instance", e);
		}
	}
	
	/**
	 * 
	 */
	private class ProcessLaunchingApplicationContainer implements ApplicationContainerProcessor {
		private final ProcessLauncher processLauncher;
		
		ProcessLaunchingApplicationContainer(ProcessLauncher processLauncher){
			this.processLauncher = processLauncher;
		}
		
		@Override
		public ByteBuffer process(ByteBuffer input) {
			this.processLauncher.launch();
			return ByteBuffer.wrap("OK".getBytes());
		}
	}
}