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
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.springframework.util.StringUtils;

import oz.hadoop.yarn.api.ApplicationContainer;
import oz.hadoop.yarn.api.YayaConstants;
import oz.hadoop.yarn.api.net.ApplicationContainerClient;
import oz.hadoop.yarn.api.net.ApplicationContainerMessageHandler;
import oz.hadoop.yarn.api.utils.PrimitiveImmutableTypeMap;
import oz.hadoop.yarn.api.utils.ReflectionUtils;

/**
 * @author Oleg Zhurakousky
 *
 */
public class ApplicationContainerLauncher extends AbstractContainerLauncher {

	/**
	 * 
	 * @param containerArguments
	 */
	public ApplicationContainerLauncher(PrimitiveImmutableTypeMap containerArguments) {
		super(containerArguments);
	}

	/**
	 * 
	 */
	@SuppressWarnings("unchecked")
	@Override
	void run() {
		logger.info("###### Starting Application Container ######");
		
		String appContainerImplClass = (String) ((Map<String, Object>) this.applicationSpecification.get(YayaConstants.CONTAINER_SPEC)).get(YayaConstants.CONTAINER_IMPL);
		ApplicationContainer applicationContainer = (ApplicationContainer) ReflectionUtils.newDefaultInstance(appContainerImplClass);

		String containerArguments = (String) ((Map<String, Object>)this.applicationSpecification.get(YayaConstants.CONTAINER_SPEC)).get(YayaConstants.CONTAINER_ARG);
		if (!StringUtils.hasText(containerArguments)){
			InetSocketAddress address = new InetSocketAddress(this.applicationSpecification.getString(YayaConstants.CLIENT_HOST), this.applicationSpecification.getInt(YayaConstants.CLIENT_PORT));
			MessageDispatchingHandler messageHandler = new MessageDispatchingHandler(applicationContainer);
			ApplicationContainerClient client = this.buildApplicationContainerClient(address, messageHandler);
			InetSocketAddress listeningAddress = client.start();
			if (logger.isInfoEnabled()){
				logger.info("Started ApplicationContainerClient on " + listeningAddress);
			}
			client.awaitShutdown();
			client.stop();
		}
		else {
			byte[] decodedBytes = Base64.decodeBase64(containerArguments);
			ByteBuffer reply = applicationContainer.process(ByteBuffer.wrap(decodedBytes));
			
			if (reply.limit() > 0){
				reply.rewind();
				logger.info("ApplicationContainer produced reply which will be logged to Standard out");
				byte[] replyBytes = new byte[reply.limit()];
				reply.get(replyBytes);
				logger.info("[ApplicationContainer REPLY]\n" + new String(replyBytes));
			}
		}
		
		logger.info("ApplicationContainerClient has been shut down");
		logger.info("###### Application Container Stopped ######");
	}
	
	/**
	 * 
	 */
	private ApplicationContainerClient buildApplicationContainerClient(InetSocketAddress address, MessageDispatchingHandler messageHandler){
		try {
			Constructor<ApplicationContainerClient> acCtr = ReflectionUtils.getInvocableConstructor(
					ApplicationContainerClient.class.getPackage().getName() + ".ApplicationContainerClientImpl", InetSocketAddress.class, ApplicationContainerMessageHandler.class);
			ApplicationContainerClient ac = acCtr.newInstance(address, messageHandler);
			return ac;
		} 
		catch (Exception e) {
			throw new IllegalStateException("Failed to create ApplicationContainerClient instance", e);
		}
	}
}