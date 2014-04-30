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

import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import oz.hadoop.yarn.api.ApplicationContainerProcessor;
import oz.hadoop.yarn.api.net.ApplicationContainerClient;
import oz.hadoop.yarn.api.net.ApplicationContainerMessageHandler;
import oz.hadoop.yarn.api.net.ApplicationContainerServer;

/**
 * INTERNAL API
 * 
 * A dispatcher-type class which is bound to {@link ApplicationContainerClient} with the 
 * purpose of dispatching messages coming from {@link ApplicationContainerServer} to the 
 * provided {@link ApplicationContainerProcessor}.
 * 
 * @author Oleg Zhurakousky
 *
 */
class MessageDispatchingHandler implements ApplicationContainerMessageHandler {
	
	private final static Log logger = LogFactory.getLog(MessageDispatchingHandler.class);
	
	private final ApplicationContainerProcessor applicationContainer;
	
	/**
	 * 
	 * @param applicationContainer
	 * @param disconnectMonitor
	 */
	public MessageDispatchingHandler(ApplicationContainerProcessor applicationContainer){
		this.applicationContainer = applicationContainer;
	}

	/**
	 * 
	 */
	@Override
	public ByteBuffer handle(ByteBuffer messageBuffer) {
		if (logger.isDebugEnabled()){
			logger.debug("Handling buffer: " + messageBuffer);
		}
		return this.applicationContainer.process(messageBuffer);
	}

	/**
	 * 
	 */
	@Override
	public void onDisconnect() {
		if (logger.isDebugEnabled()){
			logger.debug("Received onDisconnect event");
		}
	}
}
