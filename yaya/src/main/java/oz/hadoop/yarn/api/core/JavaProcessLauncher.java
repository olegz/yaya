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
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import oz.hadoop.yarn.api.ApplicationContainerProcessor;

/**
 * INTERNAL API
 * 
 * Launcher for launching java-based tasks
 * 
 * @author Oleg Zhurakousky
 *
 */
class JavaProcessLauncher<R> extends ProcessLauncher<R> {
	
	private final Log logger = LogFactory.getLog(JavaProcessLauncher.class);

	private final ApplicationContainerProcessor applicationContainer;
	
	private final String containerArguments;
	
	/**
	 * 
	 * @param applicationContainer
	 * @param containerArguments
	 * @param containerLivelinesBarrier
	 */
	JavaProcessLauncher(ApplicationContainerProcessor applicationContainer, String containerArguments) {
		super();
		this.containerArguments = containerArguments;
		this.applicationContainer = applicationContainer;
	}

	/**
	 * Will execute java process asynchronously to allow it to be terminated abruptly 
	 * (e.g., process may be sitting in the infinite loop). Upon exit it will call {@link #finish()}
	 * which will trip 'containerLivelinesBarrier' allowing the owning Application Container 
	 * to finish.
	 */
	@SuppressWarnings("unchecked")
	@Override
	public R launch() {
		logger.info("Executing java process");	
		final AtomicReference<Object> result = new AtomicReference<>();
		this.executor.execute(new Runnable() {

			@Override
			public void run() {
				try {
					byte[] decodedBytes = Base64.decodeBase64(containerArguments);
					ByteBuffer reply = applicationContainer.process(ByteBuffer.wrap(decodedBytes));		
					logger.info("Java process completed successfully");
					result.set(reply);
				} 
				catch (Exception e) {
					logger.error("Java process failed.", e);
					result.set(new RuntimeException(e));
				}
				finally {
					JavaProcessLauncher.this.containerLivelinesBarrier.countDown();
				}
			}
		});
		this.awaitCompletion();
		this.finish();
		Object resultObject = result.get();
		
		if (resultObject instanceof Throwable){
			throw new RuntimeException((Throwable)resultObject);
		}
		return (R) resultObject;
	}
}
