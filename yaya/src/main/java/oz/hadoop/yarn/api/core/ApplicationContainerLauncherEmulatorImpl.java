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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import oz.hadoop.yarn.api.utils.PrimitiveImmutableTypeMap;

/**
 * @author Oleg Zhurakousky
 *
 */
public class ApplicationContainerLauncherEmulatorImpl extends AbstractApplicationContainerLauncher {
	
	private final Log logger = LogFactory.getLog(ApplicationContainerLauncherEmulatorImpl.class);
	
	private final ExecutorService executor;

	public ApplicationContainerLauncherEmulatorImpl(PrimitiveImmutableTypeMap applicationSpecification, PrimitiveImmutableTypeMap containerSpecification) {
		super(applicationSpecification, containerSpecification);
		this.executor = Executors.newCachedThreadPool();
	}

	@Override
	void doLaunch() throws Exception {
		for (int i = 0; i < this.containerCount; i++) {	
			this.executor.execute(new Runnable() {	
				@Override
				public void run() {
					try {
						ApplicationContainer launcher = new ApplicationContainer(ApplicationContainerLauncherEmulatorImpl.this.applicationSpecification);
						launcher.run();
						if (logger.isInfoEnabled()){
							logger.info("Container finished");
						}
					} 
					finally {
						ApplicationContainerLauncherEmulatorImpl.this.livelinessBarrier.countDown();
					}
				}
			});
		}
	}

	@Override
	void doShutDown() throws Exception {
		logger.info("Shutting down executor");
		this.executor.shutdown();
	}
}
