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

import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import oz.hadoop.yarn.api.YayaConstants;
import oz.hadoop.yarn.api.utils.PrimitiveImmutableTypeMap;

/**
 * @author Oleg Zhurakousky
 *
 */
public abstract class AbstractApplicationContainerLauncher implements ApplicationContainerLauncher {
	
	private final Log logger = LogFactory.getLog(AbstractApplicationContainerLauncher.class);
	
	protected final PrimitiveImmutableTypeMap applicationSpecification;
	
	protected final PrimitiveImmutableTypeMap containerSpecification;
	
	protected final int containerCount;
	
	protected final CountDownLatch livelinessBarrier;
	
	protected volatile Throwable error;
	
	/**
	 * 
	 * @param applicationSpecification
	 * @param containerSpecification
	 */
	public AbstractApplicationContainerLauncher(PrimitiveImmutableTypeMap applicationSpecification, PrimitiveImmutableTypeMap containerSpecification){
		this.applicationSpecification = applicationSpecification;
		this.containerSpecification = containerSpecification;
		this.containerCount = this.containerSpecification.getInt(YayaConstants.CONTAINER_COUNT);
		this.livelinessBarrier = new CountDownLatch(this.containerCount);
	}

	/**
	 * 
	 */
	@Override
	public void launch() {
		try {
			this.doLaunch();
			// Wait till all containers are finished. clean up and exit.
			logger.info("Waiting for Application Containers to finish");
			this.livelinessBarrier.await();
			logger.info("Done waiting for Application Containers to finish");
			if (this.error != null){
				logger.error("Application Master for " + this.applicationSpecification.getString(YayaConstants.APPLICATION_NAME) + "_" + 
						this.applicationSpecification.get(YayaConstants.APP_ID) + " failed with error.", this.error);
			}
		} 
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			logger.warn("APPLICATION MASTER's launch thread was interrupted");
		}
		catch (Exception e){
			throw new IllegalStateException("Failed to launch Application Container", e);
		}
		finally {
			this.shutDown();
		}
	}

	/**
	 * 
	 */
	@Override
	public void shutDown() {
		try {
			this.doShutDown();
			if (logger.isInfoEnabled()){
				logger.info("Shut down " + this.getClass().getName());
			}
		} catch (Exception e) {
			logger.error("Failure during shut down", e);
		}
	}
	
	/**
	 * 
	 * @throws Exception
	 */
	abstract void doLaunch() throws Exception;
	
	/**
	 * 
	 * @throws Exception
	 */
	abstract void doShutDown() throws Exception;

}
