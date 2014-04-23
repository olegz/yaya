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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * INTERNAL API
 * 
 * @author Oleg Zhurakousky
 *
 */
abstract class ProcessLauncher {
	
	private final Log logger = LogFactory.getLog(ProcessLauncher.class);
	
	protected final ExecutorService executor;
	
	protected final CountDownLatch containerLivelinesBarrier;
	
	/**
	 * 
	 * @param containerLivelinesBarrier
	 */
	ProcessLauncher(CountDownLatch containerLivelinesBarrier){
		this.executor = Executors.newCachedThreadPool();
		this.containerLivelinesBarrier = containerLivelinesBarrier;
	}

	/**
	 * 
	 */
	abstract void launch();
	
	/**
	 * 
	 * @return
	 */
	boolean isFinished() {
		return this.executor.isTerminated();
	}
	
	/**
	 * 
	 */
	void finish(){
		while (this.containerLivelinesBarrier.getCount() > 0){
			this.containerLivelinesBarrier.countDown();
		}
		logger.info("Shutting down executor");
		this.executor.shutdown();
		if (!this.executor.isTerminated()){
			try {
				if (!this.executor.awaitTermination(5, TimeUnit.SECONDS)){
					logger.warn("Killing executor with interrupt");
					this.executor.shutdownNow();
				}
			} 
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				logger.warn("Interrupted while waiting for executor to shut down");
			}
		}
	}
	
	/**
	 * 
	 */
	void awaitCompletion(){
		try {
			this.containerLivelinesBarrier.await();
		} 
		catch (InterruptedException e) {
			logger.warn("Interrupted while waiting for process to complete.");
			this.finish();
			Thread.currentThread().interrupt();
		}
	}
}
