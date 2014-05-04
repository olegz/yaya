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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * Base class for implementing launchers. See {@link CommandProcessLauncher} and {@link JavaProcessLauncher}
 * 
 * @author Oleg Zhurakousky
 *
 */
public abstract class ProcessLauncher<R> {
	
	private final Log logger = LogFactory.getLog(ProcessLauncher.class);
	
	protected final ExecutorService executor;
	
	protected final CountDownLatch containerLivelinesBarrier;
	
	/**
	 * 
	 * @param containerLivelinesBarrier
	 */
	ProcessLauncher(){
		this.executor = Executors.newCachedThreadPool();
		this.containerLivelinesBarrier = new CountDownLatch(1);
	}

	/**
	 * 
	 */
	abstract R launch();
	
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
		logger.debug("Shutting down executor");
		/*
		 * We are actually terminating the executor. Since this is an internal method and its invocation 
		 * is tightly controlled its invocation constitutes desire to stop ASAP.
		 * If some containers need to be finished (e.g., graceful shutdown), it's taken care of by other 
		 * processes before this method is invoked. 
		 */
		this.executor.shutdownNow();
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
