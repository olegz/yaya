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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.Assert;

import oz.hadoop.yarn.api.utils.PrintUtils;
import oz.hadoop.yarn.api.utils.ReflectionUtils;

/**
 * INTERNAL API
 * 
 * Launcher for launching command-based tasks
 * 
 * @author Oleg Zhurakousky
 *
 */
class CommandProcessLauncher extends ProcessLauncher {
	
	private final Log logger = LogFactory.getLog(CommandProcessLauncher.class);

	private final String command;
	
	private final AtomicInteger streamsFinished;
	
	private volatile Process process;
	
	/**
	 * 
	 * @param command
	 * @param containerLivelinesBarrier
	 */
	public CommandProcessLauncher(String command, CountDownLatch containerLivelinesBarrier) {
		super(containerLivelinesBarrier);
		Assert.hasText(command, "'command' must not be null or empty");
		this.command = command;
		this.streamsFinished = new AtomicInteger();
	}

	/**
	 * 
	 */
	@Override
	public void launch() {
		try {
			logger.info("Executing command " + this.command);
			this.process = Runtime.getRuntime().exec(this.command);
			
			this.executor.execute(new Runnable() {
				@Override
				public void run() {
					PrintUtils.printInputStreamToConsole(CommandProcessLauncher.this.process.getInputStream(), false);
					streamFinished();
				}
			});
			this.executor.execute(new Runnable() {
				@Override
				public void run() {
					PrintUtils.printInputStreamToConsole(CommandProcessLauncher.this.process.getErrorStream(), true);
					streamFinished();
				}
			});
			this.awaitCompletion();
		} 
		catch (Exception e) {
			logger.error("Command '" + this.command + "' failed.", e);
		}
	}
	
	/**
	 * 
	 */
	private void streamFinished(){
		int sFinished = this.streamsFinished.incrementAndGet();
		this.containerLivelinesBarrier.countDown();
		if (sFinished == 2){
			if (logger.isDebugEnabled()){
				if (logger.isDebugEnabled()){
					logger.debug("Process has completed");
				}
			}
		}
	}

	/**
	 * 
	 */
	@Override
	public void finish() {
		if (!((boolean) ReflectionUtils.getFieldValue(this.process, "hasExited"))){
			this.process.destroy();
		}
		while (!((boolean) ReflectionUtils.getFieldValue(this.process, "hasExited"))){
			LockSupport.parkNanos(1000);
		}
		int exitCode = this.process.exitValue();
		logger.info("Command finished with exit code: " + exitCode);
		super.finish();
	}
}
