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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.StringUtils;

import oz.hadoop.yarn.api.DataProcessor;
import oz.hadoop.yarn.api.net.ApplicationContainerServer;
import oz.hadoop.yarn.api.net.ContainerDelegate;
import oz.hadoop.yarn.api.net.ReplyPostProcessor;

/**
 * INTERNAL API
 * 
 * @author Oleg Zhurakousky
 *
 */
class DataProcessorImpl implements DataProcessor {
	
	private final Log logger = LogFactory.getLog(DataProcessorImpl.class);
	
	private final AtomicLong completedSinceStart;
	
	private final AtomicBoolean[] busyDelegatesFlags;
	
	private final ContainerDelegate[] containerDelegates;
	
	private final ApplicationContainerServer clientServer;
	
	private volatile boolean active;

	/**
	 * 
	 * @param containerDelegates
	 */
	DataProcessorImpl(ApplicationContainerServer clientServer) {
		this.clientServer = clientServer;
		this.containerDelegates = this.clientServer.getContainerDelegates();
		this.busyDelegatesFlags = new AtomicBoolean[containerDelegates.length];
		for (int i = 0; i < busyDelegatesFlags.length; i++) {
			this.busyDelegatesFlags[i] = new AtomicBoolean();
		}
		this.completedSinceStart = new AtomicLong();
		this.active = true;
	}
	
	/**
	 * 
	 */
	@Override
	public long completedSinceStart() {
		return this.completedSinceStart.get();
	}

	/**
	 * 
	 */
	@Override
	public int containers() {
		return containerDelegates.length;
	}
	
	/**
	 * 
	 */
	@Override
	public void process(ByteBuffer data) {
		
		this.process(data, null);
	}
	
	/**
	 * 
	 */
	@Override
	public void process(ByteBuffer data, String ipRegexFilter) {
		if (this.active){
			final int index = this.getIndexOfAvailableDelegate(ipRegexFilter);
			if (index >= 0){
				final ContainerDelegate delegate = this.containerDelegates[index];
				if (logger.isDebugEnabled()){
					logger.debug("Selected ContainerDelegate for process invocation: " + delegate);
				}
				
				ReplyPostProcessor replyPostProcessor = new ReplyPostProcessor() {
					@Override
					public void doProcess(ByteBuffer reply) {
						/*
						 * This is release logic which will make ContainerDelegate available again.
						 */
						if (!DataProcessorImpl.this.busyDelegatesFlags[index].compareAndSet(true, false)) {
							logger.error("Failed to release 'busyDelegatesFlag'. Should never happen. Concurrency issue; if you see this message, REPORT!");
							DataProcessorImpl.this.stop();
							throw new IllegalStateException("Should never happen. Concurrency issue, if you see this message, REPORT!");
						}
					}
				};
				if (logger.isDebugEnabled()){
					logger.debug("Submitting data " + data + " to the Application Container");
				}
				delegate.process(data, replyPostProcessor);
				this.completedSinceStart.getAndIncrement();
			}
			else {
				logger.debug("Process awaiting available container delegate was discarded due to application termination.");
			}
		}
		else {
			logger.warn("Rejecting submission due to the shutdown. Completed processes: " + this.completedSinceStart.get());
			throw new RejectedExecutionException("Rejecting submission due to a termination");
		}
	}
	
	/**
	 * 
	 */
	void stop(){
		this.active = false;
	}

	/**
	 * 
	 * @param ipRegexFilter
	 * @return
	 */
	private int getIndexOfAvailableDelegate(String ipRegexFilter){
		int index = -1;
		boolean found = false;
		while (!found && this.active){
			for (int i = 0; i < this.busyDelegatesFlags.length && !found; i++) {
				found = this.busyDelegatesFlags[i].compareAndSet(false, true);
				if (found){
					if (isMatch(index, ipRegexFilter)){
						index = i;
					}
					else {
						found = false;
						if (this.busyDelegatesFlags[index].compareAndSet(true, false)){
							logger.error("Failed to release 'busyDelegatesFlag'. Should never happen. Concurrency issue; if you see this message, REPORT!");
							DataProcessorImpl.this.stop();
							throw new IllegalStateException("Should never happen. Concurrency issue, if you see this message, REPORT!");
						}
					}
				}
			}
			LockSupport.parkNanos(10000);
		}
		return index;
	}
	
	/**
	 * 
	 * @param index
	 * @param ipRegexFilter
	 * @return
	 */
	private boolean isMatch(int index, String ipRegexFilter) {
		if (StringUtils.hasText(ipRegexFilter)){
			ContainerDelegate delegate = this.containerDelegates[index];
			String delegateIpAddress = delegate.getHost().getAddress().getHostAddress();
			Pattern pattern = Pattern.compile(ipRegexFilter);
			Matcher matcher = pattern.matcher(delegateIpAddress);
			return matcher.find();
		}
		else {
			return true;
		}
	}
}
