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
package oz.hadoop.yarn.api.net;

import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Internal utility class used by Socket handlers ({@link ApplicationContainerServer} & {@link ApplicationContainerClient}) 
 * in this package to manage pools of {@link ByteBuffer} essentially allowing used {@link ByteBuffer}s to be reused 
 * instead of reallocating memory.
 * 
 * @author Oleg Zhurakousky
 *
 */
class ByteBufferPoll {
	
	private final Log logger = LogFactory.getLog(ByteBufferPoll.class);
	
	private static final int DEF_SIZE = 1048576; // 1MB
	
	private static final int BUFFER_COUNT = 10;
	
	private final BlockingQueue<ByteBuffer> bufferQueue;
	
	private final int initialBufferSize;

	/**
	 * 
	 */
	public ByteBufferPoll(){
		this(DEF_SIZE, BUFFER_COUNT);
	}
	
	/**
	 * 
	 * @param initialBufferSize
	 * @param bufferCount
	 */
	public ByteBufferPoll(int initialBufferSize, int bufferCount){
		this.initialBufferSize = initialBufferSize;
		this.bufferQueue = new ArrayBlockingQueue<>(bufferCount);
	}	
	
	/**
	 * Will return the first available {@link ByteBuffer} from the pool
	 * or new one
	 */
	public ByteBuffer poll() {
		ByteBuffer buffer = this.bufferQueue.poll();
		if (buffer == null){
			if (logger.isDebugEnabled()){
				logger.debug("Creating new buffer with capacity: " + this.initialBufferSize);
			}
			buffer = ByteBuffer.allocate(this.initialBufferSize);
		}
		else {
			logger.debug("Polled buffer");
		}
		buffer.clear();
		return buffer;
	}
	
	/**
	 * Will release buffer to a common pool or discard if queue is full.
	 * 
	 * @param buffer
	 */
	public void release(ByteBuffer buffer){
		logger.debug("Releasing buffer");
		this.bufferQueue.offer(buffer);
	}
}
