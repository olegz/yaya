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

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.Semaphore;

/**
 * 
 * @author Oleg Zhurakousky
 *
 */
public interface ContainerDelegate {

	/**
	 * Returns the host name of the active Application Container represented by this 
	 * ContainerDelegate.
	 */
	InetSocketAddress getHost();

	/**
	 * A general purpose method which allows data represented as 
	 * {@link ByteBuffer}s to be sent for processing to the Application Container 
	 * represented by this ContainerDelegate.
	 */
	void process(ByteBuffer data, ReplyPostProcessor replyPostProcessor);
	
	/**
	 * Since ContainerDelegate is a proxy to the actual Application Container, depending on the 
	 * implementation details of the Application Container, this delegate may need to be aware
	 * of when Application Container finishes the task. If so this method could be implemented
	 * to signal such task completion.
	 * In the current implementation each Application Container dedicates a single thread to
	 * process user's task and should not be able to accept more tasks until previous task completes.
	 * In other words subsequent calls to {@link #process(ByteBuffer, ReplyPostProcessor)} method
	 * should block until previous call completes. Since invocation of {@link #process(ByteBuffer, ReplyPostProcessor)}
	 * happens over the network, its naturally asynchronous and implementation of {@link ContainerDelegateImpl}
	 * will use {@link Semaphore} to govern invocations of {@link #process(ByteBuffer, ReplyPostProcessor)} method.
	 * <br>
	 * Having said that, in the future we may provide different options where Application Containers will 
	 * use multiple threads to process multiple tasks, hence this method is optional and depends on 
	 * the control logic provided by the implementtaion of this strategy.
	 */
	void release();
	
	
	boolean available();

}