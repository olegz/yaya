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
	 * 
	 */
	void suspend();
	
	/**
	 * 
	 * @return
	 */
	boolean available();

}