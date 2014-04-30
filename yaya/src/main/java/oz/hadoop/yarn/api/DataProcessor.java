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
package oz.hadoop.yarn.api;

import java.nio.ByteBuffer;

/**
 * Strategy to represent launched Application Containers of the YARN application assembled
 * by {@link YarnAssembly}.
 * It is created and returned by calling {@link YarnApplication#launch()} method
 * for the cases where {@link YarnAssembly} assembled long-running reusable YARN application.
 * 
 * @author Oleg Zhurakousky
 *
 */
public interface DataProcessor {

	/**
	 * Allows you to submit data as {@link ByteBuffer} to a first available 
	 * Application Container.
	 * 
	 * @param data
	 * 		data to process
	 */
	void process(ByteBuffer data);
	
	/**
	 * Allows you to submit data as {@link ByteBuffer} to a first available 
	 * Application Container that matches the IP filter described via regular expression.
	 * 
	 * @param data
	 * 		data to process
	 * @param ipRegexFilter
	 * 		regular expression for IP address filtering (e.g., "192\.168\.19\.(1[0-5])")
	 */
	// TODO, may be instead of a void return the host name of the AC
	void process(ByteBuffer data, String ipRegexFilter);
	
	/**
	 * The amount of successful calls to {@link #process(ByteBuffer)} or
	 * {@link #process(ByteBuffer, String)} methods since the launch of the application.
	 * 
	 * @return
	 */
	long completedSinceStart();
	
	/**
	 * Returns the amount of distributed Application Containers this strategy represents.
	 * 
	 * @return
	 */
	int containers();
	
//	/**
//	 * Allow for the registration of the {@link DataProcessorReplyListener} for the cases where
//	 * you need to deal with replies produced by the {@link ApplicationContainerProcessor}s or commands executed 
//	 * by Application Containers.
//	 *
//	 * @param replyListener
//	 */
//	void registerReplyListener(DataProcessorReplyListener replyListener);
}
