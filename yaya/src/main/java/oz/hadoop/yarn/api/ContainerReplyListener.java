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
 * Strategy for implementing listeners which could be registered with {@link DataProcessor}
 * to be invoked every time an Application Container task running as command or 
 * {@link ApplicationContainerProcessor} produces a reply.
 * 
 * @author Oleg Zhurakousky
 *
 */
public interface ContainerReplyListener {

	/**
	 * Callback method to receive and process a reply produced by a task running as
	 * command or {@link ApplicationContainerProcessor}
	 * 
	 * @param replyData
	 */
	void onReply(ByteBuffer replyData);
}
