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
 * Strategy for implementing java-based Applications Containers.
 *
 * @author Oleg Zhurakousky
 *
 */
public interface ApplicationContainerProcessor {

	/**
	 * This method takes {@link ByteBuffer} containing data to be processed by an Application Container.
	 * 
	 * @param input
	 * 		input data
	 * @return
	 */
	public ByteBuffer process(ByteBuffer input);
}