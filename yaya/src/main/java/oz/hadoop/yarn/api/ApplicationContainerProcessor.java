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

import oz.hadoop.yarn.api.utils.PrimitiveImmutableTypeMap;

/**
 * Strategy for implementing java-based Applications Containers.
 * See {@link #launch(PrimitiveImmutableTypeMap)} method.
 *
 * @author Oleg Zhurakousky
 *
 */
public interface ApplicationContainerProcessor {

	/**
	 * This method takes {@link PrimitiveImmutableTypeMap} which contains
	 * container arguments that were set when creating
	 * {@link JavaApplicationContainerSpec#addContainerArgument(String, java.util.List)} or
	 * {@link JavaApplicationContainerSpec#addContainerArgument(String, String)}.
	 * NOTE: Every argument value in {@link PrimitiveImmutableTypeMap} is either of type
	 * String or {@link List<String>} or {@link Map<String, String>} or {@link Map<String, List<String>>}
	 * or {@link Map<String, Map<String, String>} etc.
	 *
	 * @param arguments
	 */
	public ByteBuffer process(ByteBuffer input);
}