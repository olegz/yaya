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

import org.apache.commons.collections.BufferOverflowException;

/**
 * An internal utility class that allows merging of the two ByteBuffers into one while 
 * expending the final buffer as necessary. 
 * 
 * @author Oleg Zhurakousky
 *
 */
class ByteBufferUtils {

	private static int threshold = 1073741824; // 1GB

	/**
	 * 
	 * @param source
	 * @param target
	 * @return
	 */
	public static ByteBuffer merge(ByteBuffer source, ByteBuffer target) {
		if (target.position() != 0){
			target.flip();
		}
		int rPos = source.position() + target.limit();
		if (rPos <= source.capacity()){
			source.put(target);
			return source;
		}
		else {
			int newLength = calculateNewLength(source, target);
			if (newLength > 0){
				ByteBuffer b = ByteBuffer.allocate(newLength);
				source.flip();
				b.put(source);
				b.put(target);
				return b;
			}
			else {
				throw new BufferOverflowException("Buffer can no longer be expended. Maximum allowed size is " + threshold + " bytes.");
			}
		}
	}

	/**
	 * 
	 */
	private static int calculateNewLength(ByteBuffer source, ByteBuffer target){
		long newLength = source.capacity() + (target.limit() - source.remaining());
		if (newLength <= threshold){
			return (int) newLength;
		}
		return -1;
	}
}
