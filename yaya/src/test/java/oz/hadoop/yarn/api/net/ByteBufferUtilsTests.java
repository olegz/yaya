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

import org.junit.Test;

/**
 * @author Oleg Zhurakousky
 *
 */
public class ByteBufferUtilsTests {

	@Test
	public void validateMergeWithoutExpension(){
		ByteBuffer b1 = ByteBuffer.allocate(100);
		b1.put("Hello ".getBytes());
		ByteBuffer b2 = ByteBuffer.allocate(10);
		b2.put("Wolrd!".getBytes());
		b2.flip();
		b1 = ByteBufferUtils.merge(b1, b2);
		System.out.println(new String(b1.array()));
	}

	@Test
	public void validateMergeWithExpension(){
		ByteBuffer b1 = ByteBuffer.allocate(10);
		b1.put("Hello ".getBytes());
		ByteBuffer b2 = ByteBuffer.allocate(10);
		b2.put("Wolrd!".getBytes());
		b2.flip();
		b1 = ByteBufferUtils.merge(b1, b2);
		System.out.println(new String(b1.array()));

		b2.clear();
		b2.put(" Hello".getBytes());
		b2.flip();
		b1 = ByteBufferUtils.merge(b1, b2);
		System.out.println(new String(b1.array()));

		b2.clear();
		b2.put(" Again!".getBytes());
		b2.flip();
		b1 = ByteBufferUtils.merge(b1, b2);
		System.out.println(new String(b1.array()));
	}
}
