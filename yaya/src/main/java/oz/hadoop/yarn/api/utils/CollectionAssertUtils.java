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
package oz.hadoop.yarn.api.utils;

import java.util.Collection;

/**
 * @author Oleg Zhurakousky
 *
 */
public class CollectionAssertUtils {

	/**
	 *
	 * @param collection
	 */
	public static void assertNotEmpty(Collection<?> collection){
		if (collection == null || collection.size() == 0){
			throw new IllegalArgumentException("'collection' must not be null or empty");
		}
	}

	/**
	 *
	 * @param array
	 */
	public static void assertNotEmpty(Object[] array){
		if (array == null || array.length == 0){
			throw new IllegalArgumentException("'array' must not be null or empty");
		}
	}

	/**
	 *
	 * @param collection
	 * @param size
	 */
	public static void assertSize(Collection<?> collection, int size){
		assertNotEmpty(collection);
		if (collection.size() != size){
			throw new IllegalArgumentException("'collection' must be of size " + size + " was " + collection.size());
		}
	}

	public static void assertSize(Object[] array, int size){
		assertNotEmpty(array);
		if (array.length != size){
			throw new IllegalArgumentException("'array' must be of size " + size + " was " + array.length);
		}
	}
}
