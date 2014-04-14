/*
 * Copyright 2013 the original author or authors.
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

import java.util.HashMap;
import java.util.Map;


/**
 * A convenience class for reading JSON Map in a type-safe way
 * allowing values to be read as primitives (int, long, float, double, boolean, String).
 *
 * @author Oleg Zhurakousky
 *
 */
@SuppressWarnings("serial")
public class PrimitiveImmutableTypeMap extends HashMap<String, Object> {

	/**
	 * Not to be created as default.
	 */
	@SuppressWarnings("unused")
	private PrimitiveImmutableTypeMap() {
		super(new HashMap<String, Object>());
	}

	/**
	 * Wraps injected {@link Map} as {@link PrimitiveImmutableTypeMap}.
	 *
	 * @param map
	 */
	public PrimitiveImmutableTypeMap(Map<String, Object> map) {
		super(map);
	}

	/**
	 *
	 */
	@Override
	public Object put(String key, Object value) {
		throw new UnsupportedOperationException("This map is immutable");
	}

	/**
	 *
	 */
	@Override
	public void putAll(Map<? extends String, ? extends Object> m) {
		throw new UnsupportedOperationException("This map is immutable");
	}

	/**
	 *
	 */
	@Override
	public Object remove(Object key) {
		throw new UnsupportedOperationException("This map is immutable");
	}

	/**
	 *
	 * @param key
	 * @return
	 */
	public int getInt(String key) {
		Object value = this.get(key);
		ObjectAssertUtils.assertNotNull(value);
		return new Integer(value.toString());
	}

	/**
	 *
	 * @param key
	 * @return
	 */
	public float getFloat(String key) {
		Object value = this.get(key);
		ObjectAssertUtils.assertNotNull(value);
		return new Float(value.toString());
	}

	/**
	 *
	 * @param key
	 * @return
	 */
	public double getDouble(String key) {
		Object value = this.get(key);
		ObjectAssertUtils.assertNotNull(value);
		return new Double(value.toString());
	}

	/**
	 *
	 * @param key
	 * @return
	 */
	public long getLong(String key) {
		Object value = this.get(key);
		ObjectAssertUtils.assertNotNull(value);
		return new Long(value.toString());
	}

	/**
	 *
	 * @param key
	 * @return
	 */
	public boolean getBoolean(String key) {
		Object value = this.get(key);
		return value != null && new Boolean(value.toString());
	}

	/**
	 *
	 * @param key
	 * @return
	 */
	public String getString(String key) {
		Object value = this.get(key);
		if (value == null){
			return null;
		}
		return value.toString();
	}
}
