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

import java.util.Map;
import java.util.Map.Entry;

/**
 * @author Oleg Zhurakousky
 *
 */
public class PrintUtils {
	
	private static final String TAB = "  ";

	/**
	 * 
	 * @param map
	 * @return
	 */
	public static String prettyMap(Map<?, ?> map){
		return doPrettyMap(map, "");
	}
	
	/**
	 * 
	 * @param map
	 * @param delimiter
	 * @return
	 */
	public static String prettyMap(Map<?, ?> map, String delimiter){
		return doPrettyMap(map, delimiter);
	}
	
	/**
	 * 
	 */
	private static String doPrettyMap(Map<?, ?> map, String delimiter){
		StringBuffer buffer = new StringBuffer();
		String nlchar = "";
		buffer.append(nlchar);
		buffer.append("{\n");
		for (Entry<?, ?> entry : map.entrySet()) {
			buffer.append(nlchar);
			buffer.append(delimiter);
			buffer.append(TAB);
			buffer.append(entry.getKey());
			buffer.append("=");
			Object value = entry.getValue();
			if (value instanceof Map){
				buffer.append(doPrettyMap((Map<?, ?>) value, delimiter + TAB));
			}
			else if (value instanceof Iterable<?>) {
				buffer.append(doPrettyIterable((Iterable<?>) value, delimiter + TAB));
			}
			else {
				buffer.append(value);
			}
			if (nlchar.equals("")){
				nlchar = "\n";
			}
		}
		buffer.append("\n");
		buffer.append(delimiter);
		buffer.append("}");
		return buffer.toString();
	}
	
	/**
	 * 
	 */
	private static String doPrettyIterable(Iterable<?> iterable, String delimiter) {
		StringBuffer buffer = new StringBuffer();
		String nlchar = "";
		buffer.append(nlchar);
		buffer.append("[\n");
		for (Object value : iterable) {
			buffer.append(nlchar);
			buffer.append(delimiter);
			buffer.append(TAB);
			if (value instanceof Map){
				buffer.append(doPrettyMap((Map<?, ?>) value, delimiter + TAB));
			}
			else if (value instanceof Iterable<?>){
				buffer.append(doPrettyIterable((Iterable<?>) value, delimiter + TAB));
			}
			else {
				buffer.append(value);
			}
			if (nlchar.equals("")){
				nlchar = "\n";
			}
		}
		
		buffer.append("\n");
		buffer.append(delimiter);
		buffer.append("]");
		return buffer.toString();
	}
}
