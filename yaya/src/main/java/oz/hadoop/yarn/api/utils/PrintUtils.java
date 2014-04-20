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

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Utility class for a variety of printing functions
 * 
 * @author Oleg Zhurakousky
 *
 */
public class PrintUtils {
	
	private static final Log logger = LogFactory.getLog(PrintUtils.class);
	
	private static final String TAB = "  ";
	
	/**
	 * Will read the {@link InputStream} and print its contents to a either 
	 * System.out or System.err depending on the 'error' flag.
	 * Upon completion it will attempt to close the {@link InputStream}.
	 * 
	 * Will handle any exceptions that may arise from stream processing and
	 * log them at the ERROR level.
	 * 
	 * @param is
	 * @param error
	 */
	public static void printInputStreamToConsole(InputStream is, boolean error){
		BufferedReader streamReader = null;
		try {
			
			streamReader = new BufferedReader(new InputStreamReader(is));
			String line;
			while ((line = streamReader.readLine()) != null){
				if (error){
					System.err.println("[PROCESS]: " + line);
				}
				else {
					System.out.println("[PROCESS]: " + line);
				}
			}
		} 
		catch (Exception e) {
			logger.error("Stream processing failed with Exception.", e);
		}
		finally {
			if (streamReader != null){
				try {
					streamReader.close();
				} catch (Exception e) {
					// ignore
				}
			}
		}
	}

	/**
	 * Will pretty-print the contents of the {@link Map} using two spaces as a delimiter.
	 * 
	 * @param map
	 * @return
	 */
	public static String prettyMap(Map<?, ?> map){
		return doPrettyMap(map, "");
	}
	
	/**
	 * Will pretty-print the contents of the {@link Map} using provided delimiter.
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
			else if (value instanceof Object[]) {
				buffer.append(doPrettyIterable(Arrays.asList((Object[])value), delimiter + TAB));
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
			else if (value instanceof Iterable<?>) {
				buffer.append(doPrettyIterable((Iterable<?>) value, delimiter + TAB));
			}
			else if (value instanceof Object[]) {
				buffer.append(doPrettyIterable(Arrays.asList((Object[])value), delimiter + TAB));
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
