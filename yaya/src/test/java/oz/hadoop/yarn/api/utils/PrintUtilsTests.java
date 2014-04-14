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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

/**
 * @author Oleg Zhurakousky
 *
 */
public class PrintUtilsTests {

	/**
	 * There is not much to assert other then it does not throws an exception with 100% test coverage.
	 */
	@Test
	public void validatePrintUtilsNotTHrowingException(){
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("name", "Joe");
		map.put("names", new String[]{"Seva", "Jad", "Cameron"});
		map.put("list", Arrays.asList(new String[]{"foo", "bar", "baz"}));
		map.put("list", Arrays.asList(new String[]{"foo", "bar", "baz"}));
		
		map.put("list2", Arrays.asList(new Object[]{new HashMap<>(), new ArrayList<>(), "foo", new Object[]{"foo", 2}}));
		
		Map<String, Object> map2 = new HashMap<String, Object>();
		map2.put("list", Arrays.asList(new String[]{"foo", "bar", "baz"}));
		map.put("map", map2);
		PrintUtils.prettyMap(map);
		PrintUtils.prettyMap(map, "\t");
	}
}
