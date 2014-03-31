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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Test;

import oz.hadoop.yarn.api.utils.ReflectionUtils;

/**
 * @author Oleg Zhurakousky
 *
 */
public class YarnApplicationBuilderTests {

	@Test
	public void validateDefault(){
		UnixApplicationContainerSpec appSpec = new UnixApplicationContainerSpec("cal");
		YarnApplicationBuilder builder = YarnApplicationBuilder.forApplication("foo", appSpec);
		assertEquals("foo", ReflectionUtils.getFieldValue(builder, "applicationName"));
		assertNotNull(ReflectionUtils.getFieldValue(builder, "yarnConfig"));
		assertEquals(appSpec, ReflectionUtils.getFieldValue(builder, "applicationContainerSpec"));
		Resource capability = (Resource) ReflectionUtils.getFieldValue(builder, "capability");
		assertEquals(2048, capability.getMemory());
		assertEquals(1, capability.getVirtualCores());
		assertEquals("default", ReflectionUtils.getFieldValue(builder, "queueName"));
		assertEquals(0, ReflectionUtils.getFieldValue(builder, "priority"));
		assertEquals(1, ReflectionUtils.getFieldValue(builder, "maxAttempts"));
	}

	@Test
	public void validateCustomized(){
		YarnConfiguration yarnConfig = new YarnConfiguration();
		UnixApplicationContainerSpec appSpec = new UnixApplicationContainerSpec("cal");
		YarnApplicationBuilder builder = YarnApplicationBuilder.forApplication("foo", appSpec).
				setMaxAttempts(2).
				setMemory(128).
				setPriority(3).
				setQueueName("bar").
				setVirtualCores(5).
				setYarnConfiguration(yarnConfig);

		assertEquals("foo", ReflectionUtils.getFieldValue(builder, "applicationName"));
		assertEquals(yarnConfig, ReflectionUtils.getFieldValue(builder, "yarnConfig"));
		assertEquals(appSpec, ReflectionUtils.getFieldValue(builder, "applicationContainerSpec"));
		Resource capability = (Resource) ReflectionUtils.getFieldValue(builder, "capability");
		assertEquals(128, capability.getMemory());
		assertEquals(5, capability.getVirtualCores());
		assertEquals("bar", ReflectionUtils.getFieldValue(builder, "queueName"));
		assertEquals(3, ReflectionUtils.getFieldValue(builder, "priority"));
		assertEquals(2, ReflectionUtils.getFieldValue(builder, "maxAttempts"));
	}

}
