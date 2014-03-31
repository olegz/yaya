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
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.junit.Test;

import oz.hadoop.yarn.api.utils.ReflectionUtils;

/**
 * @author Oleg Zhurakousky
 *
 */
public class JavaApplicationContainerSpecTests {

	@Test(expected=IllegalArgumentException.class)
	public void validateNullMainClassFailure(){
		new JavaApplicationContainerSpec(null);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void validateDefault(){
		JavaApplicationContainerSpec javaContainer = new JavaApplicationContainerSpec(MyApplicationContainer.class);
		Map<String, Object> containerSpec = (Map<String, Object>) ReflectionUtils.getFieldValue(javaContainer, "containerSpec");

		assertEquals(AbstractApplicationContainerSpec.amSpec.getName(), containerSpec.get(AbstractApplicationContainerSpec.AM_SPEC));
		assertEquals(AbstractApplicationContainerSpec.containerCount, containerSpec.get(AbstractApplicationContainerSpec.CONTAINER_COUNT));
		assertEquals(AbstractApplicationContainerSpec.memory, containerSpec.get(AbstractApplicationContainerSpec.MEMORY));
		assertEquals(AbstractApplicationContainerSpec.priority, containerSpec.get(AbstractApplicationContainerSpec.PRIORITY));
		assertEquals(AbstractApplicationContainerSpec.virtualCores, containerSpec.get(AbstractApplicationContainerSpec.VIRTUAL_CORES));

		Map<String, Object> containerArguments = (Map<String, Object>) ReflectionUtils.getFieldValue(javaContainer, "containerArguments");
		assertTrue(containerArguments.isEmpty());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void validateValidMainNonDefault(){
		JavaApplicationContainerSpec javaContainer = new JavaApplicationContainerSpec(MyApplicationContainer.class);
		javaContainer.addContainerArgument("name1", "Oleg");
		javaContainer.addContainerArgument("name2", "Anastasiya");
		javaContainer.setMemory(128);
		javaContainer.setVirtualCores(2);
		javaContainer.setPriority(3);
		Map<String, Object> containerSpec = (Map<String, Object>) ReflectionUtils.getFieldValue(javaContainer, "containerSpec");
		assertEquals(AbstractApplicationContainerSpec.amSpec.getName(), containerSpec.get(AbstractApplicationContainerSpec.AM_SPEC));
		assertEquals(AbstractApplicationContainerSpec.containerCount, containerSpec.get(AbstractApplicationContainerSpec.CONTAINER_COUNT));
		assertEquals(128, containerSpec.get(AbstractApplicationContainerSpec.MEMORY));
		assertEquals(3, containerSpec.get(AbstractApplicationContainerSpec.PRIORITY));
		assertEquals(2, containerSpec.get(AbstractApplicationContainerSpec.VIRTUAL_CORES));

		Map<String, Object> containerArguments = (Map<String, Object>) ReflectionUtils.getFieldValue(javaContainer, "containerArguments");
		assertEquals("Oleg", containerArguments.get("name1"));
		assertEquals("Anastasiya", containerArguments.get("name2"));
	}

	/**
	 */
	private static class MyApplicationContainer implements JavaApplicationContainer {
		@Override
		public void launch(PrimitiveImmutableTypeMap arguments) {
			System.out.println("Arguments: " + arguments);
		}
	}
}
