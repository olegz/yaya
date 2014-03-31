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
import static org.junit.Assert.fail;

import java.util.Map;

import org.junit.Test;

import oz.hadoop.yarn.api.utils.ReflectionUtils;



/**
 * @author Oleg Zhurakousky
 *
 */
public class UnixApplicationContainerSpecTests {

	@Test(expected=IllegalArgumentException.class)
	public void validateNullCommandFailure(){
		new UnixApplicationContainerSpec(null);
	}

	@Test(expected=IllegalArgumentException.class)
	public void validateEmptyCommandFailure(){
		new UnixApplicationContainerSpec("");
	}

	@Test(expected=IllegalArgumentException.class)
	public void validateEmptyCommandFailureWithSpaceString(){
		new UnixApplicationContainerSpec("   ");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void validateCommandWithDefaultContainerArguments(){
		UnixApplicationContainerSpec unixContainer = new UnixApplicationContainerSpec("cal");
		Map<String, Object> containerSpec = (Map<String, Object>) ReflectionUtils.getFieldValue(unixContainer, "containerSpec");
		assertEquals(AbstractApplicationContainerSpec.amSpec.getName(), containerSpec.get(AbstractApplicationContainerSpec.AM_SPEC));
		assertEquals(AbstractApplicationContainerSpec.containerCount, containerSpec.get(AbstractApplicationContainerSpec.CONTAINER_COUNT));
		assertEquals(AbstractApplicationContainerSpec.memory, containerSpec.get(AbstractApplicationContainerSpec.MEMORY));
		assertEquals(AbstractApplicationContainerSpec.priority, containerSpec.get(AbstractApplicationContainerSpec.PRIORITY));
		assertEquals(AbstractApplicationContainerSpec.virtualCores, containerSpec.get(AbstractApplicationContainerSpec.VIRTUAL_CORES));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void validateCommandWithOverridenContainerArguments(){
		UnixApplicationContainerSpec unixContainer = new UnixApplicationContainerSpec("ls");
		unixContainer.setMemory(128);
		unixContainer.setVirtualCores(2);
		unixContainer.setPriority(3);
		Map<String, Object> containerSpec = (Map<String, Object>) ReflectionUtils.getFieldValue(unixContainer, "containerSpec");
		assertEquals(AbstractApplicationContainerSpec.amSpec.getName(), containerSpec.get(AbstractApplicationContainerSpec.AM_SPEC));
		assertEquals(AbstractApplicationContainerSpec.containerCount, containerSpec.get(AbstractApplicationContainerSpec.CONTAINER_COUNT));
		assertEquals(128, containerSpec.get(AbstractApplicationContainerSpec.MEMORY));
		assertEquals(3, containerSpec.get(AbstractApplicationContainerSpec.PRIORITY));
		assertEquals(2, containerSpec.get(AbstractApplicationContainerSpec.VIRTUAL_CORES));
	}

	@Test
	public void validateContainerArgumentIllegalValues(){
		UnixApplicationContainerSpec unixCommand = new UnixApplicationContainerSpec("ls");
		try {
			unixCommand.setContainerCount(0);
			fail();
		} catch (IllegalArgumentException e) {
			//ignore
		}
		try {
			unixCommand.setMemory(0);
			fail();
		} catch (IllegalArgumentException e) {
			//ignore
		}
		try {
			unixCommand.setPriority(-1);
			fail();
		} catch (IllegalArgumentException e) {
			//ignore
		}
		try {
			unixCommand.setVirtualCores(0);
			fail();
		} catch (IllegalArgumentException e) {
			//ignore
		}
	}
}
