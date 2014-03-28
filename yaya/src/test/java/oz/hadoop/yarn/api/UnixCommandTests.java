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

import org.junit.Test;

/**
 * @author Oleg Zhurakousky
 *
 */
public class UnixCommandTests {

	@Test(expected=IllegalArgumentException.class)
	public void validateNullCommandFailure(){
		new UnixCommand(null);
	}

	@Test(expected=IllegalArgumentException.class)
	public void validateEmptyCommandFailure(){
		new UnixCommand("");
	}

	@Test(expected=IllegalArgumentException.class)
	public void validateEmptyCommandFailureWithSpaceString(){
		new UnixCommand("   ");
	}

	@Test
	public void validateCommandWithDefaultContainerArguments(){
		UnixCommand unixCommand = new UnixCommand("cal");
		String command = unixCommand.build("hello", 1);
		assertEquals("containerCount 1 memory 128 virtualCores 1 priority 0 amSpec oz.hadoop.yarn.api.ApplicationMasterSpec command cal", command);
	}

	@Test
	public void validateCommandWithOverridenContainerArguments(){
		UnixCommand unixCommand = new UnixCommand("ls");
		unixCommand.setContainerCount(3);
		unixCommand.setMemory(512);
		unixCommand.setPriority(4);
		unixCommand.setVirtualCores(4);
		String command = unixCommand.build("hello", 1);
		assertEquals("containerCount 3 memory 512 virtualCores 4 priority 4 amSpec oz.hadoop.yarn.api.ApplicationMasterSpec command ls", command);
	}

	@Test
	public void validateContainerArgumentIllegalValues(){
		UnixCommand unixCommand = new UnixCommand("ls");
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
