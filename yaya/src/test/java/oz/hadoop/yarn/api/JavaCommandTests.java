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
public class JavaCommandTests {

	@Test(expected=IllegalArgumentException.class)
	public void validateNullMainClassFailure(){
		new JavaCommand(null);
	}

	@Test
	public void validateInvalidMain(){
		try {
			new JavaCommand(InvalidMainNoMain.class);
			fail();
		} catch (IllegalArgumentException e) {
			// ignore
		}
		try {
			new JavaCommand(InvalidMainPrivateMain.class);
			fail();
		} catch (IllegalArgumentException e) {
			// ignore
		}
		try {
			new JavaCommand(InvalidMainNonStaticMain.class);
			fail();
		} catch (IllegalArgumentException e) {
			// ignore
		}
		try {
			new JavaCommand(InvalidMainNonVoidReturnType.class);
			fail();
		} catch (IllegalArgumentException e) {
			// ignore
		}
		try {
			new JavaCommand(InvalidMainWrongArguments.class);
			fail();
		} catch (IllegalArgumentException e) {
			// ignore
		}
	}

	@Test
	public void validateValidMain(){
		JavaCommand javaCommand = new JavaCommand(ValidMain.class);
		String command = javaCommand.build("hello", 1);
		assertEquals("containerCount 1 memory 128 virtualCores 1 priority 0 " +
				"amSpec oz.hadoop.yarn.api.ApplicationMasterSpec command 'java oz.hadoop.yarn.api.JavaCommandTests$ValidMain'", command);
	}

	private static class ValidMain {
		public static void main(String[] args){}
	}

	private static class InvalidMainNoMain {
	}

	private static class InvalidMainPrivateMain {
		private static void main(String[] args){}
	}

	private static class InvalidMainNonStaticMain {
		public void main(String[] args){}
	}

	private static class InvalidMainNonVoidReturnType {
		public static String main(String[] args){
			return "";
		}
	}
	private static class InvalidMainWrongArguments {
		public static void main(String args){}
	}
}
