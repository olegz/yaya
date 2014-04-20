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

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

import java.nio.ByteBuffer;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Test;

import oz.hadoop.yarn.api.net.ContainerDelegate;
import oz.hadoop.yarn.api.utils.PrimitiveImmutableTypeMap;

/**
 * Also, see LocalApplicationLaunchTests
 * 
 * @author Oleg Zhurakousky
 *
 */
public class YarnAssemblyTests {
	
	@Test
	public void validateConfigurationFailures() {
		try {
			YarnAssembly.forApplicationContainer((String)null);
			fail();
		} catch (Exception e) {
			// ignore
		}
		
		try {
			YarnAssembly.forApplicationContainer("");
			fail();
		} catch (Exception e) {
			// ignore
		}
		
		try {
			YarnAssembly.forApplicationContainer("foo").containerCount(0);
			fail();
		} catch (Exception e) {
			// ignore
		}
		
		try {
			YarnAssembly.forApplicationContainer("foo").memory(0);
			fail();
		} catch (Exception e) {
			// ignore
		}
		
		try {
			YarnAssembly.forApplicationContainer("foo").virtualCores(0);
			fail();
		} catch (Exception e) {
			// ignore
		}
		
		try {
			YarnAssembly.forApplicationContainer("foo").withApplicationMaster().maxAttempts(0);
			fail();
		} catch (Exception e) {
			// ignore
		}
		
		try {
			YarnAssembly.forApplicationContainer("foo").withApplicationMaster().memory(0);
			fail();
		} catch (Exception e) {
			// ignore
		}
		
		try {
			YarnAssembly.forApplicationContainer("foo").withApplicationMaster().queueName(null);
			fail();
		} catch (Exception e) {
			// ignore
		}
		
		try {
			YarnAssembly.forApplicationContainer("foo").withApplicationMaster().queueName("");
			fail();
		} catch (Exception e) {
			// ignore
		}
		
		try {
			YarnAssembly.forApplicationContainer("foo").withApplicationMaster().queueName("foo bar");
			fail();
		} catch (Exception e) {
			// ignore
		}
		
		try {
			YarnAssembly.forApplicationContainer("foo").withApplicationMaster().virtualCores(0);
			fail();
		} catch (Exception e) {
			// ignore
		}
	}
	
	@Test
	public void validateCommandBasedContainerWithDefaults() {
		ApplicationMasterSpecBuilder<Void> applicationMasterBuilder = YarnAssembly.forApplicationContainer("ls -all").withApplicationMaster();
		YarnApplication<Void> yarnApplication = applicationMasterBuilder.build("my-application");
		
		PrimitiveImmutableTypeMap specMap = (PrimitiveImmutableTypeMap) yarnApplication.getApplicationSpecification();
		assertEquals(1, specMap.getInt(YayaConstants.VIRTUAL_CORES));
		assertEquals(0, specMap.getInt(YayaConstants.PRIORITY));
		assertEquals(1, specMap.getInt(YayaConstants.MAX_ATTEMPTS));
		assertEquals("default", specMap.getString(YayaConstants.QUEUE_NAME));
		assertEquals(512, specMap.getInt(YayaConstants.MEMORY));
		assertTrue(specMap.getBoolean(YayaConstants.YARN_EMULATOR));
		assertEquals("my-application", specMap.getString(YayaConstants.APPLICATION_NAME));
		
		PrimitiveImmutableTypeMap containerSpecMap = (PrimitiveImmutableTypeMap) specMap.get(YayaConstants.CONTAINER_SPEC);
		assertEquals(1, containerSpecMap.getInt(YayaConstants.CONTAINER_COUNT));
		assertEquals(0, containerSpecMap.getInt(YayaConstants.PRIORITY));
		assertEquals(256, containerSpecMap.getInt(YayaConstants.MEMORY));
		assertEquals(1, containerSpecMap.getInt(YayaConstants.VIRTUAL_CORES));
		assertEquals("ls -all", containerSpecMap.getString(YayaConstants.COMMAND));
	}
	
	@Test
	public void validateCommandBasedContainerWithCustom() {
		YarnConfiguration yarnConfig = new YarnConfiguration();
		ApplicationMasterSpecBuilder<Void> applicationMasterBuilder = YarnAssembly.forApplicationContainer("ls -all").
				memory(28).
				containerCount(4).
				virtualCores(34).
				priority(3).
				withApplicationMaster(yarnConfig);
		
		YarnApplication<Void> yarnApplication = applicationMasterBuilder.
				maxAttempts(12).
				priority(34).
				queueName("foo").
				virtualCores(35).
				memory(1024).
				build("my-application");
		
		PrimitiveImmutableTypeMap specMap = (PrimitiveImmutableTypeMap) yarnApplication.getApplicationSpecification();
		assertEquals(35, specMap.getInt(YayaConstants.VIRTUAL_CORES));
		assertEquals(34, specMap.getInt(YayaConstants.PRIORITY));
		assertEquals(12, specMap.getInt(YayaConstants.MAX_ATTEMPTS));
		assertEquals("foo", specMap.getString(YayaConstants.QUEUE_NAME));
		assertEquals(1024, specMap.getInt(YayaConstants.MEMORY));
		assertFalse(specMap.getBoolean(YayaConstants.YARN_EMULATOR));
		assertEquals("my-application", specMap.getString(YayaConstants.APPLICATION_NAME));
		
		PrimitiveImmutableTypeMap containerSpecMap = (PrimitiveImmutableTypeMap) specMap.get(YayaConstants.CONTAINER_SPEC);
		assertEquals(4, containerSpecMap.getInt(YayaConstants.CONTAINER_COUNT));
		assertEquals(3, containerSpecMap.getInt(YayaConstants.PRIORITY));
		assertEquals(28, containerSpecMap.getInt(YayaConstants.MEMORY));
		assertEquals(34, containerSpecMap.getInt(YayaConstants.VIRTUAL_CORES));
		assertEquals("ls -all", containerSpecMap.getString(YayaConstants.COMMAND));
	}
	
	@Test
	public void validateJavaBasedContainerWithDefaults() {
		
		ApplicationMasterSpecBuilder<ContainerDelegate[]> applicationMasterBuilder = YarnAssembly.forApplicationContainer(DslTestApplicationContainer.class).withApplicationMaster();
		YarnApplication<ContainerDelegate[]> yarnApplication = applicationMasterBuilder.build("my-application");
		
		PrimitiveImmutableTypeMap specMap = (PrimitiveImmutableTypeMap) yarnApplication.getApplicationSpecification();
		assertEquals(1, specMap.getInt(YayaConstants.VIRTUAL_CORES));
		assertEquals(0, specMap.getInt(YayaConstants.PRIORITY));
		assertEquals(1, specMap.getInt(YayaConstants.MAX_ATTEMPTS));
		assertEquals("default", specMap.getString(YayaConstants.QUEUE_NAME));
		assertEquals(512, specMap.getInt(YayaConstants.MEMORY));
		assertTrue(specMap.getBoolean(YayaConstants.YARN_EMULATOR));
		assertEquals("my-application", specMap.getString(YayaConstants.APPLICATION_NAME));
		
		PrimitiveImmutableTypeMap containerSpecMap = (PrimitiveImmutableTypeMap) specMap.get(YayaConstants.CONTAINER_SPEC);
		assertEquals(1, containerSpecMap.getInt(YayaConstants.CONTAINER_COUNT));
		assertEquals(0, containerSpecMap.getInt(YayaConstants.PRIORITY));
		assertEquals(256, containerSpecMap.getInt(YayaConstants.MEMORY));
		assertEquals(1, containerSpecMap.getInt(YayaConstants.VIRTUAL_CORES));
		assertEquals(DslTestApplicationContainer.class.getName(), containerSpecMap.getString(YayaConstants.CONTAINER_IMPL));
	}
	
	@Test
	public void validateJavaBasedContainerWithCustom() {
		YarnConfiguration yarnConfig = new YarnConfiguration();
		ApplicationMasterSpecBuilder<ContainerDelegate[]> applicationMasterBuilder = YarnAssembly.forApplicationContainer(DslTestApplicationContainer.class).
				memory(28).
				containerCount(4).
				virtualCores(34).
				priority(3).
				withApplicationMaster(yarnConfig);
		
		YarnApplication<ContainerDelegate[]> yarnApplication = applicationMasterBuilder.
				maxAttempts(12).
				priority(34).
				queueName("foo").
				virtualCores(35).
				memory(1024).
				build("my-application");
		
		PrimitiveImmutableTypeMap specMap = (PrimitiveImmutableTypeMap) yarnApplication.getApplicationSpecification();
		assertEquals(35, specMap.getInt(YayaConstants.VIRTUAL_CORES));
		assertEquals(34, specMap.getInt(YayaConstants.PRIORITY));
		assertEquals(12, specMap.getInt(YayaConstants.MAX_ATTEMPTS));
		assertEquals("foo", specMap.getString(YayaConstants.QUEUE_NAME));
		assertEquals(1024, specMap.getInt(YayaConstants.MEMORY));
		assertFalse(specMap.getBoolean(YayaConstants.YARN_EMULATOR));
		assertEquals("my-application", specMap.getString(YayaConstants.APPLICATION_NAME));
		
		PrimitiveImmutableTypeMap containerSpecMap = (PrimitiveImmutableTypeMap) specMap.get(YayaConstants.CONTAINER_SPEC);
		assertEquals(4, containerSpecMap.getInt(YayaConstants.CONTAINER_COUNT));
		assertEquals(3, containerSpecMap.getInt(YayaConstants.PRIORITY));
		assertEquals(28, containerSpecMap.getInt(YayaConstants.MEMORY));
		assertEquals(34, containerSpecMap.getInt(YayaConstants.VIRTUAL_CORES));
		assertNull(containerSpecMap.get(YayaConstants.CONTAINER_ARG));
		assertEquals(DslTestApplicationContainer.class.getName(), containerSpecMap.getString(YayaConstants.CONTAINER_IMPL));
	}
	
	@Test
	public void validateJavaBasedContainerWithArgumentsAndShell() {
		YarnConfiguration yarnConfig = new YarnConfiguration();
		ApplicationMasterSpecBuilder<Void> applicationMasterBuilder = YarnAssembly.forApplicationContainer(DslTestApplicationContainer.class, ByteBuffer.wrap("Hello".getBytes()), "java").
				memory(28).
				containerCount(4).
				virtualCores(34).
				priority(3).
				withApplicationMaster(yarnConfig);
		
		YarnApplication<Void> yarnApplication = applicationMasterBuilder.
				maxAttempts(12).
				priority(34).
				queueName("foo").
				virtualCores(35).
				memory(1024).
				build("my-application");
		
		PrimitiveImmutableTypeMap specMap = (PrimitiveImmutableTypeMap) yarnApplication.getApplicationSpecification();
		assertEquals(35, specMap.getInt(YayaConstants.VIRTUAL_CORES));
		assertEquals(34, specMap.getInt(YayaConstants.PRIORITY));
		assertEquals(12, specMap.getInt(YayaConstants.MAX_ATTEMPTS));
		assertEquals("foo", specMap.getString(YayaConstants.QUEUE_NAME));
		assertEquals(1024, specMap.getInt(YayaConstants.MEMORY));
		assertFalse(specMap.getBoolean(YayaConstants.YARN_EMULATOR));
		assertEquals("my-application", specMap.getString(YayaConstants.APPLICATION_NAME));

		PrimitiveImmutableTypeMap containerSpecMap = (PrimitiveImmutableTypeMap) specMap.get(YayaConstants.CONTAINER_SPEC);
		assertEquals(4, containerSpecMap.getInt(YayaConstants.CONTAINER_COUNT));
		assertEquals(3, containerSpecMap.getInt(YayaConstants.PRIORITY));
		assertEquals(28, containerSpecMap.getInt(YayaConstants.MEMORY));
		assertEquals(34, containerSpecMap.getInt(YayaConstants.VIRTUAL_CORES));
		assertNotNull(containerSpecMap.get(YayaConstants.CONTAINER_ARG));
		assertEquals(DslTestApplicationContainer.class.getName(), containerSpecMap.getString(YayaConstants.CONTAINER_IMPL));
	}
	
	@Test
	public void validateJavaBasedContainerWithShell() {
		YarnConfiguration yarnConfig = new YarnConfiguration();
		ApplicationMasterSpecBuilder<ContainerDelegate[]> applicationMasterBuilder = YarnAssembly.forApplicationContainer(DslTestApplicationContainer.class, "java").
				memory(28).
				containerCount(4).
				virtualCores(34).
				priority(3).
				withApplicationMaster(yarnConfig);
		
		YarnApplication<ContainerDelegate[]> yarnApplication = applicationMasterBuilder.
				maxAttempts(12).
				priority(34).
				queueName("foo").
				virtualCores(35).
				memory(1024).
				build("my-application");
		
		PrimitiveImmutableTypeMap specMap = (PrimitiveImmutableTypeMap) yarnApplication.getApplicationSpecification();
		assertEquals(35, specMap.getInt(YayaConstants.VIRTUAL_CORES));
		assertEquals(34, specMap.getInt(YayaConstants.PRIORITY));
		assertEquals(12, specMap.getInt(YayaConstants.MAX_ATTEMPTS));
		assertEquals("foo", specMap.getString(YayaConstants.QUEUE_NAME));
		assertEquals(1024, specMap.getInt(YayaConstants.MEMORY));
		assertFalse(specMap.getBoolean(YayaConstants.YARN_EMULATOR));
		assertEquals("my-application", specMap.getString(YayaConstants.APPLICATION_NAME));

		PrimitiveImmutableTypeMap containerSpecMap = (PrimitiveImmutableTypeMap) specMap.get(YayaConstants.CONTAINER_SPEC);
		assertEquals(4, containerSpecMap.getInt(YayaConstants.CONTAINER_COUNT));
		assertEquals(3, containerSpecMap.getInt(YayaConstants.PRIORITY));
		assertEquals(28, containerSpecMap.getInt(YayaConstants.MEMORY));
		assertEquals(34, containerSpecMap.getInt(YayaConstants.VIRTUAL_CORES));
		assertNull(containerSpecMap.get(YayaConstants.CONTAINER_ARG));
		assertEquals(DslTestApplicationContainer.class.getName(), containerSpecMap.getString(YayaConstants.CONTAINER_IMPL));
	}
	
	public static class DslTestApplicationContainer implements ApplicationContainerProcessor {

		@Override
		public ByteBuffer process(ByteBuffer inputMessage) {
			return ByteBuffer.wrap("Gladto hear from you!".getBytes());
		}
	}
}
