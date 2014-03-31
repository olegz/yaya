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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.json.simple.JSONObject;

import oz.hadoop.yarn.api.utils.NumberAssertUtils;
import oz.hadoop.yarn.api.utils.ObjectAssertUtils;
import oz.hadoop.yarn.api.utils.StringAssertUtils;

/**
 * Base class for implementing Application Container specification.
 * See {@link JavaApplicationContainerSpec} and {@link UnixApplicationContainerSpec}
 *
 * @author Oleg Zhurakousky
 *
 */
abstract class AbstractApplicationContainerSpec {
	public static final String COMMAND = "command";
	public static final String CONTAINER_COUNT = "containerCount";
	public static final String MEMORY = "memory";
	public static final String VIRTUAL_CORES = "virtualCores";
	public static final String PRIORITY = "priority";
	public static final String AM_SPEC = "amSpec";
	public static final String APPLICATION_NAME = "appName";
	public static final String APPLICATION_ID = "appId";
	public final static String CONTAINER_LAUNCHER = "containerLauncher";
	public final static String CONTAINER_IMPL = "containerImpl";
	public final static String CONTAINER_SPEC_ARG = "containerArguments";
	public final static String CONTAINER_TYPE = "containerType";

	public static final int containerCount = 1;

	public static final int memory = 512;

	public static final int virtualCores = 1;

	public static final int priority = 0;

	public static final Class<ApplicationMasterSpec> amSpec = ApplicationMasterSpec.class;

	protected final Map<String, Object> containerSpec;

	private final Map<String, Object> containerArguments;

	/**
	 *
	 */
	public AbstractApplicationContainerSpec(){
		this.containerSpec = new LinkedHashMap<String, Object>();
		this.containerSpec.put(CONTAINER_COUNT, containerCount);
		this.containerSpec.put(MEMORY, memory);
		this.containerSpec.put(VIRTUAL_CORES, virtualCores);
		this.containerSpec.put(PRIORITY, priority);
		this.containerSpec.put(AM_SPEC, amSpec.getName());
		this.containerArguments = new HashMap<String, Object>();
	}

	/**
	 *
	 * @param containerCount
	 */
	public void setContainerCount(int containerCount) {
		NumberAssertUtils.assertGreaterThenZero(containerCount);
		this.containerSpec.put(CONTAINER_COUNT, containerCount);
	}

	/**
	 *
	 * @param memory
	 */
	public void setMemory(int memory){
		NumberAssertUtils.assertGreaterThenZero(memory);
		this.containerSpec.put(MEMORY, memory);
	}

	/**
	 *
	 * @param virtualCores
	 */
	public void setVirtualCores(int virtualCores){
		NumberAssertUtils.assertGreaterThenZero(virtualCores);
		this.containerSpec.put(VIRTUAL_CORES, virtualCores);
	}

	/**
	 *
	 * @param priority
	 */
	public void setPriority(int priority){
		NumberAssertUtils.assertZeroOrPositive(priority);
		this.containerSpec.put(PRIORITY, priority);
	}

	/**
	 * Converts representation of this specification to JSON string.
	 *
	 * @return
	 */
	public String toJsonString() {
		this.containerSpec.put("containerArguments", this.containerArguments);
		return JSONObject.toJSONString(this.containerSpec);
	}

	/**
	 * Encodes representation of the JSON string of this specification to Base64 String.
	 * It is used while passing arguments between the containers while preserving its JSON
	 * representation.
	 *
	 * @return
	 */
	public String toBase64EncodedJsonString() {
		String jsonString = this.toJsonString();
		String encoded = new String(Base64.encodeBase64(jsonString.getBytes()));
		return encoded;
	}

	/**
	 * Adds a container argument
	 *
	 * @param key
	 * @param value
	 */
	public void addContainerArgument(String key, String value){
		StringAssertUtils.assertNotEmptyAndNoSpaces(key);
		this.containerArguments.put(key, value);
	}

	/**
	 * Adds a container argument
	 *
	 * @param key
	 * @param values
	 */
	public void addContainerArgument(String key, List<String> values){
		StringAssertUtils.assertNotEmptyAndNoSpaces(key);
		this.containerArguments.put(key, values);
	}

	/**
	 * Adds to this container specification.
	 * Used only internally. Mainly to dynamically add application name and id
	 * before launching Application Container.
	 *
	 * @param key
	 * @param value
	 */
	protected void addToContainerSpec(String key, Object value) {
		ObjectAssertUtils.assertNotNull(key);
		this.containerSpec.put(key, value);
	}

	/**
	 *
	 */
	@Override
	public String toString(){
		return this.getClass().getSimpleName() + ":" + this.toJsonString();
	}
}
