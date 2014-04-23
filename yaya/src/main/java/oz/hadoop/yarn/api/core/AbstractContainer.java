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
package oz.hadoop.yarn.api.core;

import java.lang.reflect.Constructor;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.json.simple.parser.JSONParser;

import oz.hadoop.yarn.api.YayaConstants;
import oz.hadoop.yarn.api.utils.CollectionAssertUtils;
import oz.hadoop.yarn.api.utils.PrimitiveImmutableTypeMap;
import oz.hadoop.yarn.api.utils.PrintUtils;
import oz.hadoop.yarn.api.utils.ReflectionUtils;

/**
 * INTERNAL API
 * 
 * Base class for implementing container launchers.
 * 
 * @author Oleg Zhurakousky
 *
 */
abstract class AbstractContainer {

	protected final Log logger = LogFactory.getLog(this.getClass());

	protected final PrimitiveImmutableTypeMap applicationSpecification;
	
	protected final PrimitiveImmutableTypeMap containerSpec;
	
	protected final YarnConfiguration yarnConfig;

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		CollectionAssertUtils.assertSize(args, 2);
		PrimitiveImmutableTypeMap containerArguments = buildArgumentsMap(args[0]);
		Constructor<AbstractContainer> lCtr = ReflectionUtils.getInvocableConstructor(args[1], PrimitiveImmutableTypeMap.class);
		AbstractContainer containerLauncher = lCtr.newInstance(containerArguments);
		containerLauncher.launch();
	}
	
	/**
	 * 
	 * @param applicationSpecification
	 */
	@SuppressWarnings("unchecked")
	public AbstractContainer(PrimitiveImmutableTypeMap applicationSpecification){
		if (logger.isInfoEnabled()){
			logger.info("Creating " + this.getClass().getName());
		}
		
		this.yarnConfig = new YarnConfiguration();
		this.applicationSpecification = applicationSpecification;
		this.containerSpec = new PrimitiveImmutableTypeMap((Map<String, Object>) this.applicationSpecification.get(YayaConstants.CONTAINER_SPEC));
		
		if (logger.isInfoEnabled()){
			logger.info("Application Container specification: " + PrintUtils.prettyMap(this.applicationSpecification));
		}
		if (logger.isDebugEnabled()) {
			logger.debug("SYSTEM PROPERTIES:\n" + PrintUtils.prettyMap(System.getProperties()));
			logger.debug("ENVIRONMENT VARIABLES:\n" + PrintUtils.prettyMap(System.getenv()));
		}
	}
	
	/**
	 * Will launch containers defined by concrete implementation of this class
	 */
	abstract void launch();

	/**
	 * Converts Base64 encoded JSON String of container arguments to {@link PrimitiveImmutableTypeMap}
	 *
	 * @param jsonArguments
	 * @return
	 */
	@SuppressWarnings("unchecked")
	static PrimitiveImmutableTypeMap buildArgumentsMap(String base64EncodedJsonString){
		try {
			JSONParser jsonParser = new JSONParser();
			String decodedArguments = new String(Base64.decodeBase64(base64EncodedJsonString.getBytes()));
			Map<String, Object> arguments = (Map<String, Object>) jsonParser.parse(decodedArguments);
			return new PrimitiveImmutableTypeMap(arguments);
		}
		catch (Exception e) {
			throw new IllegalArgumentException("Failed to parse arguments out of the json string", e);
		}
	}
}
