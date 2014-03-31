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

import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.json.simple.parser.JSONParser;

/**
 * @author Oleg Zhurakousky
 *
 */
abstract class BaseContainerLauncher {
	protected final Log logger = LogFactory.getLog(this.getClass());

	protected final JSONParser jsonParser = new JSONParser();

	protected final YarnConfiguration yarnConfig;

	public BaseContainerLauncher(){
		this.yarnConfig = new YarnConfiguration();
	}

	/**
	 *
	 * @param jsonArguments
	 * @return
	 */
	protected PrimitiveImmutableTypeMap buildArgumentsMap(String jsonArguments){
		try {
			String decodedArguments = new String(Base64.decodeBase64(jsonArguments.getBytes()));
			@SuppressWarnings("unchecked")
			Map<String, Object> arguments = (Map<String, Object>) jsonParser.parse(decodedArguments);
			return new PrimitiveImmutableTypeMap(arguments);
		}
		catch (Exception e) {
			throw new IllegalArgumentException("Failed to parse arguments out of the json string " + jsonArguments, e);
		}
	}

	public abstract void launch(String[] args) throws Exception;

}
