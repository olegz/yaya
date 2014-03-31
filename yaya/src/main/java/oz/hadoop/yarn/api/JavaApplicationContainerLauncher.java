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

import java.lang.reflect.Method;
import java.util.Map;

import oz.hadoop.yarn.api.utils.CollectionAssertUtils;
import oz.hadoop.yarn.api.utils.ReflectionUtils;

/**
 * @author Oleg Zhurakousky
 *
 */
class JavaApplicationContainerLauncher extends BaseContainerLauncher {



	public static void main(String[] args) throws Exception {
		CollectionAssertUtils.assertSize(args, 1);
		JavaApplicationContainerLauncher launcher = new JavaApplicationContainerLauncher();
		launcher.launch(args);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void launch(String[] args) throws Exception {
		logger.info("###### Starting JAVA Application Container ######");
		if (logger.isDebugEnabled()){
			logger.debug("SYSTEM PROPERTIES:\n" + System.getProperties());
			logger.debug("ENVIRONMENT VARIABLES:\n" + System.getenv());
		}

		PrimitiveImmutableTypeMap argumentMap = this.buildArgumentsMap(args[0]);

		String containerImplStr = argumentMap.getString(AbstractApplicationContainerSpec.CONTAINER_IMPL);

		Object containerImpl = ReflectionUtils.newDefaultInstance(containerImplStr);
		Method launchMethod = ReflectionUtils.getMethodAndMakeAccessible(containerImpl.getClass(), "launch", PrimitiveImmutableTypeMap.class);

		String arguments = argumentMap.getString(AbstractApplicationContainerSpec.CONTAINER_SPEC_ARG);
		PrimitiveImmutableTypeMap containerArguments = new PrimitiveImmutableTypeMap((Map<String, Object>)jsonParser.parse(arguments));

		if (logger.isInfoEnabled()){
			logger.info("Invoking " + containerImpl.getClass().getName());
		}

		launchMethod.invoke(containerImpl, containerArguments);

		if (logger.isInfoEnabled()){
			logger.info("Done invoking " + containerImpl.getClass().getName());
		}
		logger.info("###### Stopped JAVA Application Container ######");
	}
}
