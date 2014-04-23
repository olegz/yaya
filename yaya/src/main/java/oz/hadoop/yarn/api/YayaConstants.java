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

/**
 * Collection of constant values used by the framework.
 * 
 * @author Oleg Zhurakousky
 *
 */
public interface YayaConstants {
	
	String APPLICATION_NAME = "build";
	
	String APPLICATION_MASTER_NAME = "appMasterName";
	
	String APP_ID = "appId";
	
	String CONTAINER_COUNT = "containerCount";
	
	String MAX_ATTEMPTS = "maxAttempts";
	
	String PRIORITY = "priority";
	
	String QUEUE_NAME = "queueName";
	
	String MEMORY = "memory";
	
	String APPLICATION_COMMAND = "command";
	
	String VIRTUAL_CORES = "virtualCores";
	
	String CLIENT_HOST = "CL_HOST";
	
	String CLIENT_PORT = "CL_PORT";
	
	String YARN_EMULATOR = "true";
	
	String CONTAINER_SPEC = "CONTAINER_SPEC";
	
	String CONTAINER_TYPE = "CONTAINER_TYPE";
	
	String CONTAINER_IMPL = "CONTAINER_IMPL";
	
	String CONTAINER_LAUNCHER = "CONTAINER_LAUNCHER";
	
	String CONTAINER_ARG = "CONTAINER_ARG";
	
	String YARN_CONFIG = "YARN_CONFIG";
	
	String COMMAND = "COMMAND";
	
	String JAVA_COMMAND = "JAVA_COMMAND";
}
