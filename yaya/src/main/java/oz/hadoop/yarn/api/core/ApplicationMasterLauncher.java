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

import oz.hadoop.yarn.api.ApplicationContainerProcessor;
import oz.hadoop.yarn.api.ContainerReplyListener;



/**
 * INTERNAL API
 * 
 * @author Oleg Zhurakousky
 *
 */
interface ApplicationMasterLauncher<T> {

	/**
	 * 
	 * @return
	 */
	T launch();
	
	/**
	 * 
	 */
	void shutDown();
	
	/**
	 * 
	 */
	void terminate();
	
	
	/**
	 * 
	 * @return
	 */
	boolean isRunning();
	
	/**
	 * 
	 * @return
	 */
	int liveContainers();
	
	/**
	 * Allow for the registration of the {@link ContainerReplyListener} for the cases where
	 * you need to deal with replies produced by the {@link ApplicationContainerProcessor}s or commands executed 
	 * by Application Containers.
	 *
	 * @param replyListener
	 */
	void registerReplyListener(ContainerReplyListener replyListener);
}
