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

import oz.hadoop.yarn.api.YayaConstants;
import oz.hadoop.yarn.api.utils.PrimitiveImmutableTypeMap;


/**
 * INTERNAL API
 * 
 * Implementation of the generic YARN Application Master whose responsibility 
 * is to deploy YARN Application Containers defined using application specification.   
 * 
 * @author Oleg Zhurakousky
 *
 */
class ApplicationMaster extends AbstractContainer {

	private final boolean emulated;

	private final ApplicationContainerLauncher applicationContainerLauncher;
	
	/**
	 * Will construct this class using {@link PrimitiveImmutableTypeMap} which contains YARN Application specification 
	 * It is typically invoked from its supper class's main(..) method where {@link PrimitiveImmutableTypeMap}
	 * of application specification is constructed from the JSON String.
	 * 
	 * @param applicationSpecification
	 */
	public ApplicationMaster(PrimitiveImmutableTypeMap applicationSpecification) {
		super(applicationSpecification);
		this.emulated = this.applicationSpecification.getBoolean(YayaConstants.YARN_EMULATOR);
		if (emulated){
			this.applicationContainerLauncher = new ApplicationContainerLauncherEmulatorImpl(applicationSpecification, this.containerSpec);
		}
		else {
			this.applicationContainerLauncher = new ApplicationContainerLauncherImpl(applicationSpecification, this.containerSpec);
		}
	}

	/**
	 * Will launch Application Containers
	 */
	@Override
	void launch() {
		logger.info("###### Starting APPLICATION MASTER ######");
		
		// this method will block until containers are finished (on its own or terminated)
		this.applicationContainerLauncher.launch();

		logger.info("###### Stopped APPLICATION MASTER ######");
	}
}
