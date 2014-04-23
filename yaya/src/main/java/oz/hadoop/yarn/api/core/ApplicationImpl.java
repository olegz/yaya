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

import java.util.Map;

import org.apache.hadoop.yarn.conf.YarnConfiguration;

import oz.hadoop.yarn.api.YarnApplication;
import oz.hadoop.yarn.api.YarnAssembly;
import oz.hadoop.yarn.api.YayaConstants;
import oz.hadoop.yarn.api.utils.PrimitiveImmutableTypeMap;

/**
 * INTERNAL API
 * 
 * Implementation of {@link YarnApplication} which will be backing proxy returned by the 
 * {@link YarnAssembly}'s build() method.
 * Its main responsibility is to construct the appropriate instance of {@link ApplicationMasterLauncher}
 * (Emulated or Real), then all life-cycle calls will be delegated to this instance.
 * 
 * @author Oleg Zhurakousky
 *
 */
class ApplicationImpl<T> implements YarnApplication<T> {

	private final Map<String, Object> applicationSpecification;
	
	private final ApplicationMasterLauncher<T> yarnApplicationMasterLauncher;

	/**
	 * 
	 * @param applicationSpecification
	 */
	ApplicationImpl(Map<String, Object> applicationSpecification){
		this.applicationSpecification = applicationSpecification;

		YarnConfiguration yarnConfig = (YarnConfiguration) this.applicationSpecification.get(YayaConstants.YARN_CONFIG);
		boolean emulator = yarnConfig == null;
		this.applicationSpecification.put(YayaConstants.YARN_EMULATOR, emulator);
		if (emulator){	
			this.yarnApplicationMasterLauncher = new ApplicationMasterLauncherEmulatorImpl<>(this.applicationSpecification);
		}
		else {
			this.yarnApplicationMasterLauncher = new ApplicationMasterLauncherImpl<>(this.applicationSpecification);
		}
	}
	
	/**
	 * 
	 */
	@Override
	public Map<String, Object> getApplicationSpecification() {
		return new PrimitiveImmutableTypeMap(this.applicationSpecification);
	}

	/**
	 * 
	 */
	@Override
	public boolean isRunning() {
		return this.yarnApplicationMasterLauncher.isRunning();
	}
	
	/**
	 * 
	 */
	@Override
	public int liveContainers(){
		return this.yarnApplicationMasterLauncher.liveContainers();
	}
	
	/**
	 * 
	 */
	@Override
	public T launch() {
		return this.yarnApplicationMasterLauncher.launch();
	}

	/**
	 * 
	 */
	@Override
	public void shutDown() {
		this.yarnApplicationMasterLauncher.shutDown();
	}
	
	@Override
	public void terminate() {
		this.yarnApplicationMasterLauncher.terminate();
	}
}
