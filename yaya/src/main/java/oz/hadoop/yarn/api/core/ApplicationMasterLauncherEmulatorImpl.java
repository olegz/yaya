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

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Random;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.json.simple.JSONObject;

import oz.hadoop.yarn.api.YayaConstants;
import oz.hadoop.yarn.api.utils.ReflectionUtils;

/**
 * INTERNAL API
 * 
 * @author Oleg Zhurakousky
 *
 */
class ApplicationMasterLauncherEmulatorImpl<T> extends AbstractApplicationMasterLauncher<T> {

	/**
	 * 
	 * @param applicationSpecification
	 */
	ApplicationMasterLauncherEmulatorImpl(
			Map<String, Object> applicationSpecification) {
		super(applicationSpecification);
	}

	/**
	 * 
	 */
	@Override
	ApplicationId doLaunch(int launchApplicationMaster) {
		this.applicationSpecification.put(YayaConstants.APP_ID, new Random().nextInt(1000));
		String jsonArguments = JSONObject.toJSONString(this.applicationSpecification);
		final String encodedJsonArguments = new String(Base64.encodeBase64(jsonArguments.getBytes()));
		this.executor.execute(new Runnable() {	
			@Override
			public void run() {
				try {
					/*
					 * While it would be better/simpler to avoid Reflection here, the following code emulates as close as possible
					 * the way YARN invokes Application Master. So its done primarily for consistency
					 */
					Class<? extends AbstractContainer> applicationMasterLauncher = ApplicationMaster.class;
					Method mainMethod = ReflectionUtils.getMethodAndMakeAccessible(applicationMasterLauncher, "main", new Class[] {String[].class});
					mainMethod.invoke(null, (Object)new String[]{encodedJsonArguments, ApplicationMaster.class.getName()});
				} 
				catch (Exception e) {
					throw new IllegalStateException("Failed to launch Application Master: " + ApplicationMasterLauncherEmulatorImpl.this.applicationName, e);
				}
			}
		});
		return new EmulatedApplicationId();
	}
	
	/**
	 * 
	 */
	ApplicationId doShutDown() {
		return null;
	}
	
	/**
	 * 
	 */
	private class EmulatedApplicationId extends ApplicationId {

		@Override
		public int getId() {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		protected void setId(int id) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public long getClusterTimestamp() {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		protected void setClusterTimestamp(long clusterTimestamp) {
			// TODO Auto-generated method stub
			
		}

		@Override
		protected void build() {
			// TODO Auto-generated method stub
			
		}
		
	}
}
