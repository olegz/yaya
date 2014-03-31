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

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.junit.Test;
import org.mockito.Mockito;

import oz.hadoop.yarn.api.utils.ReflectionUtils;

/**
 * @author Oleg Zhurakousky
 *
 */
public class YarnApplicationBuilderTests {

	@Test
	public void test() throws Exception {

		UnixApplicationContainerSpec unixContainer = new UnixApplicationContainerSpec("echo oleg");

		YarnApplicationBuilder applicationBuilder = YarnApplicationBuilder.forApplication("foo", unixContainer);
		YarnApplication yarnApplication = applicationBuilder.build();


		YarnClientApplication yarnClientApplication = this.buildYarnClientApplicationForTest();

		//ApplicationSubmissionContext ctx = new AppSub
		Method m = ReflectionUtils.getMethodAndMakeAccessible(yarnApplication.getClass(), "initApplicationContext", YarnClientApplication.class);
		m.invoke(yarnApplication, yarnClientApplication);

		System.out.println(m);
	}

	private YarnClientApplication buildYarnClientApplicationForTest(){
		YarnClientApplication yarnClientApplication = Mockito.mock(YarnClientApplication.class);
		ApplicationSubmissionContext appContext = Mockito.mock(ApplicationSubmissionContext.class);
		ApplicationId appId = new ApplicationIdPBImpl();

		Mockito.when(yarnClientApplication.getApplicationSubmissionContext()).thenReturn(appContext);
		Mockito.when(appContext.getApplicationId()).thenReturn(appId);
		return yarnClientApplication;
	}
}
