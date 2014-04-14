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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Test;

import oz.hadoop.yarn.api.YayaConstants;

/**
 * @author Oleg Zhurakousky
 *
 */
public class YayaUtilsTests {

	@Test
	public void validateJarNameGeneration(){
		String name = YayaUtils.generateJarFileName("foo");
		assertTrue(name.startsWith("foo_"));
		assertTrue(name.endsWith(".jar"));
	}

	@Test
	public void validateExecutionCommandGeneration(){
		String name = YayaUtils.generateExecutionCommand("/user/bin/java", " -cp ./foo.jar:./bar.jsr", "foo.bar.Main", "bar baz", "my-app", "_AC_");
		assertEquals("/user/bin/java -cp ./foo.jar:./bar.jsr foo.bar.Main bar baz foo.bar.Main  1><LOG_DIR>/my-app_AC__out 2><LOG_DIR>/my-app_AC__err", name);
	}

	@Test
	public void validateInJvmPrepForJava(){
		ContainerLaunchContext containerLaunchContext = Records.newRecord(ContainerLaunchContext.class);
		YayaUtils.inJvmPrep("JAVA", containerLaunchContext, "mylauncher", "bar baz");
		assertEquals("JAVA", containerLaunchContext.getEnvironment().get(YayaConstants.CONTAINER_TYPE));
		assertEquals("mylauncher", containerLaunchContext.getEnvironment().get(YayaConstants.CONTAINER_LAUNCHER));
		assertEquals("bar baz", containerLaunchContext.getEnvironment().get(YayaConstants.CONTAINER_ARG));
	}

	@Test
	public void validateClassPathFromLocalSource() throws Exception {
		LocalResource l1 = LocalResource.newInstance(ConverterUtils.getYarnUrlFromURI(new URI("file://foo.jar")), LocalResourceType.FILE, LocalResourceVisibility.APPLICATION, 1234L, 678L);
		LocalResource l2 = LocalResource.newInstance(ConverterUtils.getYarnUrlFromURI(new URI("file://bar.jar")), LocalResourceType.FILE, LocalResourceVisibility.APPLICATION, 1234L, 678L);
		Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
		localResources.put("foo.jar", l1);
		localResources.put("bar.jar", l2);
		assertEquals("./bar.jar:./foo.jar:", YayaUtils.calculateClassPath(localResources));
	}
}
