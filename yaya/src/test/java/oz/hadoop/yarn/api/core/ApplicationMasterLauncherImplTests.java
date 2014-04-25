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

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

import java.io.File;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.springframework.core.io.ClassPathResource;

import oz.hadoop.yarn.api.YayaConstants;
import oz.hadoop.yarn.api.utils.ReflectionUtils;

/**
 * @author Oleg Zhurakousky
 *
 */
public class ApplicationMasterLauncherImplTests {

	@Test
	public void validateDefaultClasspathExclusions() throws Exception {
		Map<String, Object> applicationSpecification = new HashMap<>();
		applicationSpecification.put(YayaConstants.CONTAINER_SPEC, new HashMap<>());
		ApplicationMasterLauncherImpl<Void> launcher = new ApplicationMasterLauncherImpl<>(applicationSpecification);
		
		Method excludedMethod = ReflectionUtils.getMethodAndMakeAccessible(ApplicationMasterLauncherImpl.class, "excluded", String.class);
		String[] cp = System.getProperty("java.class.path").split(":");
		for (String v : cp) {
			File f = new File(v);
			if (!f.isDirectory()){
				boolean excluded = (boolean) excludedMethod.invoke(launcher, f.getName());
				if (f.getName().contains("junit") || f.getName().contains("mockito") || f.getName().contains("hamcrest")){
					assertTrue(excluded);
				}
				else {
					assertFalse(f.getName(), excluded);
				}
			}
		}
	}
	
	@Test
	public void validateWithMissingClasspathFilters() throws Exception {
		ClassPathResource res = new ClassPathResource("classpath.filters");
		File cpFilterFile = res.getFile();
		File backup = new File("classpath.filters.old");
		try {
			cpFilterFile.renameTo(backup);
			Map<String, Object> applicationSpecification = new HashMap<>();
			applicationSpecification.put(YayaConstants.CONTAINER_SPEC, new HashMap<>());
			ApplicationMasterLauncherImpl<Void> launcher = new ApplicationMasterLauncherImpl<>(applicationSpecification);
			
			Method excludedMethod = ReflectionUtils.getMethodAndMakeAccessible(ApplicationMasterLauncherImpl.class, "excluded", String.class);
			String[] cp = System.getProperty("java.class.path").split(":");
			for (String v : cp) {
				File f = new File(v);
				if (!f.isDirectory()){
					boolean excluded = (boolean) excludedMethod.invoke(launcher, f.getName());
					assertFalse(f.getName(), excluded);
				}
			}
		} 
		finally {
			backup.renameTo(cpFilterFile);
		}
	}
	
	@Test
	public void validateWithEmptyClasspathFilters() throws Exception {
		ClassPathResource res = new ClassPathResource("classpath.filters");
		File cpFilterFile = res.getFile();
		File parentFile = cpFilterFile.getParentFile();
		File backup = new File("classpath.filters.old");
		try {
			cpFilterFile.renameTo(backup);
			File file = new File(parentFile, "classpath.filters");
			file.createNewFile();
			Map<String, Object> applicationSpecification = new HashMap<>();
			applicationSpecification.put(YayaConstants.CONTAINER_SPEC, new HashMap<>());
			ApplicationMasterLauncherImpl<Void> launcher = new ApplicationMasterLauncherImpl<>(applicationSpecification);
			
			Method excludedMethod = ReflectionUtils.getMethodAndMakeAccessible(ApplicationMasterLauncherImpl.class, "excluded", String.class);
			String[] cp = System.getProperty("java.class.path").split(":");
			for (String v : cp) {
				File f = new File(v);
				if (!f.isDirectory()){
					boolean excluded = (boolean) excludedMethod.invoke(launcher, f.getName());
					assertFalse(f.getName(), excluded);
				}
			}
		} 
		finally {
			cpFilterFile.delete();
			File file = new File(parentFile, "classpath.filters");
			backup.renameTo(file);
			assertTrue(file.exists());
			assertTrue(file.length() > 0);
		}
	}

}
