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
package oz.hadoop.yarn.api.utils;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;

import org.apache.hadoop.conf.Configuration;
import org.springframework.util.Assert;

/**
 * Utilities to manipulate {@link Configuration} object to be used for
 * bootstrapping the application. 
 * 
 * @author Oleg Zhurakousky
 * 
 */
public class ConfigUtils {

	private ConfigUtils() {
	}

	/**
	 * Will dynamically add configuration directory to the classpath.
	 * 
	 * @param configurationDirectoryPath
	 */
	public static void addToClasspath(File configurationDirectoryPath) {
		Assert.notNull(configurationDirectoryPath,"'configurationDirectoryPath' must not be null");
		Assert.isTrue(configurationDirectoryPath.exists(),"'configurationDirectoryPath' must exist");
		Assert.isTrue(configurationDirectoryPath.isDirectory(),"'configurationDirectoryPath' must be a directory");

		URL configUrl = null;
		try {
			configUrl = new URL("file:" + configurationDirectoryPath.getAbsolutePath() + "/");
		} 
		catch (Exception e) {
			throw new IllegalArgumentException("Failed to construct URL for " + configurationDirectoryPath, e);
		}
		URLClassLoader cl = (URLClassLoader) ClassLoader.getSystemClassLoader();
		Method addUrlMethod = ReflectionUtils.getMethodAndMakeAccessible(URLClassLoader.class, "addURL", URL.class);
		try {
			addUrlMethod.invoke(cl, configUrl);
		} 
		catch (Exception e) {
			throw new IllegalStateException("Failed to add URL: " + configUrl + " to the classpath", e);
		}
	}
}
