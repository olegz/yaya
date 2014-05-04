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

import org.springframework.util.Assert;
import org.springframework.util.FileCopyUtils;

/**
 * @author Oleg Zhurakousky
 *
 */
public class ConfigUtils {
	
	private ConfigUtils(){}

	/**
	 * 
	 * @param file
	 */
	public static void setConfig(File configurationPath){
		Assert.notNull(configurationPath, "'configurationPath' must not be null");
		Assert.isTrue(configurationPath.exists(), "'configurationPath' must exist");
		
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
	    File classpathRoot = new File(classLoader.getResource("").getPath());
	    if (configurationPath.isDirectory()){
			String[] configs = configurationPath.list();
			for (String configFileName : configs) {
				File configFile = new File(configurationPath, configFileName);
				File configFileOnClassPath = new File(classpathRoot, configFileName);
				doCopy(configFile, configFileOnClassPath);
			}
		}
    	else {
    		File configFileOnClassPath = new File(classpathRoot, configurationPath.getName());
			if (configFileOnClassPath.exists()){
				configFileOnClassPath.delete();
			}
			doCopy(configurationPath, configFileOnClassPath);
    	}
	}
	
	/**
	 * 
	 * @param from
	 * @param to
	 */
	private static void doCopy(File from, File to) {
		if (from.getName().endsWith("-site.xml")){
			try {
				if (to.exists()){
					to.delete();
				}
				FileCopyUtils.copy(from, to);
			} 
			catch (Exception e) {
				throw new IllegalStateException("Failed to copy configurations to the claspath", e);
			}
		}
	}
}
