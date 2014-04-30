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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.StringUtils;
/**
 *
 * @author Oleg Zhurakousky
 *
 */
public class JarUtils {
	private final static Log logger = LogFactory.getLog(JarUtils.class);
	/**
	 * Will create a JAR file frombase dir
	 *
	 * @param source
	 * @param jarName
	 * @return
	 */
	public static File toJar(File source, String jarName) {
		if (!source.isAbsolute()) {
			throw new IllegalArgumentException("Source must be expressed through absolute path");
		}
		StringAssertUtils.assertNotEmptyAndNoSpacesAndEndsWith(jarName, ".jar");
		Manifest manifest = new Manifest();
		manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
		File jarFile = new File(jarName);
		try {
			JarOutputStream target = new JarOutputStream(new FileOutputStream(jarFile), manifest);
			add(source, source.getAbsolutePath().length(), target);
			target.close();
		}
		catch (Exception e) {
			throw new IllegalStateException("Failed to create JAR file '"
					+ jarName + "' from " + source.getAbsolutePath(), e);
		}
		return jarFile;
	}

	/**
	 *
	 * @param source
	 * @param lengthOfOriginalPath
	 * @param target
	 * @throws IOException
	 */
	private static void add(File source, int lengthOfOriginalPath, JarOutputStream target) throws IOException {
		BufferedInputStream in = null;
		try {
			String path = source.getAbsolutePath();
			path = path.substring(lengthOfOriginalPath);

			if (source.isDirectory()) {
				String name = path.replace("\\", "/");
				if (!name.isEmpty()) {
					if (!name.endsWith("/")) {
						name += "/";
					}
					JarEntry entry = new JarEntry(name.substring(1)); // avoiding absolute path warning
					target.putNextEntry(entry);
					target.closeEntry();
				}

				for (File nestedFile : source.listFiles()) {
					add(nestedFile, lengthOfOriginalPath, target);
				}

				return;
			}

			JarEntry entry = new JarEntry(path.replace("\\", "/").substring(1)); // avoiding absolute path warning
			entry.setTime(source.lastModified());
			try {
				target.putNextEntry(entry);
				in = new BufferedInputStream(new FileInputStream(source));

				byte[] buffer = new byte[1024];
				while (true) {
					int count = in.read(buffer);
					if (count == -1) {
						break;
					}
					target.write(buffer, 0, count);
				}
				target.closeEntry();
			} 
			catch (Exception e) {
				String message = e.getMessage();
				if (StringUtils.hasText(message)){
					if (!message.toLowerCase().contains("duplicate")){
						throw new IllegalStateException(e);
					}
					logger.warn(message);
				}
				else {
					throw new IllegalStateException(e);
				}
			}
		}
		finally {
			if (in != null)
				in.close();
		}
	}
}
