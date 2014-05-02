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

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @author Oleg Zhurakousky
 *
 */
public class FsByteBufferPersister {
	
	private final static Log logger = LogFactory.getLog(FsByteBufferPersister.class);
	
	private final Map<String, WritableByteChannel> outputChannels;
	
	private final FileSystem fileSystem;

	/**
	 * 
	 * @param fileNamingStrategy
	 */
	public FsByteBufferPersister(){
		this.outputChannels = new HashMap<String, WritableByteChannel>();
		Configuration configuration = new Configuration();
		try {
			this.fileSystem = FileSystem.get(configuration);
		} 
		catch (Exception e) {
			throw new IllegalStateException("Failed to create an instance of FsPersistingApplicationContainerProcessor", e);
		}
		if (logger.isDebugEnabled()){
			logger.debug("Successfully created FsPersistingApplicationContainerProcessor");
		}
	}
	
	/**
	 * 
	 * @param dataBuffer
	 */
	public void persist(String dataIdentifier, ByteBuffer dataBuffer) {
		WritableByteChannel outputChannel = this.outputChannels.get(dataIdentifier);
		if (outputChannel == null){
			String fileName = this.generateFileName(dataIdentifier);
			try {
				OutputStream os = this.fileSystem.create(new Path(fileName), true);
				outputChannel = Channels.newChannel(os);
				this.outputChannels.put(dataIdentifier, outputChannel);
			} 
			catch (Exception e) {
				throw new IllegalStateException("Failed to create FSDataOutputStream with fileIdentifier '" + dataIdentifier + "'", e);
			}
		}
		dataBuffer.rewind();
		try {
			outputChannel.write(dataBuffer);
		} 
		catch (Exception e) {
			throw new IllegalStateException("Failed to write data to channel", e);
		}
	}
	
	/**
	 * 
	 * @param fileIdentifier
	 * @return
	 */
	private String generateFileName(String fileIdentifier){
		UUID uuid = UUID.randomUUID();
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
		String fileName = fileIdentifier + "_" + df.format(new Date()) + "_" + uuid.toString().substring(24);
		return fileName;
	}
	
}
