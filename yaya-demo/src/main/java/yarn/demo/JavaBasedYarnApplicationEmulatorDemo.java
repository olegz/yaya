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
package yarn.demo;

import java.nio.ByteBuffer;

import oz.hadoop.yarn.api.ApplicationContainerProcessor;
import oz.hadoop.yarn.api.YarnApplication;
import oz.hadoop.yarn.api.YarnAssembly;

/**
 * Demo of Application Container(s) implemented as Java process and runs in 
 * YARN Emulator
 * 
 * There is an identical demo that runs in YARN Cluster. Please see 
 * JavaBasedYarnApplicationClusterDemo.java in this package.
 * 
 * @author Oleg Zhurakousky
 *
 */
public class JavaBasedYarnApplicationEmulatorDemo {

	/**
	 * This demo will run in the YARN Emulator and requires no preparation. 
	 * Just hit run and see your output in the console.
	 * 
	 */
	public static void main(String[] args) throws Exception {
		YarnApplication<Void> yarnApplication = YarnAssembly.forApplicationContainer(ReverseMessageContainer.class, ByteBuffer.wrap("Hello Yarn!".getBytes())).
								containerCount(4).
								withApplicationMaster().
									build("JavaBasedYarnApplicationDemo");
		
		yarnApplication.launch();
	}
	
	/**
	 * As name suggests this ApplicationContainerProcessor will reverse the input message printing it to 
	 * the logs.
	 */
	public static class ReverseMessageContainer implements ApplicationContainerProcessor {
		@Override
		public ByteBuffer process(ByteBuffer inputMessage) {
			inputMessage.rewind();
			byte[] inputBytes = new byte[inputMessage.limit()];
			inputMessage.get(inputBytes);
			String strMessage = new String(inputBytes);
			strMessage = new StringBuilder(strMessage).reverse().toString();
			System.out.println("Processing input: " + strMessage);
			return ByteBuffer.wrap(strMessage.getBytes());
		}
	}
	
}
