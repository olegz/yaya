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
package yarn.montecarlo;

import java.nio.ByteBuffer;
import java.text.DecimalFormat;

import org.apache.hadoop.yarn.conf.YarnConfiguration;

import oz.hadoop.yarn.api.ApplicationContainerProcessor;
import oz.hadoop.yarn.api.ContainerReplyListener;
import oz.hadoop.yarn.api.DataProcessor;
import oz.hadoop.yarn.api.FsByteBufferPersister;
import oz.hadoop.yarn.api.YarnApplication;
import oz.hadoop.yarn.api.YarnAssembly;

/**
 * @author Oleg Zhurakousky
 *
 */
public class MonteCarloSimulationDemo {

	static DecimalFormat df = new DecimalFormat("#################.00");
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		YarnApplication<DataProcessor> yarnApplication = YarnAssembly.forApplicationContainer(MonteCarloSimulationContainer.class).
					containerCount(4).
						withApplicationMaster(new YarnConfiguration()).
							build("MonteCarloSimulation");
		
		yarnApplication.registerReplyListener(new ContainerReplyListener() {	
			@Override
			public void onReply(ByteBuffer replyData) {
				byte[] metaBytes = new byte[replyData.capacity() - 24];
				replyData.rewind();
				replyData.get(metaBytes);
				System.out.println("REPLY: " + new String(metaBytes) + " - MEAN:" +  df.format(replyData.getDouble()) + "; MEDIAN:" + 
							df.format(replyData.getDouble()) + "; STDV:" + df.format(replyData.getDouble()));
			}
		});
		
		DataProcessor processor = yarnApplication.launch();
		long start = System.currentTimeMillis();
		for (int sigma = 2; sigma < 5; sigma++) {
			for (int avReturn = 12; avReturn < 14; avReturn++) {
				for (int anualInv = 5000; anualInv < 6000; anualInv++) { // B5
					ByteBuffer inputBuffer = ByteBuffer.allocate(6*4);
					inputBuffer.putInt(sigma);
					inputBuffer.putInt(avReturn);
					inputBuffer.putInt(anualInv);
					inputBuffer.putInt(30); 	// cycle
					inputBuffer.putInt(100000); // initial investment
					inputBuffer.putInt(10000);  // simulations
					inputBuffer.flip();
					processor.process(inputBuffer);
				}
			}
		}
		long stop = System.currentTimeMillis();
		yarnApplication.shutDown();
		System.out.println("Completed in " + (stop-start) + " milliseconds");
	}

	/**
	 * 
	 */
	public static class MonteCarloSimulationContainer implements ApplicationContainerProcessor {
		
		private final FsByteBufferPersister persister = new FsByteBufferPersister();

		@Override
		public ByteBuffer process(ByteBuffer input) {
			
			InvestementSimulation simulation = new InvestementSimulation(input, this.persister);
			ByteBuffer result = simulation.runSimulation();
			return result;
		}
		
	}
}
