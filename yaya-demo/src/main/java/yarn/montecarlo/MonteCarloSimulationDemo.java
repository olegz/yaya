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

import java.io.File;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;

import org.apache.hadoop.yarn.conf.YarnConfiguration;

import demo.utils.MiniClusterUtils;

import oz.hadoop.yarn.api.ApplicationContainerProcessor;
import oz.hadoop.yarn.api.ContainerReplyListener;
import oz.hadoop.yarn.api.DataProcessor;
import oz.hadoop.yarn.api.FsByteBufferPersister;
import oz.hadoop.yarn.api.YarnApplication;
import oz.hadoop.yarn.api.YarnAssembly;
import oz.hadoop.yarn.api.utils.ConfigUtils;

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
		int containerCount = prepare(args);
		
		YarnApplication<DataProcessor> yarnApplication = YarnAssembly.forApplicationContainer(MonteCarloSimulationContainer.class).
					containerCount(containerCount).
					withApplicationMaster(new YarnConfiguration()).
							build("MonteCarloSimulation");
		
		yarnApplication.registerReplyListener(new ResultsPrinter());
		
		DataProcessor processor = yarnApplication.launch();
		
		long start = System.currentTimeMillis();
		for (int sigma = 5; sigma < 15; sigma++) {
			for (int avReturn = 9; avReturn < 14; avReturn++) {
				for (int anualInv = 5000; anualInv < 6000; anualInv += 100) { 
					ByteBuffer inputBuffer = ByteBuffer.allocate(6*4);
					inputBuffer.putInt(sigma);
					inputBuffer.putInt(avReturn);
					inputBuffer.putInt(anualInv);
					inputBuffer.putInt(30); 	// cycle
					inputBuffer.putInt(100000); // initial investment
					inputBuffer.putInt(100000);  // simulations
					inputBuffer.flip();
					processor.process(inputBuffer);
				}
			}
		}
		long stop = System.currentTimeMillis();
		yarnApplication.shutDown();
		System.out.println("Completed in " + (stop-start) + " milliseconds");
		
		cleanup();
	}

	/**
	 * 
	 */
	public static class MonteCarloSimulationContainer implements ApplicationContainerProcessor {
		
		private final FsByteBufferPersister persister = new FsByteBufferPersister();
		
		private final InvestementSimulation simulation;
		
		public MonteCarloSimulationContainer(){
			this.simulation = new InvestementSimulation(this.persister);
		}

		@Override
		public ByteBuffer process(ByteBuffer input) {
			//InvestementSimulation simulation = new InvestementSimulation(input, this.persister);
			ByteBuffer result = simulation.runSimulation(input);
			return result;
		}
	}
	
	/**
	 * 
	 */
	public static class ResultsPrinter implements ContainerReplyListener {
		@Override
		public void onReply(ByteBuffer replyData) {
			byte[] metaBytes = new byte[replyData.capacity() - 24];
			replyData.rewind();
			replyData.get(metaBytes);
			System.out.println("REPLY: " + new String(metaBytes) + " - MEAN:" +  df.format(replyData.getDouble()) + "; MEDIAN:" + 
						df.format(replyData.getDouble()) + "; STDV:" + df.format(replyData.getDouble()));
		}
	}
	
	/**
	 * 
	 * @param args
	 * @return
	 */
	private static int prepare(String[] args) {
		int containerCount = 2;
		String configPath = "mini-cluster-config";
		if (args != null){
			if (args.length > 0){
				containerCount = Integer.parseInt(args[0]);
			}
			if (args.length > 1){
				configPath = args[1];
			}
			else {
				MiniClusterUtils.startMiniCluster();
			}
		}
		File configLocation = new File(configPath);
		ConfigUtils.setConfig(configLocation);
		System.out.println("Will use " + containerCount + " containers and configuration from " + configLocation.getAbsolutePath());
		return containerCount;
	}
	
	/**
	 * 
	 */
	private static void cleanup() {
		MiniClusterUtils.stoptMiniCluster();
	}
}
