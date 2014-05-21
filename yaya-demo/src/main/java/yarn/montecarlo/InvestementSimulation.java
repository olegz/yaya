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
import java.util.Random;

import oz.hadoop.yarn.api.FsByteBufferPersister;

/**
 * @author Oleg Zhurakousky
 *
 */
public class InvestementSimulation  {
	
	private final Random random = new Random();

	private volatile ByteBuffer simulationResults;
	
	private volatile  ByteBuffer cycleResults;
	
	private final FsByteBufferPersister persister;

	/**
	 * 
	 * @param input
	 */
	public InvestementSimulation(FsByteBufferPersister persister){
		this.persister = persister;
	}

	/**
	 * 
	 * @return
	 */
	public ByteBuffer runSimulation(ByteBuffer input) {
		int sigma = input.getInt(0);
		int avReturn = input.getInt(4);
		int anualInvestement = input.getInt(8);
		int cycle = input.getInt(12);
		int initialInvestment = input.getInt(16);
		int simulations = input.getInt(20);
		if (simulationResults == null){
			this.simulationResults = ByteBuffer.allocate(simulations * 8);
			System.out.println("CREATED");
		}
		if (cycleResults == null){
			this.cycleResults = ByteBuffer.allocate(cycle * 4 * simulations);
			System.out.println("CREATED");
		}
		this.simulationResults.clear();
		this.cycleResults.clear();
		
		for (int i = 0; i < simulations; i++) {
			double anualIncrease = initialInvestment; // B1 init inv
			double normInv = MathUtils.compute(random.nextDouble(), avReturn, sigma)/100;
			int couneter = 0;
			do {
				anualIncrease = (float) (anualIncrease * (1 + normInv) + anualInvestement);
				cycleResults.putFloat(initialInvestment);
			} while (++couneter < cycle); // B4 duration
			simulationResults.putDouble(anualIncrease);
		}
		simulationResults.flip();
		double mean = MathUtils.getMean(simulationResults);
		double median = MathUtils.getMedian(simulationResults);
		double stddev = MathUtils.getStdDev(simulationResults);
		ByteBuffer results = ByteBuffer.allocate(3 * 8);
		results.putDouble(mean);
		results.putDouble(median);
		results.putDouble(stddev);
		
		simulationResults.clear();
		cycleResults.flip();
		if (this.persister != null){
			// write cycle buffer to HDFS
			this.persister.persist(this.getClass().getSimpleName(), cycleResults);
		}
		cycleResults.clear();
		results.flip();
		return results;
	}
}
