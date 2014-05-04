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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.Assert;

import oz.hadoop.yarn.api.core.CommandProcessLauncher;

/**
 * @author Oleg Zhurakousky
 *
 */
public class MiniClusterUtils {
	private final static Log logger = LogFactory.getLog(MiniClusterUtils.class);
	
	private static CommandProcessLauncher clusterLauncher;
	
	private static ExecutorService executor;
	
	private static final Semaphore semaphore = new Semaphore(1);

	public static void startMiniCluster(){
		try {
			semaphore.acquire();
		} 
		catch (InterruptedException e) {
			throw new IllegalStateException("Acquisition of semaphore is interrupted. Exiting");
		}
		if (clusterLauncher != null){
			throw new IllegalStateException("MiniClustrer is currently running");
		}
		File file = new File("");
		Path path = Paths.get(file.getAbsolutePath());
		
		Path parentPath = path.getParent();
		
		File clusterConfiguration = new File(parentPath + "/yarn-test-cluster/src/main/resources");
		Assert.isTrue(clusterConfiguration.exists());
		ConfigUtils.setConfig(clusterConfiguration);
		
		File miniClusterExe = new File(parentPath.toString() + "/yarn-test-cluster/build/install/yarn-test-cluster/bin/yarn-test-cluster");
		System.out.println(miniClusterExe.getAbsolutePath());
		if (!miniClusterExe.exists()){
			logger.info("BUILDING MINI_CLUSTER");
			CommandProcessLauncher buildLauncher = new CommandProcessLauncher(path.toString() + "/build-mini-cluster");
			buildLauncher.launch();
		}
		Assert.isTrue(miniClusterExe.exists(), "Failed to find mini-cluster executable");
		clusterLauncher = new CommandProcessLauncher(miniClusterExe.getAbsolutePath());
		executor = Executors.newSingleThreadExecutor();
		executor.execute(new Runnable() {
			@Override
			public void run() {
				logger.info("STARTING MINI_CLUSTER");
				clusterLauncher.launch();
			}
		});
	}
	
	public static void stoptMiniCluster(){
		if (clusterLauncher != null){
			logger.info("STOPPING MINI_CLUSTER");
			clusterLauncher.finish();
			executor.shutdownNow();
			executor = null;
			clusterLauncher = null;
		}
		semaphore.release();
	}
}
