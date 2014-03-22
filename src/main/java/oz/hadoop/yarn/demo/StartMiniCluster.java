package oz.hadoop.yarn.demo;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 *
 * @author Oleg Zhurakousky
 *
 */
public class StartMiniCluster {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		Thread.currentThread().setContextClassLoader(new URLClassLoader(new URL[]{new File("local-config").toURI().toURL()}));
		Configuration conf = new Configuration();

//		conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class, ResourceScheduler.class);
//		conf.setClass(YarnConfiguration.RM_SCHEDULER, FairScheduler.class, ResourceScheduler.class);

		YarnConfiguration yarnConfig = new YarnConfiguration(conf);
		MiniYarnCluster yarnCluster= new MiniYarnCluster("LOCAL_YARN_CLUSTER", 1, 1, 1);
		yarnCluster.init(new YarnConfiguration(yarnConfig));
		yarnCluster.start();
	}

}
