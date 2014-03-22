package oz.hadoop.yarn.demo;

import oz.hadoop.yarn.api.ApplicationCommand;
import oz.hadoop.yarn.api.YarnApplication;
import oz.hadoop.yarn.api.YarnApplicationBuilder;

/**
 *
 * @author Oleg Zhurakousky
 *
 */
public class SampleYarnAppLocal {

	/**
	 * Before running ensure that properly configured yarn-site.xml are copied
	 * into src/main/resources. You can use the yarn-site.xml from local-config
	 * directory of this project. THe newly checkout out project is already
	 * setup for this.
	 *
	 * Also, make sure you start local YARN cluster by executing
	 * StartMiniCluster.java first.
	 */
	public static void main(String[] args) throws Exception {
		System.setProperty("local-cluster", "true");

		// Create a command to be executed in the container launched by the Application Master
		ApplicationCommand applicationCommand = new ApplicationCommand("cal");

		// Create YARN application
		YarnApplication yarnApplication = YarnApplicationBuilder.forApplication("sample-yarn-app", applicationCommand).build();

		// Start YARN application
		yarnApplication.start();

		// Check target/LOCAL_YARN_CLUSTER directory of this project for application logs
	}
}
