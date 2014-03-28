package yarn.demo;

import oz.hadoop.yarn.api.UnixCommand;
import oz.hadoop.yarn.api.YarnApplication;
import oz.hadoop.yarn.api.YarnApplicationBuilder;

/**
 *
 * @author Oleg Zhurakousky
 *
 */
public class SampleYarnUnixCommandApplication {

	/**
	 * Before running ensure that properly configured yarn-site.xml are copied
	 * into src/main/resources. You can use the yarn-site.xml from local-config
	 * directory of this project. The newly checkout out project is already
	 * setup for this.
	 * Examples for remote configurations are located in remote-config directory,
	 * but you might as well use the ones from your installed cluster.
	 *
	 * Also, make sure you start local YARN cluster by executing
	 * StartMiniCluster.java first.
	 */
	public static void main(String[] args) throws Exception {

		// Create a command to be executed in the container launched by the Application Master
		UnixCommand applicationCommand = new UnixCommand("cal");
		applicationCommand.setContainerCount(2);

		// Create YARN application
		YarnApplication yarnApplication = YarnApplicationBuilder.forApplication("sample-yarn-app", applicationCommand).
				setMemory(2048).
				setVirtualCores(2).
				build();

		// Start YARN application
		yarnApplication.launch();

		// Check target/LOCAL_YARN_CLUSTER directory of this project for application logs
	}
}
