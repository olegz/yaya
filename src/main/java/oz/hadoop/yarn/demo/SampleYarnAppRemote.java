package oz.hadoop.yarn.demo;

import oz.hadoop.yarn.api.ApplicationCommand;
import oz.hadoop.yarn.api.YarnApplication;
import oz.hadoop.yarn.api.YarnApplicationBuilder;

/**
 *
 * @author Oleg Zhurakousky
 *
 */
public class SampleYarnAppRemote {

	/**
	 * Before running ensure that properly configured core-site.xml and yarn-site.xml are copied into
	 * src/main/resources.
	 * You can copy those files from your Hadoop installation.
	 */
	public static void main(String[] args) throws Exception{
		// Create a command to be executed in the container launched by the Application Master
		ApplicationCommand applicationCommand = new ApplicationCommand("java -cp ./*.jar oz.hadoop.yarn.demo.HelloWorld");

		// Create YARN application
		YarnApplication yarnApplication = YarnApplicationBuilder.forApplication("sample-yarn-app", applicationCommand).build();

		// Start YARN application
		yarnApplication.launch();

		// Check <LOG_DIR>/userlogs for application logs
	}
}
