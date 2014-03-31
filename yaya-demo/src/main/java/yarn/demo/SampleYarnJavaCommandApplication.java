package yarn.demo;

import java.util.Arrays;

import oz.hadoop.yarn.api.JavaApplicationContainer;
import oz.hadoop.yarn.api.JavaApplicationContainerSpec;
import oz.hadoop.yarn.api.PrimitiveImmutableTypeMap;
import oz.hadoop.yarn.api.YarnApplication;
import oz.hadoop.yarn.api.YarnApplicationBuilder;

/**
 *
 * @author Oleg Zhurakousky
 *
 */
public class SampleYarnJavaCommandApplication {

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

		// or create a Java command
		JavaApplicationContainerSpec applicationContainer = new JavaApplicationContainerSpec(MyJavaContainer.class);
		applicationContainer.addContainerArgument("fname", "Joe");
		applicationContainer.addContainerArgument("knownNames", Arrays.asList(new String[]{"Bob", "Greg"}));
		applicationContainer.setContainerCount(2);

		// Create YARN application
		YarnApplication yarnApplication = YarnApplicationBuilder.forApplication("sample-yarn-java-app", applicationContainer).
				setMemory(2048).
				setVirtualCores(2).
				build();

		// Start YARN application
		yarnApplication.launch();

		// Check target/LOCAL_YARN_CLUSTER directory of this project for application logs
	}

	private static class MyJavaContainer implements JavaApplicationContainer {

		@Override
		public void launch(PrimitiveImmutableTypeMap arguments) {
			System.out.println("Hello Yarn");
			System.out.println("Arguments: " + arguments);
		}

	}
}
