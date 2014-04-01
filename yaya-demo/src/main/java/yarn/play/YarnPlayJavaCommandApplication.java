package yarn.play;

import java.util.Enumeration;
import java.util.Iterator;
import java.util.Vector;

import oz.hadoop.yarn.api.JavaApplicationContainer;
import oz.hadoop.yarn.api.JavaApplicationContainerSpec;
import oz.hadoop.yarn.api.PrimitiveImmutableTypeMap;
import oz.hadoop.yarn.api.YarnApplication;
import oz.hadoop.yarn.api.YarnApplicationBuilder;

/**
 *
 * @author Oleg Zhurakousky, Owen Taylor
 *
 */
public class YarnPlayJavaCommandApplication {

	/**
	 * Before running ensure that properly configured yarn-site.xml are copied
	 * into src/main/resources. You can use the yarn-site.xml from local-config
	 * directory of this project. The newly checkout out project is already
	 * setup for this. Examples for remote configurations are located in
	 * remote-config directory, but you might as well use the ones from your
	 * installed cluster.
	 *
	 * Also, make sure you start local YARN cluster by executing
	 * StartMiniCluster.java first.
	 */
	public static void main(String[] args) throws Exception {

		// Create a command to be executed in the container launched by the
		// Application Master

		// create a Java command
		JavaApplicationContainerSpec appEven = new JavaApplicationContainerSpec(
				MyJavaContainer.class);
		appEven.addContainerArgument("type", "command");
		appEven.addContainerArgument("ttl", "20");
		appEven.addContainerArgument("subset", "even");
		appEven.setContainerCount(1);

		// create a second Java command
		JavaApplicationContainerSpec appOdd = new JavaApplicationContainerSpec(
				MyJavaContainer.class);
		appOdd.addContainerArgument("type", "command");
		appOdd.addContainerArgument("ttl", "20");
		appOdd.addContainerArgument("subset", "odd");
		appOdd.setContainerCount(1);

		// Create YARN application
		YarnApplication yarnApplicationEven = YarnApplicationBuilder
				.forApplication("yarn-play-java-app-even", appEven)
				.setMemory(256).setVirtualCores(2).build();

		// Create YARN application
		YarnApplication yarnApplicationOdd = YarnApplicationBuilder
				.forApplication("yarn-play-java-app-odd", appOdd)
				.setMemory(256).setVirtualCores(2).build();

		// Start YARN applications
		yarnApplicationEven.launch();
		yarnApplicationOdd.launch();

		// Check target/LOCAL_YARN_CLUSTER directory of this project for
		// application logs
	}

	private static class MyJavaContainer implements JavaApplicationContainer {

		@Override
		public void launch(PrimitiveImmutableTypeMap arguments) {
			System.out.println("Hello Yarn");
			System.out.println("Arguments: " + arguments);
			String ttl = arguments.getString("ttl");
			String subset = arguments.getString("subset");
			int x = Integer.valueOf(ttl); // this will be populated by the
											// second arg 'ttl'
			go(x, subset);
		}

		void go(int maxVal, String subset) {
			Vector<Integer> v = new Vector<Integer>();
			if (subset == "odd") {
				for (int x = 2; x < maxVal; x = x + 2) {
					v.add(x - 1);
				}
			} else {
				for (int x = 1; x < maxVal; x = x + 2) {
					v.add(x - 1);
				}
			}
			Enumeration<Integer> i = v.elements();
			while (i.hasMoreElements()) {
				System.out.print(i.nextElement());
			}
			CheckFizBuzzTask cbt = new CheckFizBuzzTask(v);
			cbt.run();
			System.out.print("\n\n");
		}

		class CheckFizBuzzTask implements Runnable {
			Vector<Integer> lv = null;

			public CheckFizBuzzTask(Vector<Integer> v) {
				lv = v;
			}

			@Override
			public void run() {
				Integer val = null;
				Iterator<Integer> i = lv.iterator();
				while (i.hasNext()) {
					val = i.next();
					String output = "" + val;
					if (val % 3 == 0) {
						output = "FIZ";
						// we have a number that is a multiple of 3
					}
					if (val % 5 == 0) {
						output = "BUZZ";
						if (val % 3 == 0)
							output = "FIZ_BUZZ";
					}
					System.out.println(output + "  ");
				}
			}
		}

	}
}
