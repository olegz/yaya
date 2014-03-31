package oz.hadoop.yarn.api;

public class Base64 {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String s = "{\"containerCount\":2,\"memory\":512,\"virtualCores\":1,\"priority\":0,\"amSpec\":\"oz.hadoop.yarn.api.ApplicationMasterSpec\",\"containerLauncher\":\"oz.hadoop.yarn.api.JavaApplicationContainerLauncher\",\"containerImpl\":\"yarn.demo.SampleYarnJavaCommandApplication$MyJavaContainer\",\"javaExe\":\"java\",\"containerArguments\":{\"knownNames\":[\"Bob\",\"Greg\"],\"fname\":\"Joe\"},\"appName\":\"sample-yarn-java-app\",\"appId\":1,\"containerType\":\"JAVA\"}";
		String base64 = new String(org.apache.commons.codec.binary.Base64.encodeBase64(s.getBytes()));
		System.out.println(base64);

		String decoded = new String(org.apache.commons.codec.binary.Base64.decodeBase64(base64.getBytes()));
		System.out.println(decoded);
	}

}
