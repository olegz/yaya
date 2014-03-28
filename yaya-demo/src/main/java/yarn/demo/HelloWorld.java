package yarn.demo;

public class HelloWorld {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println("Hello YARN");
		if (args != null){
			for (String string : args) {
				System.out.println(string);
			}
		}
	}

}
