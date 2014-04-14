### YAYA - _Yet Another Yarn API_ 

**_YAYA_**	 is a set of common abstractions and an API to simplify development of applications using **YARN** 

### YARN could and should be as simple as:
_**Unix command Application Container:**_
```
YarnApplication<Void> yarnApplication = YarnAssembly.forApplicationContainer("ping -c 4 yahoo.com").
										containerCount(4).
										memory(512).
										withApplicationMaster().
													maxAttempts(2).
													priority(2).
													build("Simplest-Yarn-Application");
		
yarnApplication.launch();
```
. . . or

_**Java process Application Container:**_
```
YarnConfiguration yarnConfiguration = new YarnConfiguration();
YarnApplication<Void> yarnApplication = 
	YarnAssembly.forApplicationContainer(ReverseMessageContainer.class, ByteBuffer.wrap("Hello Yarn!".getBytes())).
						containerCount(2).
						memory(512).withApplicationMaster(yarnConfiguration).
							maxAttempts(2).
							priority(2).
							build("JavaBasedYarnApplicationDemo");
		
yarnApplication.launch();

. . .

public static class ReverseMessageContainer implements ApplicationContainer {
		@Override
		public ByteBuffer process(ByteBuffer inputMessage) {
			inputMessage.rewind();
			byte[] inputBytes = new byte[inputMessage.limit()];
			inputMessage.get(inputBytes);
			String reversedMessage = new StringBuilder(new String(inputBytes)).reverse().toString();
			System.out.println("Processing input: " + reversedMessage);
			return null;
			// You can also return ByteBuffer, but since its a single task container
			// the contents of the returned ByteBuffer will be logged.
			//return ByteBuffer.wrap(strMessage.getBytes());
		}
}
```
##### [Introduction](https://github.com/olegz/yarn-tutorial/wiki/Introduction)
##### [For Developers](https://github.com/olegz/yarn-tutorial/wiki/Developers)
##### [Core Features](https://github.com/olegz/yarn-tutorial/wiki/CoreFeatures)

**_This is an evolving work in progress so more updates (code and documentation) will be coming soon_**

_Please send question and updates via pull requests and/or raising [issues](https://github.com/olegz/yarn-tutorial/issues) on this project._