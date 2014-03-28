### YAYA - _Yet Another Yarn API_ 

**_YAYA_**	 is a set of common abstractions and an API to simplify development of applications using **YARN** 

### YARN could and should be as simple as:
```
// Create a command to be executed in the container launched by the Application Master
UnixCommand applicationCommand = new UnixCommand("cal");
// or create a Java command
// JavaCommand applicationCommand = new JavaCommand(HelloWorld.class);

// Create YARN application
YarnApplication yarnApplication = 
          YarnApplicationBuilder.forApplication("sample-yarn-app", applicationCommand).build();

// Launch YARN application
yarnApplication.launch();

```

##### [Introduction](https://github.com/olegz/yarn-tutorial/wiki/Introduction)
##### [For Developers](https://github.com/olegz/yarn-tutorial/wiki/Developers)
##### [Core Features](https://github.com/olegz/yarn-tutorial/wiki/CoreFeatures)

**_This is an evolving work in progress so more updates (code and documentation) will be coming soon_**

_Please send question and updates via pull requests and/or raising [issues](https://github.com/olegz/yarn-tutorial/issues) on this project._