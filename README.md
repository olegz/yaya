#### _Try and YARN in less then 5_
This project's main goal is to define a **simpler (developer friendly) YARN API**, that the end user can use, hence -  _Yet Another Yarn API_ (YAYA)

### YARN could and should be as simple as:
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

##### [Introduction](https://github.com/olegz/yarn-tutorial/wiki/Introduction)
##### [For Developers](https://github.com/olegz/yarn-tutorial/wiki/Developers)
##### [Core Features](https://github.com/olegz/yarn-tutorial/wiki/CoreFeatures)

**_This is an evolving work in progress so more updates (code and documentation) will be coming soon_**

_Please send question and updates via pull requests and/or raising [issues](https://github.com/olegz/yarn-tutorial/issues) on this project._