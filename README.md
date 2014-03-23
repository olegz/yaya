#### _Try and YARN in less then 5_
This project has two main goals.

1. _To serve as a **tutorial** for those who would like to learn YARN_
2. _An attempt to define a **simpler (developer friendly) YARN API**, that the end user can use._ -  _Yet Another Yarn API_ (YAYA?)

### YARN could and should be as simple as:
```
// Create a command to be executed in the container launched by the Application Master
UnixCommand applicationCommand = new UnixCommand("cal");

// Create YARN application
YarnApplication yarnApplication = 
          YarnApplicationBuilder.forApplication("sample-yarn-app", applicationCommand).build();

// Launch YARN application
yarnApplication.launch();

```

##### [Introduction](https://github.com/olegz/yarn-tutorial/wiki/Introduction)
##### [For Developers](https://github.com/olegz/yarn-tutorial/wiki/Developers)

**_This is an evolving work in progress so more updates (code and documentation) will be coming soon_**

_Please send question and updates via pull requests and/or raising [issues](https://github.com/olegz/yarn-tutorial/issues) on this project._