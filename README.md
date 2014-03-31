#### _Try and YARN in less then 5_
This project has two main goals.

1. _To serve as a **tutorial** for those who would like to learn YARN_
2. _An attempt to define a **simpler (developer friendly) YARN API**, that the end user can use._ -  _Yet Another Yarn API_ (YAYA?)

### YARN could and should be as simple as:
```
JavaApplicationContainerSpec appContainer = new JavaApplicationContainerSpec(MyJavaContainer.class);
// or create a Unix application container
UnixApplicationContainerSpec appContainer = new UnixApplicationContainerSpec("ls -all");

// Create YARN application
YarnApplication yarnApplication = 
          YarnApplicationBuilder.forApplication("sample-yarn-app", appContainer).build();

// Launch YARN application
yarnApplication.launch();
```
for Java here is a sample implementation of _MyJavaContainer_

```
private static class MyJavaContainer implements JavaApplicationContainer {
    @Override
    public void launch(PrimitiveImmutableTypeMap arguments) {
        System.out.println("Hello Yarn");
        System.out.println("Arguments: " + arguments);
    }
}
````

##### [Introduction](https://github.com/olegz/yarn-tutorial/wiki/Introduction)
##### [For Developers](https://github.com/olegz/yarn-tutorial/wiki/Developers)
##### [Core Features](https://github.com/olegz/yarn-tutorial/wiki/CoreFeatures)

**_This is an evolving work in progress so more updates (code and documentation) will be coming soon_**

_Please send question and updates via pull requests and/or raising [issues](https://github.com/olegz/yarn-tutorial/issues) on this project._