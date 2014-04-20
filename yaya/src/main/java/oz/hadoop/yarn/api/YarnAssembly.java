/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package oz.hadoop.yarn.api;

import java.beans.Introspector;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import oz.hadoop.yarn.api.net.ContainerDelegate;
import oz.hadoop.yarn.api.utils.PrimitiveImmutableTypeMap;
import oz.hadoop.yarn.api.utils.ReflectionUtils;
import oz.hadoop.yarn.api.utils.StringAssertUtils;

/**
 * This class exposes an assembly DSL to build YARN Applications.
 * It allows for setting of both Application Master and Application Container settings 
 * while also providing a default settings.
 * <br>
 * Default setting for Application Master:<br> 
 * maxAttempts = 1<br>
 * priority = 0<br>
 * queueName = "default"<br>
 * memory = 512Mb<br>
 * virtualCores = 1<br>
 * 
 * Default setting for Application Container:<br> 
 * priority = 0<br>
 * containerCount = 1<br>
 * memory = 256Mb<br>
 * virtualCores = 1<br>
 * 
 * @author Oleg Zhurakousky
 *
 */
public final class YarnAssembly {
	
	private final static Log logger = LogFactory.getLog(YarnAssembly.class);
	
	private static String applicationImplName = "oz.hadoop.yarn.api.core.ApplicationImpl";
	
	/**
	 * Factory method which allows one to define specification for Command-based (e.g., unix, perl etc)  Yarn Application 
	 * which executes as a task implemented by a provided {@link ApplicationContainerProcessor} class using input arguments 
	 * as {@link ByteBuffer} and exiting upon completion.
	 * <br>
	 * Semantically this factory method is quite different then the factory method which takes {@link ApplicationContainerProcessor} without
	 * any input arguments, resulting in managed and interactable Application Containers. See its javadoc for more explanation. 
	 */
	public static WithVcPrMemCount<Void> forApplicationContainer(String command) {
		Assert.hasText(command, "'command' must not be null or empty");
		return createV(command, null, null, null);
	}
	
	/**
	 * Factory method which allows one to define specification for Java-based Yarn Application which executes as a task implemented by a 
	 * provided {@link ApplicationContainerProcessor} class using input arguments as {@link ByteBuffer} and exiting upon completion.
	 * <br>
	 * Semantically this factory method is quite different then the factory method which takes {@link ApplicationContainerProcessor} without
	 * any input arguments, resulting in managed and interactable Application Containers. See its javadoc for more explanation. 
	 *   
	 */
	public static WithVcPrMemCount<Void> forApplicationContainer(Class<? extends ApplicationContainerProcessor> applicationContainer, ByteBuffer arguments) {
		Assert.notNull(applicationContainer, "'applicationContainer' must not be null");
		Assert.notNull(arguments, "'arguments' must not be null");
		return createV(null, applicationContainer, arguments, null);
	}
	
	/**
	 * Factory method which allows one to define specification for Java-based Yarn Application which executes as a task implemented by a 
	 * provided {@link ApplicationContainerProcessor} class using input arguments as {@link ByteBuffer} and exiting upon completion.
	 * <br>
	 * Semantically this factory method is quite different then the factory method which takes {@link ApplicationContainerProcessor} without
	 * any input arguments, resulting in managed and interactable Application Containers. See its javadoc for more explanation. 
	 * <br>
	 * This factory method also allows you to provide a path to java shell (default: 'java'). It can be useful when you may want to 
	 * try execution using different JVM.   
	 */
	public static WithVcPrMemCount<Void> forApplicationContainer(Class<? extends ApplicationContainerProcessor> applicationContainer, ByteBuffer arguments, String javaShellPath) {
		Assert.notNull(applicationContainer, "'applicationContainer' must not be null");
		Assert.notNull(arguments, "'arguments' must not be null");
		StringAssertUtils.assertNotEmptyAndNoSpaces(javaShellPath);
		return createV(null, applicationContainer, arguments, javaShellPath);
	}
	
	/**
	 * Factory method which allows one to define specification for a managed and interactable Yarn Applications. 
	 * Such containers are long lived and could be interacted with by exchanging messages as {@link ByteBuffer}s.
	 * For more details on Application Container interaction please see {@link ContainerDelegate} and {@link YarnApplication} javadocs.
	 * <br>
	 * Semantically this factory method is quite different then the factory methods which take {@link ApplicationContainerProcessor} 
	 * and {@link ByteBuffer} arguments, creating short-lived Application Containers. See its javadoc for more explanation.  
	 * <br>
	 * This factory method also allows you to provide a path to java shell (default: 'java'). It can be useful when you may want to 
	 * try execution using different JVM.   
	 *
	 */
	public static WithVcPrMemCount<ContainerDelegate[]> forApplicationContainer(Class<? extends ApplicationContainerProcessor> applicationContainer, String javaShellPath) {
		Assert.notNull(applicationContainer, "'applicationContainer' must not be null");
		StringAssertUtils.assertNotEmptyAndNoSpaces(javaShellPath);
		return createC(null, applicationContainer, null, javaShellPath);
	}
	
	/**
	 * Allows one to define specification for a managed and interactable Application Containers. 
	 * Such containers are long lived and could be interacted with by exchanging messages as {@link ByteBuffer}s.
	 * For more details on Application Container interaction please see {@link ContainerDelegate} and {@link YarnApplication} javadocs.
	 * <br>
	 * Semantically this factory method is quite different then the factory methods which take {@link ApplicationContainerProcessor} 
	 * and {@link ByteBuffer} arguments, creating short-lived Application Containers. See its javadoc for more explanation.   
	 *
	 */
	public static WithVcPrMemCount<ContainerDelegate[]> forApplicationContainer(Class<? extends ApplicationContainerProcessor> applicationContainer) {
		Assert.notNull(applicationContainer, "'applicationContainer' must not be null");
		return createC(null, applicationContainer, null, null);
	}
	
	/**
	 * 
	 */
	@SuppressWarnings("unchecked")
	private static WithVcPrMemCount<Void> createV(String command, Class<? extends ApplicationContainerProcessor> applicationContainer, ByteBuffer arguments, String javaShellPath) {
		ProxyFactory pf = new ProxyFactory();
		pf.setInterfaces(WithVcPrMemCount.class);
		AssemblyAdvice assemblyAdvice = new AssemblyAdvice(command, applicationContainer, arguments, javaShellPath);
		pf.addAdvice(assemblyAdvice);
		WithVcPrMemCount<Void> builder = (WithVcPrMemCount<Void>) pf.getProxy();
		return builder;
	}
	
	/**
	 * 
	 */
	@SuppressWarnings("unchecked")
	private static WithVcPrMemCount<ContainerDelegate[]> createC(String command, Class<? extends ApplicationContainerProcessor> applicationContainer, ByteBuffer arguments, String javaShellPath) {
		ProxyFactory pf = new ProxyFactory();
		pf.setInterfaces(WithVcPrMemCount.class);
		AssemblyAdvice assemblyAdvice = new AssemblyAdvice(command, applicationContainer, arguments, javaShellPath);
		pf.addAdvice(assemblyAdvice);
		WithVcPrMemCount<ContainerDelegate[]> builder = (WithVcPrMemCount<ContainerDelegate[]>) pf.getProxy();
		return builder;
	}
	
	/**
	 * 
	 */
	private static class AssemblyAdvice implements MethodInterceptor {
		
		private Map<String, Object> specMap = new HashMap<>();
		
		/**
		 * 
		 */
		AssemblyAdvice(String command, Class<? extends ApplicationContainerProcessor> applicationContainer, ByteBuffer arguments, String javaShellPath){
			if (StringUtils.hasText(command)){
				this.specMap.put(YayaConstants.COMMAND, command);
			}
			if (applicationContainer != null){
				this.specMap.put(YayaConstants.CONTAINER_IMPL, applicationContainer.getName());
			}
			if (arguments != null){
				arguments.rewind();
				byte[] bytes = new byte[arguments.limit()];
				arguments.get(bytes);
				String encodedArguments = Base64.encodeBase64String(bytes);
				this.specMap.put(YayaConstants.CONTAINER_ARG, encodedArguments);
			}
			if (StringUtils.hasText(javaShellPath)){
				this.specMap.put(YayaConstants.JAVA_COMMAND, javaShellPath);
			}
			this.initializeContainerDefaults();
		}
		
		/**
		 * 
		 */
		@Override
		public Object invoke(MethodInvocation invocation) throws Throwable {
			Object[] arguments = invocation.getArguments();
			Method method = invocation.getMethod();
			String methodName = method.getName();
			Class<?> returnType = method.getReturnType();
			if (logger.isDebugEnabled()){
				logger.debug("Invoking builder method: " + method);
				logger.debug("Arguments: " + Arrays.asList(arguments));
			}
			
			ProxyFactory pf = new ProxyFactory();
			String keyName = Introspector.decapitalize(method.getName());
			
			if (method.getName().equals("withApplicationMaster")){
				Map<String, Object> masterSpec = new HashMap<>();
				masterSpec.put(YayaConstants.CONTAINER_SPEC, new PrimitiveImmutableTypeMap(this.specMap));
				this.specMap = masterSpec;
				this.initializeMasterDefaults();
				if (arguments.length == 1){
					if (arguments[0] == null){
						logger.warn("Passing YarnConfiguration as 'null' which is OK since the application will run in YARN Emulator");
					}
					this.specMap.put(YayaConstants.YARN_CONFIG, arguments[0]);
				}
				else {
					logger.warn("Missing YarnConfiguration which is OK since the application will run in YARN Emulator");
				}
				pf.setInterfaces(returnType);
				pf.addAdvice(this);
			}
			else if (method.getName().equals("build")){
				Assert.hasText((String) arguments[0], "Argument for method '" + method + "' must not be null or empty");
				this.specMap.put(YayaConstants.APPLICATION_NAME, arguments[0]);
				Constructor<?> invocableConstructor = ReflectionUtils.getInvocableConstructor(applicationImplName, Map.class);
				Object target = invocableConstructor.newInstance(this.specMap);
				pf.setTarget(target);
				pf.setInterfaces(returnType);
			}
			else {	
				Assert.isTrue(arguments.length == 1, "Wrong number of arguments. Expected 1, but was " + arguments.length);
				Assert.notNull(arguments[0], "Argument for method '" + method + "' must not be null");
				if (method.getParameterTypes()[0].isAssignableFrom(String.class)) {
					if (methodName.equals("queueName")) {
						String value = (String)arguments[0];
						StringAssertUtils.assertNotEmptyAndNoSpaces(value);
					}
					else {
						Assert.hasText((String) arguments[0], "Argument for method '" + method + "' must not be null or empty");
					}
				}
				else if (methodName.equals("virtualCores") ||
						 methodName.equals("containerCount") ||
						 methodName.equals("memory") ||
						 methodName.equals("maxAttempts")){
					int value = ((Integer)arguments[0]).intValue();
					Assert.isTrue(value > 0, "Value for argument in " + methodName + " must be > 0, was " + value);
				}
				this.specMap.put(keyName, arguments[0]);
				pf.setInterfaces(returnType);
				pf.addAdvice(this);
			}
			return pf.getProxy();
		}
		/**
		 * 
		 */
		private void initializeContainerDefaults(){
			this.specMap.put(YayaConstants.PRIORITY, 0);
			this.specMap.put(YayaConstants.CONTAINER_COUNT, 1);
			this.specMap.put(YayaConstants.MEMORY, 256);
			this.specMap.put(YayaConstants.VIRTUAL_CORES, 1);
			this.specMap.put(YayaConstants.JAVA_COMMAND, "java");
		}
		
		/**
		 * 
		 */
		private void initializeMasterDefaults(){
			this.specMap.put(YayaConstants.MAX_ATTEMPTS, 1);
			this.specMap.put(YayaConstants.PRIORITY, 0);
			this.specMap.put(YayaConstants.QUEUE_NAME, "default");
			this.specMap.put(YayaConstants.MEMORY, 512);
			this.specMap.put(YayaConstants.VIRTUAL_CORES, 1);
		}
	}

	// ============== Application Container BUILDER STRATEGIES ================== //
	
	public interface WithVcPrMemCount<T> extends ApplicationBuilderBuildable<T> {
		WithVcMemCount<T> priority(int priority);
		WithPrMemCount<T> virtualCores(int virtualCores);
		WithVcPrCount<T> memory(int memory);
		WithVcPrMem<T> containerCount(int containerCount);
	}
	
	public interface WithVcMemCount<T> extends ApplicationBuilderBuildable<T> {
		WithMemCount<T> virtualCores(int virtualCores);
		WithVcCount<T> memory(int memory);
		WithVcMem<T> containerCount(int containerCount);
	}
	
	public interface WithPrMemCount<T> extends ApplicationBuilderBuildable<T> {
		WithMemCount<T> priority(int priority);
		WithPrCount<T> memory(int memory);
		WithPrMem<T> containerCount(int containerCount);
	}
	
	public interface WithVcPrCount<T> extends ApplicationBuilderBuildable<T> {
		WithPrCount<T> virtualCores(int virtualCores);
		WithVcCount<T> priority(int priority);
		WithVcPr<T> containerCount(int containerCount);
	}
	
	public interface WithVcPrMem<T> extends ApplicationBuilderBuildable<T> {
		WithPrMem<T> virtualCores(int virtualCores);
		WithVcMem<T> priority(int priority);
		WithVcPr<T> memory(int memory);
	}
	
	public interface WithMemCount<T> extends ApplicationBuilderBuildable<T> {
		WithCount<T> memory(int memory);
		WithMem<T> containerCount(int containerCount);
	}

	public interface WithVcCount<T> extends ApplicationBuilderBuildable<T> {
		WithCount<T> virtualCores(int virtualCores);
		WithVc<T> containerCount(int containerCount);
	}
	
	public interface WithVcMem<T> extends ApplicationBuilderBuildable<T> {
		WithMem<T> virtualCores(int virtualCores);
		WithVc<T> memory(int memory);
	}
	
	public interface WithPrCount<T> extends ApplicationBuilderBuildable<T> {
		WithCount<T> priority(int priority);
		WithPr<T> containerCount(int containerCount);
	}
	
	public interface WithPrMem<T> extends ApplicationBuilderBuildable<T> {
		WithMem<T> priority(int priority);
		WithPr<T> memory(int memory);
	}
	
	public interface WithVcPr<T> extends ApplicationBuilderBuildable<T> {
		WithPr<T> virtualCores(int virtualCores);
		WithVc<T> priority(int priority);
	}
	
	public interface WithCount<T> extends ApplicationBuilderBuildable<T> {
		ApplicationBuilderBuildable<T> containerCount(int containerCount);
	}
	
	public interface WithMem<T> extends ApplicationBuilderBuildable<T> {
		ApplicationBuilderBuildable<T> memory(int memory);
	}
	
	public interface WithVc<T> extends ApplicationBuilderBuildable<T> {
		ApplicationBuilderBuildable<T> virtualCores(int virtualCores);
	}
	
	public interface WithPr<T> extends ApplicationBuilderBuildable<T> {
		ApplicationBuilderBuildable<T> priority(int priority);
	}
	
	public interface ApplicationBuilderBuildable<T> {
		/**
		 * Will return {@link ApplicationMasterSpecBuilder} to configure 
		 * Application Master
		 */
		ApplicationMasterSpecBuilder<T> withApplicationMaster();
		/**
		 * Will return {@link ApplicationMasterSpecBuilder} to configure
		 * Application Master to be launched in the YARN Cluster identified
		 * by {@link YarnConfiguration}.
		 */
		ApplicationMasterSpecBuilder<T> withApplicationMaster(YarnConfiguration yarnConfig);
	}
	
	// ============== Application Master BUILDER STRATEGIES ================== //

	public interface MWithVcMaPrMem<T> extends ApplicationMasterBuildable<T> {
		MWithMaPrMem<T> virtualCores(int virtualCores);
		MWithVcPrMem<T> maxAttempts(int maxAttempts);
		MWithVcMaMem<T> priority(int priority);
		MWithVcMaPr<T> memory(int memory);
	}

	public interface MWithQueueVcMaMem<T> extends ApplicationMasterBuildable<T>{
		MWithVcMaMem<T> queueName(String queueName);
		MWithQueueMaMem<T> virtualCores(int virtualCores);
		MWithQueueVcMem<T> maxAttempts(int maxAttempts);
		MWithQueueVcMa<T> memory(int memory);
	}

	public interface MWithQueueVcPrMem<T> extends ApplicationMasterBuildable<T> {
		MWithVcPrMem<T> queueName(String queueName);
		MWithQueuePrMem<T> virtualCores(int virtualCores);
		MWithQueueVcMem<T> priority(int priority);
		MWithQueueVcPr<T> memory(int memory);
	}

	public interface MWithQueueMaPrMem<T> extends ApplicationMasterBuildable<T> {
		MWithMaPrMem<T> queueName(String queueName);
		MWithQueuePrMem<T> maxAttempts(int maxAttempts);
		MWithQueueMaMem<T> priority(int priority);
		MWithQueueMaPr<T> memory(int memory);
	}
	
	public interface MWithQueueVcMaPr<T> extends ApplicationMasterBuildable<T> {
		MWithVcMaPr<T> queueName(String queueName);
		MWithQueueMaPr<T> virtualCores(int virtualCores);
		MWithQueueVcPr<T> maxAttempts(int maxAttempts);
		MWithQueueVcMa<T> priority(int priority);
	}
	
	public interface MWithMaPrMem<T> extends ApplicationMasterBuildable<T> {
		MWithPrMem<T> maxAttempts(int maxAttempts);
		MWithMaMem<T> priority(int priority);
		MWithMaPr<T> memory(int memory);
	}
	
	public interface MWithVcPrMem<T> extends ApplicationMasterBuildable<T>{
		MWithPrMem<T> virtualCores(int virtualCores);
		MWithVcMem<T> priority(int priority);
		MWithVcPr<T> memory(int memory);
	}
	
	public interface MWithVcMaMem<T> extends ApplicationMasterBuildable<T> {
		MWithMaMem<T> virtualCores(int virtualCores);
		MWithVcMem<T> maxAttempts(int maxAttempts);
		MWithVcMa<T> memory(int memory);
	}
	
	public interface MWithQueueMaMem<T> extends ApplicationMasterBuildable<T> {
		MWithMaMem<T> queueName(String queueName);
		MWithQueueMem<T> maxAttempts(int maxAttempts);
		MWithQueueMa<T> memory(int memory);
	}
	
	public interface MWithQueueVcMem<T> extends ApplicationMasterBuildable<T> {
		MWithVcMem<T> queueName(String queueName);
		MWithQueueMem<T> virtualCores(int virtualCores);
		MWithQueueVc<T> memory(int memory);
	}
	
	public interface MWithQueuePrMem<T> extends ApplicationMasterBuildable<T> {
		MWithPrMem<T> queueName(String queueName);
		MWithQueueMem<T> priority(int priority);
		MWithQueuePr<T> memory(int memory);
	}
	
	public interface MWithQueueVcMa<T> extends ApplicationMasterBuildable<T> {
		MWithQueueVc<T> maxAttempts(int maxAttempts);
		MWithQueueMa<T> virtualCores(int virtualCores);
		MWithVcMa<T> queueName(String queueName);
	}

	public interface MWithQueueMaPr<T> extends ApplicationMasterBuildable<T> {
		MWithQueueMa<T> priority(int priority);
		MWithQueuePr<T> maxAttempts(int maxAttempts);
		MWithVcPr<T> queueName(String queueName);
	}

	public interface MWithVcMaPr<T> extends ApplicationMasterBuildable<T> {
		MWithVcMa<T> priority(int priority);
		MWithVcPr<T> maxAttempts(int maxAttempts);
		MWithMaPr<T> virtualCores(int virtualCores);
	}

	public interface MWithQueueVcPr<T> extends ApplicationMasterBuildable<T> {
		MWithQueueVc<T> priority(int priority);
		MWithQueuePr<T> virtualCores(int virtualCores);
		MWithVcPr<T> queueName(String queueName);
	}
	
	public interface MWithPrMem<T> extends ApplicationMasterBuildable<T> {
		MWithMem<T> priority(int priority);
		MWithPr<T> memory(int memory);
	}
	
	public interface MWithMaMem<T> extends ApplicationMasterBuildable<T> {
		MWithMem<T> maxAttempts(int maxAttempts);
		MWithMa<T> memory(int memory);
	}
	
	public interface MWithVcMem<T> extends ApplicationMasterBuildable<T> {
		MWithMem<T> virtualCores(int virtualCores);
		MWithVc<T> memory(int memory);
	}
	
	public interface MWithQueueMem<T> extends ApplicationMasterBuildable<T> {
		MWithMem<T> queueName(String queueName);
		MWithQueue<T> memory(int memory);
	}
	
	public interface MWithQueuePr<T> extends ApplicationMasterBuildable<T> {
		MWithQueue<T> priority(int priority);
		MWithPr<T> queueName(String queueName);
	}

	public interface MWithArgumentsQueueVc<T> extends ApplicationMasterBuildable<T> {
		MWithQueue<T> virtualCores(int virtualCores);
		MWithVc<T> queueName(String queueName);
	}

	public interface MWithQueueVc<T> extends ApplicationMasterBuildable<T> {
		MWithQueue<T> virtualCores(int virtualCores);
		MWithVc<T> queueName(String queueName);
	}
	
	public interface MWithVcPr<T> extends ApplicationMasterBuildable<T> {
		MWithVc<T> priority(int priority);
		MWithPr<T> virtualCores(int virtualCores);
	}
	
	public interface MWithMaPr<T> extends ApplicationMasterBuildable<T> {
		MWithMa<T> priority(int priority);
		MWithPr<T> maxAttempts(int maxAttempts);
	}

	public interface MWithVcMa<T> extends ApplicationMasterBuildable<T> {
		MWithVc<T> maxAttempts(int maxAttempts);
		MWithMa<T> virtualCores(int virtualCores);
	}
	
	public interface MWithQueueMa<T> extends ApplicationMasterBuildable<T> {
		MWithQueue<T> maxAttempts(int maxAttempts);
		MWithMa<T> queueName(String queueName);
	}

	public interface MWithMem<T> extends ApplicationMasterBuildable<T> {
		ApplicationMasterBuildable<T> memory(int memory);
	}
	
	public interface MWithMa<T> extends ApplicationMasterBuildable <T>{
		ApplicationMasterBuildable<T> maxAttempts(int maxAttempts);
	}
	
	public interface MWithPr<T> extends ApplicationMasterBuildable<T> {
		ApplicationMasterBuildable<T> priority(int priority);
	}

	public interface MWithVc<T> extends ApplicationMasterBuildable<T> {
		ApplicationMasterBuildable<T> virtualCores(int virtualCores);
	}

	public interface MWithQueue<T> extends ApplicationMasterBuildable<T> {
		ApplicationMasterBuildable<T> queueName(String queueName);
	}

	public interface ApplicationMasterBuildable<T> {
		YarnApplication<T> build(String applicationName);
	}
}