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

import java.util.Map;
import java.util.concurrent.RejectedExecutionException;


/**
 * Strategy representing an assembled YARN Application.
 * It exposes several life-cycle method to allow you to perform launch/shutdown as
 * well as inquiries on the state of the application.
 * 
 * @author Oleg Zhurakousky
 *
 */
public interface YarnApplication<T> {

	/**
	 * Method to launch this Application. Supports launch for both 
	 * long-running Application Containers and finite Application Containers which 
	 * will exit upon completion of the task.
	 * 
	 * Will launch this application asynchronously only blocking temporarily while 
	 * awaiting for all Application Containers to establish connectivity with Application Master.
	 * Once such connectivity is established this method will return.
	 * Its return does not signify the completion of the application. In fact 
	 * with long-running Application Containers, the application will never finish
	 * until {@link #shutDown()} or {@link #terminate()} is called.
	 * However, regardless whether this application is a long-running or finite, you should 
	 * check on its status via {@link #isRunning()} method. There is also {@link #liveContainers()} method
	 * which can assist in obtaining more knowledge about your application status.
	 * <br>
	 * If this application is finite it will exit on its own upon completion and if it happens
	 * then the call to {@link #isRunning()} will return 'false' even without invoking {@link #shutDown()}
	 * or {@link #terminate()}.
	 * Invoking {@link #shutDown()} will shut down this application gracefully allowing task currently executing 
	 * by Application Containers to finish while not accepting any new tasks. Invoking {@link #terminate()}
	 * will essentially kill the application. 
	 * 
	 * @return
	 * 	  T which could be either 'void' for finite Application Containers or 
	 *    {@link DataProcessor}s for long-running Application Containers.
	 * 	
	 */
	T launch();
	
	void awaitLaunch();
	
	void awaitFinish();
	
	/**
	 * Will attempt shut down this application gracefully, allowing Application Container
	 * tasks running as commands or java {@link ApplicationContainerProcessor}s to finish
	 * while not accepting any new tasks. However, finite application will exit on its own 
	 * upon completion of all tasks. In this case subsequent calls to this method will have 
	 * no effect.<br>
	 * Application can also be terminated (killed) via {@link #terminate()} method.<br>
	 * NOTE: It is normal to see {@link RejectedExecutionException} in the logs when invoking this 
	 * method especially in the cases where {@link DataProcessor}'s process(..) method invoked in a 
	 * separate thread. It simply means that new processes are rejected while framework let's 
	 * currently running processes finish.
	 */
	void shutDown();
	
	/**
	 * Will terminate (kill) this application.<br>
	 * NOTE: This method will interrupt all currently running Application Container
	 * tasks running as commands or java {@link ApplicationContainerProcessor}s.
	 * For graceful shutdown see {@link #shutDown()} method.
	 */
	void terminate();
	
	/**
	 * Will return 'true' if application is still running and 'false' if its finished.
	 * Finite applications will exit upon its completion and if this call is made 
	 * after the exit it will return 'false'.
	 * <br>
	 * Also, see {@link #liveContainers()} method.
	 * @return
	 */
	boolean isRunning();
	
	/**
	 * Will return the count of currently running (alive) Application Containers. 
	 * Typical use of this method is purely informational/debugging since
	 * it essentially provides you with additional information related to 
	 * the {@link #isRunning()} state of your application. 
	 * 
	 * @return
	 */
	int liveContainers();
	
	/**
	 * Returns an immutable {@link Map} of the application specification.
	 * Mainly used for purely informational/debugging purposes.
	 * 
	 * @return
	 */
	Map<String, Object> getApplicationSpecification();
	
	/**
	 * Allow for the registration of the {@link ContainerReplyListener} for the cases where
	 * you need to deal with replies produced by the {@link ApplicationContainerProcessor}s or commands executed 
	 * by Application Containers.
	 *
	 * @param replyListener
	 */
	void registerReplyListener(ContainerReplyListener replyListener);
}
