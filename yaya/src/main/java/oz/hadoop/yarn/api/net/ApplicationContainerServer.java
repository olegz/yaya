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
package oz.hadoop.yarn.api.net;

/**
 * Strategy for implementing ClientServers.
 * ClientServers is a server which will be created by the invoker of Yarn 
 * application prior to starting Application Master in order to facilitate communication 
 * with {@link ApplicationContainerClient}s.
 * 
 * @author Oleg Zhurakousky
 *
 */
public interface ApplicationContainerServer extends SocketHandler {

	/**
	 * Sets the wait time (in seconds) to receive a reply from the {@link ApplicationContainerClient}.
	 * Default is 1 min (60 sec).
	 * 
	 */
	void setReplyWaitTimeout(int replyWaitTimeout);

	/**
	 * Blocking method allowing this ClientServer to wait until all
	 * {@link ApplicationContainerClient}s have been connected to it.
	 */
	boolean awaitAllClients(long timeOutInSeconds);

	/**
	 * Returns an array of {@link ContainerDelegate}s which acts as
	 * local proxies to the remote Application Containers allowing 
	 * a simple message exchange with Application Containers.
	 */
	ContainerDelegate[] getContainerDelegates();
}