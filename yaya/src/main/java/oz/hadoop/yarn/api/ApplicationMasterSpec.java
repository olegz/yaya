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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;

/**
 * A specification class meant to customize the behavior of {@link YarnApplicationMaster}.
 * The two customization strategies are {@link AMRMClientAsync.CallbackHandler} and
 * {@link NMClientAsync.CallbackHandler}.
 *
 * While it provides a default implementation you may want to override it fully or partially.
 *
 * @author Oleg Zhurakousky
 *
 */
public class ApplicationMasterSpec {

	protected static final Log logger = LogFactory.getLog(ApplicationMasterSpec.class);

	/**
	 * Builds and instance of {@link AMRMClientAsync.CallbackHandler}
	 *
	 * @param yarnApplicationMaster
	 * @return
	 */
	protected AMRMClientAsync.CallbackHandler buildResourceManagerCallbackHandler(YarnApplicationMaster yarnApplicationMaster) {
		return new ResourceManagerCallbackHandler(yarnApplicationMaster);
	}

	/**
	 * Builds and instance of {@link NMClientAsync.CallbackHandler}
	 *
	 * @param yarnApplicationMaster
	 * @return
	 */
	protected NMClientAsync.CallbackHandler buildNodeManagerCallbackHandler(YarnApplicationMaster yarnApplicationMaster) {
		return new NodeManagerCallbaclHandler();
	}

	/**
	 * A callback handler registered with the {@link NodeManager} to be notified of
	 * life-cycle events of the deployed containers. For more information
	 * see {@link NMClientAsync.CallbackHandler} documentation
	 *
	 * While there may be no need for handling node manager call backs, the implementation
	 * of {@link NMClientAsyncImpl) requires an instance of such callback handler to be provided
	 * during the construction. Therefore current implementation simply logs received events.
	 */
	private static class NodeManagerCallbaclHandler implements NMClientAsync.CallbackHandler {

		@Override
		public void onContainerStopped(ContainerId containerId) {
			logger.info("Received stopped container callback: " + containerId);
		}

		@Override
		public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
			logger.info("Received container status callback: " + containerStatus);
		}

		@Override
		public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
			logger.info("Received container started callback: " + containerId);
		}

		@Override
		public void onStartContainerError(ContainerId containerId, Throwable t) {
			logger.error("Received container start callback: " + containerId, t);
		}

		@Override
		public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
			logger.error("Received container status callback: " + containerId, t);
		}

		@Override
		public void onStopContainerError(ContainerId containerId, Throwable t) {
			logger.info("Received stop container callback: " + containerId);
		}
	}

	/**
	 * A callback handler registered with the {@link AMRMClientAsync} to be notified of
	 * Resource Manager events allowing you to manage the life-cycle of the containers.
	 * The 'containerMonitor' - a {@link CountDownLatch} created with the same count as
	 * the allocated containers is used to manage the life-cycle of this Application Master.
	 */
	private class ResourceManagerCallbackHandler implements AMRMClientAsync.CallbackHandler {

		private final Log logger = LogFactory.getLog(ResourceManagerCallbackHandler.class);

		private final AtomicInteger completedContainersCounter = new AtomicInteger();

		private final YarnApplicationMaster applicationMaster;

		public ResourceManagerCallbackHandler(YarnApplicationMaster applicationMaster) {
			this.applicationMaster = applicationMaster;
		}

		@Override
		public void onContainersCompleted(List<ContainerStatus> completedContainers) {
			logger.info("Received completed contaners callback: " + completedContainers);
			 for (ContainerStatus containerStatus : completedContainers) {
				 logger.info("ContainerStatus: " + containerStatus);
				 this.applicationMaster.signalContainerCompletion(containerStatus);
				 this.completedContainersCounter.incrementAndGet();
			 }
		}

		/**
		 * Will launch each allocated container asynchronously.
		 */
		@Override
		public void onContainersAllocated(List<Container> allocatedContainers) {
			logger.info("Received allocated containers callback: " + allocatedContainers);
			for (final Container allocatedContainer : allocatedContainers) {
				this.applicationMaster.launchContainerAsync(allocatedContainer);
			}
		}

		@Override
		public void onShutdownRequest() {
			logger.info("Received shut down callback");
		}

		@Override
		public void onNodesUpdated(List<NodeReport> updatedNodes) {
			logger.info("Received node update callback for " + updatedNodes);
		}

		@Override
		public float getProgress() {
			float progress =
					(float) this.completedContainersCounter.get() / this.applicationMaster.containerCount;
			return progress;
		}

		@Override
		public void onError(Throwable e) {
			logger.error("Received error", e);
		}
	}

}
