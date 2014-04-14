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

import oz.hadoop.yarn.api.YarnAssembly.ApplicationMasterBuildable;
import oz.hadoop.yarn.api.YarnAssembly.MWithQueueMaPrMem;
import oz.hadoop.yarn.api.YarnAssembly.MWithQueueVcMaMem;
import oz.hadoop.yarn.api.YarnAssembly.MWithQueueVcMaPr;
import oz.hadoop.yarn.api.YarnAssembly.MWithQueueVcPrMem;
import oz.hadoop.yarn.api.YarnAssembly.MWithVcMaPrMem;

/**
 * @author Oleg Zhurakousky
 *
 */
public interface ApplicationMasterSpecBuilder<T> extends ApplicationMasterBuildable<T> { // QueueVcMaPrMem
	MWithVcMaPrMem<T> queueName(String queueName);
	MWithQueueVcMaMem<T> priority(int priority);
	MWithQueueVcPrMem<T> maxAttempts(int maxAttempts);
	MWithQueueMaPrMem<T> virtualCores(int virtualCores);
	MWithQueueVcMaPr<T> memory(int memory);
}
