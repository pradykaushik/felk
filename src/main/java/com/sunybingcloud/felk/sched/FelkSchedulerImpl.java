/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sunybingcloud.felk.sched;

import com.netflix.fenzo.TaskScheduler;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.functions.Action1;
import com.netflix.fenzo.functions.Action2;
import com.netflix.fenzo.plugins.VMLeaseObject;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;

import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * A basic implementation of the {@link FelkScheduler}.
 */
public class FelkSchedulerImpl extends FelkScheduler {

    public FelkSchedulerImpl(
            final TaskScheduler taskScheduler,
            final BlockingQueue<VirtualMachineLease> leasesQueue,
            final Action2<String, String> onTaskRunning,
            final Action1<String> onTaskComplete) {
        setTaskScheduler(taskScheduler);
        setLeasesQueue(leasesQueue);
        setOnTaskRunning(onTaskRunning);
        setOnTaskComplete(onTaskComplete);
    }

    /**
     * Adding the resource offers to the leaseQueue Fenzo's task scheduler can pick it
     * up when it's assigning tasks to hosts corresponding to offers.
     */
    @Override
    public void resourceOffers(final SchedulerDriver driver, final List<Protos.Offer> offers) {
        for (Protos.Offer offer : offers) {
            System.out.println("Received Offer with ID = " + offer.getId().getValue()
                    + " for " + "host [" + offer.getHostname() + "]");
            getLeasesQueue().offer(new VMLeaseObject(offer));
        }
    }
}
