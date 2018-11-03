package com.sunybingcloud.felk.sched;

import com.netflix.fenzo.TaskScheduler;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.plugins.VMLeaseObject;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * A basic implementation of the {@link FelkScheduler}.
 */
public class FelkSchedulerImpl extends FelkScheduler {

    public FelkSchedulerImpl(
            final TaskScheduler taskScheduler, final BlockingQueue<VirtualMachineLease> leasesQueue,
            final Map<String, String> launchedTasks, final Map<String, String> runningTasks) {
        setTaskScheduler(taskScheduler);
        setLeasesQueue(leasesQueue);
        setLaunchedTasks(launchedTasks);
        setRunningTasks(runningTasks);
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
