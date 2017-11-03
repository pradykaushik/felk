package com.sunybingcloud.felk;

import com.netflix.fenzo.TaskScheduler;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.functions.Action1;
import com.netflix.fenzo.plugins.BinPackingFitnessCalculators;
import com.sunybingcloud.felk.config.Schema;
import com.sunybingcloud.felk.config.task.Task;
import com.sunybingcloud.felk.sched.FelkSchedulerImpl;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

class FelkFramework {

    private Collection<Task> taskQueue;
    private TaskScheduler taskScheduler;
    private MesosSchedulerDriver mesosSchedulerDriver;
    private String mesosMaster;
    private final AtomicReference<MesosSchedulerDriver> mesosSchedulerDriverRef = new AtomicReference<>();
    private Scheduler felkScheduler;
    private AtomicBoolean isShutdown = new AtomicBoolean(false);

    public class LeaseRejectActionBuilder {
        private Action1<VirtualMachineLease> leaseRejectAction;
        private Map<String, Action1<VirtualMachineLease>> leaseRejectActionsDirectory = createLeaseRejectActions();

        /**
         * Defining different leaseRejectActions associated with a name. This is done in order for the user
         * to be able to specify the lease reject action in the configuration file.
         * @return a mapping from a name to a lease reject action.
         */
        private Map<String, Action1<VirtualMachineLease>> createLeaseRejectActions() {
            Map<String, Action1<VirtualMachineLease>> actions = new HashMap<>();
            actions.put("decline", virtualMachineLease ->
                    mesosSchedulerDriverRef.get().declineOffer(virtualMachineLease.getOffer().getId()));
            return actions;
        }

        public LeaseRejectActionBuilder withLeaseRejectAction(String actionName) {
            leaseRejectAction = leaseRejectActionsDirectory.get(actionName);
            return this;
        }

        public Action1<VirtualMachineLease> build() {
            return leaseRejectAction;
        }
    }

    final class Builder {
        private Map<String, VMTaskFitnessCalculator> fitnessCalculators = createFitnessCalculatorRegistry();
        private TaskScheduler taskScheduler;
        private TaskScheduler.Builder taskSchedulerBuilder = new TaskScheduler.Builder();
        private Collection<Task> taskQueue;

        private Map<String, VMTaskFitnessCalculator> createFitnessCalculatorRegistry() {
            Map<String, VMTaskFitnessCalculator> fcr = new HashMap<>();
            fcr.put("cpuBinPacker", BinPackingFitnessCalculators.cpuBinPacker);
            fcr.put("memBinPacker", BinPackingFitnessCalculators.memoryBinPacker);
            fcr.put("cpuMemBinPacker", BinPackingFitnessCalculators.cpuMemBinPacker);
            return fcr;
        }

        /**
         * Build the taskScheduler based on the schema.
         * @param schema The workload configuration schema.
         * @return Builder
         */
        Builder withSchema(Schema schema) {
            // Input validation needs to have been done prior to this.
            taskQueue = schema.getTasks();
            // If no fitness calculator specified, then we're not going to be using one.
            String fc;
            if ((fc = schema.getFitnessCalculator()) != null) {
                taskSchedulerBuilder.withFitnessCalculator(fitnessCalculators.get(fc));
            }
            // If lease offer expiry not specified (or is 0), then not explicitly specifying (using default 120 seconds).
            if (schema.getLeaseOfferExpirySeconds() > 0) {
                taskSchedulerBuilder.withLeaseOfferExpirySecs(schema.getLeaseOfferExpirySeconds());
            }
            if (schema.isDisableShortfallEvaluation()) taskSchedulerBuilder.disableShortfallEvaluation();

            // Default lease reject action is 'decline'.
            // Right now, we have only one kind of action that can be specified.
            // Modify code when we have non-default lease reject actions specifiable.
            taskSchedulerBuilder.withLeaseRejectAction(new LeaseRejectActionBuilder()
                    .withLeaseRejectAction(schema.getLeaseRejectAction()).build());
            return this;
        }

        public FelkFramework build() {
            return new FelkFramework(this);
        }
    }

    FelkFramework() {}

    FelkFramework(Builder builder) {
        taskQueue = builder.taskQueue;
        taskScheduler = builder.taskSchedulerBuilder.build();
        felkScheduler = new FelkSchedulerImpl(taskScheduler);
    }

    FelkFramework buildSchedulerDriver(String mesosMaster) {
        // Creating FrameworkInfo
        Protos.FrameworkInfo frameworkInfo = Protos.FrameworkInfo.newBuilder()
                .setName("felk")
                .setUser("")
                .build();
        mesosSchedulerDriver = new MesosSchedulerDriver(felkScheduler, frameworkInfo, mesosMaster);
        mesosSchedulerDriverRef.set(mesosSchedulerDriver);
        return this;
    }

    /**
     * Running a scheduling loop that iteratively calls {@link TaskScheduler#scheduleOnce(List, List)},
     * by passing the current set of pending tasks and any new resource offers received from mesos.
     * {@link com.sunybingcloud.felk.sched.FelkScheduler#leaseQueue} would contain the latest
     * unused set of resource offers.
     * Upon receipt of task assignment information from Fenzo's task scheduler, the
     * corresponding tasks are launched on the hosts corresponding to the consumed offer.
     * Upon launching a task, the respective {@link Task#instances} needs to be decremented.
     */
    void execute() {
        List<VirtualMachineLease> newLeases = new ArrayList<>();
        // Need to launch felk to execute all the tasks using Fenzo's task scheduler.
        System.out.println("Executing...");
    }

    void shutdown() {
        System.out.println("Shutting down mesos scheduler driver...");
        mesosSchedulerDriver.stop();
        // Letting the runner know that the driver has been shutdown.
        // This trigger can be used to perform any necessary clean up before Felk stops running.
        isShutdown.set(true);
    }
}
