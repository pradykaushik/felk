package com.sunybingcloud.felk;

import com.netflix.fenzo.SchedulingResult;
import com.netflix.fenzo.TaskScheduler;
import com.netflix.fenzo.VMAssignmentResult;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.functions.Action1;
import com.netflix.fenzo.plugins.BinPackingFitnessCalculators;
import com.sunybingcloud.felk.config.Schema;
import com.sunybingcloud.felk.config.task.Task;
import com.sunybingcloud.felk.config.task.TaskUtils;
import com.sunybingcloud.felk.sched.FelkSchedulerImpl;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

class FelkFramework {

    /**
     * Tasks pending execution. Key = {@link Task#getId()}, Value = {@link Task}
     * Whenever Fenzo's task scheduler assigns a task to a host, then we need to decrement
     * the number of instances, of that task, that are remaining to be executed.
     * If all the instances of a task have been executed, then we remove the entry from {@code pendingTasks}
     */
    private Map<String, Task> pendingTasks = new HashMap<>();
    private TaskScheduler taskScheduler;
    private BlockingQueue<VirtualMachineLease> leasesQueue;
    private Map<String, String> launchedTasks;
    private Map<String, String> runningTasks;
    private MesosSchedulerDriver mesosSchedulerDriver;
    private String mesosMaster;
    private final AtomicReference<MesosSchedulerDriver> mesosSchedulerDriverRef = new AtomicReference<>();
    private Scheduler felkScheduler;
    private AtomicBoolean isDone;
    private AtomicBoolean canShutdown;

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
        private TaskScheduler.Builder taskSchedulerBuilder = new TaskScheduler.Builder();
        private Collection<Task> pendingTaskQueue;
        private String masterLocation;
        private AtomicBoolean isDone;
        private AtomicBoolean canShutdown;

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
            pendingTaskQueue = schema.getTasks();
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

        /**
         * Initialize the location string of the Mesos master daemon.
         * @param masterLocation Location string of the mesos master in the form <host>:<port>
         * @return Builder
         */
        Builder withMasterLocation(String masterLocation) {
            this.masterLocation = masterLocation;
            return this;
        }

        /**
         * Indicating whether the framework is done launching all tasks.
         */
        Builder withDone(AtomicBoolean isDone) {
            this.isDone = isDone;
            return this;
        }

        /**
         * Indicating whether all the launched tasks have completed execution.
         */
        Builder withShutdown(AtomicBoolean canShutdown) {
            this.canShutdown = canShutdown;
            return this;
        }

        public FelkFramework build() {
            return new FelkFramework(this);
        }
    }

    FelkFramework() {}

    FelkFramework(Builder builder) {
        // Assigning unique task ID to every instance of every pending task.
        TaskUtils.TaskIdGenerator.assignTaskIds(builder.pendingTaskQueue);
        // Creating pendingTasks map. For each instance of each task, an entry is made.
        // TODO: Unnecessary many objects created. Fix this.
        builder.pendingTaskQueue.forEach( pt -> {
           for (int i = pt.getInstances().get(); i > 0; i--) {
               Task t = new Task(pt);
               t.setInstance(i);
               pendingTasks.put(t.getId(), t);
           }
        });
        taskScheduler = builder.taskSchedulerBuilder.build();
        leasesQueue = new LinkedBlockingQueue<>();
        launchedTasks = new HashMap<>();
        runningTasks = new HashMap<>();
        mesosMaster = builder.masterLocation;
        isDone = builder.isDone;
        canShutdown = builder.canShutdown;
        felkScheduler = new FelkSchedulerImpl(taskScheduler, leasesQueue, launchedTasks, runningTasks);

        // Creating FrameworkInfo
        Protos.FrameworkInfo frameworkInfo = Protos.FrameworkInfo.newBuilder()
            .setName("Felk")
            .setUser("")
            .build();
        // Building the Mesos scheduler driver.
        mesosSchedulerDriver = new MesosSchedulerDriver(felkScheduler, frameworkInfo, mesosMaster);
        mesosSchedulerDriverRef.set(mesosSchedulerDriver);
        new Thread(() -> mesosSchedulerDriver.run()).start();
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
        while(true) {
            // Checking whether there are any pending tasks to schedule.
            if (pendingTasks.isEmpty()) {
                System.out.println("All tasks have been launched. No more pending tasks.");
                // There are no more tasks pending execution.
                isDone.set(true);
                // Checking whether there are any tasks that are still executing.
                // Also, checking is launchedTasks is empty. This is because, there could be tasks that were assigned
                // to hosts by Fenzo's task scheduler, and were launched, but we haven't received a status update yet.
                while (true) {
                    if (launchedTasks.isEmpty() && runningTasks.isEmpty()) {
                        System.out.println("All tasks have been launched and have completed execution");
                        System.out.println("Shutting down Felk!");
                        // All launched tasks have completed execution and there are no more tasks that are currently running.
                        // It is okay to shutdown the framework.
                        // TODO: When dealing with a continuous workload, we would need to constantly poll the pendingTasks.
                        canShutdown.set(true);
                        return;
                    }
                }
            }

            // Clearing all previous resource offers.
            newLeases.clear();
            // Recording all the new resource offers, if any.
            leasesQueue.drainTo(newLeases);


            // Using Fenzo's task scheduler to make task assignments on our behalf.
            // Task assignment information is then used to launch tasks on the hosts. If the task assignment
            // is taken into consideration, where in the tasks are launched using the mesos scheduler driver,
            // then we need to call Fenzo's taskAssigner for each launched.
            SchedulingResult schedulingResult = taskScheduler
                .scheduleOnce(new ArrayList<>(pendingTasks.values()), newLeases);
            Map<String, VMAssignmentResult> schedulerAssignmentResult;
            if (!(schedulerAssignmentResult = schedulingResult.getResultMap()).isEmpty()) {
                schedulerAssignmentResult.forEach((vm, tasksAssignment) -> {
                    List<VirtualMachineLease> leasesUsed = tasksAssignment.getLeasesUsed();
                    StringBuilder stringBuilder = new StringBuilder("Launching on host [" + vm + "] tasks [");
                    List<Protos.TaskInfo> taskInfos = new ArrayList<>();
                    // As the slaveID is common for all task assignments to a given host.
                    Protos.SlaveID slaveID = leasesUsed.get(0).getOffer().getSlaveId();
                    tasksAssignment.getTasksAssigned().forEach(assignedTask -> {
                        stringBuilder.append(assignedTask.getTaskId()).append(", ");
                        // Creating TaskInfo object.
                        taskInfos.add(TaskUtils.TaskInfoGenerator.getTaskInfo(slaveID,
                            ((Task) assignedTask.getRequest())));
                        // Removing task from the set of pending tasks.
                        pendingTasks.remove(assignedTask.getTaskId());
                        // Adding pTask to launched tasks.
                        launchedTasks.put(assignedTask.getTaskId(), vm);
                        // Informing Fenzo that we have accepted this task assignment.
                        taskScheduler.getTaskAssigner().call(assignedTask.getRequest(), vm);
                    });

                    List<Protos.OfferID> offerIDs = new ArrayList<>();
                    leasesUsed.forEach( lease -> offerIDs.add(lease.getOffer().getId()));
                    stringBuilder.append("]");
                    System.out.println(stringBuilder.toString());
                    mesosSchedulerDriver.launchTasks(offerIDs, taskInfos);
                });
            }
        }
    }

    void shutdown() {
        System.out.println("Shutting down mesos scheduler driver...");
        Protos.Status status = mesosSchedulerDriver.stop();
        System.out.println("Shutdown status = " + status.toString());
    }
}
