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
package com.sunybingcloud.felk;

import com.netflix.fenzo.SchedulingResult;
import com.netflix.fenzo.TaskScheduler;
import com.netflix.fenzo.VMAssignmentResult;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.functions.Action1;
import com.netflix.fenzo.functions.Action2;
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
     * If all the instances of a task have been executed, then we remove the entry from
     * {@code pendingTasks}
     */
    private Map<String, Task> pendingTasks = new HashMap<>();
    private TaskScheduler taskScheduler;
    private BlockingQueue<VirtualMachineLease> leasesQueue;

    /**
     * Maintain TaskID and hostname of launched tasks. TaskID would be retrieved from
     * the {@link com.netflix.fenzo.TaskAssignmentResult} object.
     * Update this data structure upon receipt of {@link com.netflix.fenzo.VMAssignmentResult}, and
     * also when a terminal status message (LOST, FAILED, KILLED, FINISHED) is received.
     */
    private Map<String, String> launchedTasks;
    private Map<String, String> runningTasks;
    private MesosSchedulerDriver mesosSchedulerDriver;
    private String mesosMaster;
    private final AtomicReference<MesosSchedulerDriver> mesosSchedulerDriverRef
            = new AtomicReference<>();
    private Scheduler felkScheduler;
    private AtomicBoolean isDone;
    private AtomicBoolean canShutdown;
    private Action2<String, String> onTaskLaunch;
    private Action2<String, String> onTaskRunning;
    private Action1<String> onTaskComplete;

    /**
     * Lease rejection actions supported.
     * Storing a mapping between the name of the action and the action itself. This allows
     * for the action to be specified in the configuration.
     */
    private Map<String, Action1<VirtualMachineLease>> leaseRejectActions;
    private enum LeaseRejectActionName {
        DECLINE("decline");

        private String name;

        LeaseRejectActionName(final String name) {
            this.name = name;
        }

        String get() {
            return this.name;
        }

        static boolean isValidActionName(final String name) {
            for (LeaseRejectActionName actionName : LeaseRejectActionName.values()) {
                if (actionName.get().equals(name)) {
                    return true;
                }
            }
            return false;
        }
    }

    private void initLeaseRejectActions() {
        leaseRejectActions = new HashMap<>();
        leaseRejectActions.put("decline", virtualMachineLease ->
                mesosSchedulerDriverRef.get().declineOffer(
                    virtualMachineLease.getOffer().getId()));
    }

    /**
     * Fitness calculators supported.
     */
    private Map<String, VMTaskFitnessCalculator> fitnessCalculators;

    private void initFitnessCalculators() {
        fitnessCalculators = new HashMap<>();
        fitnessCalculators.put("cpuBinPacker", BinPackingFitnessCalculators.cpuBinPacker);
        fitnessCalculators.put("memBinPacker", BinPackingFitnessCalculators.memoryBinPacker);
        fitnessCalculators.put("cpuMemBinPacker", BinPackingFitnessCalculators.cpuMemBinPacker);
    }

    static final class Builder {
        private String fitnessCalculator;
        private long leaseOfferExpirySeconds;
        private boolean isShortFallEvaluationDisabled;
        private String leaseRejectActionName;
        private Collection<Task> pendingTaskQueue;
        private String masterLocation;
        private AtomicBoolean isDone;
        private AtomicBoolean canShutdown;

        /**
         * Build the taskScheduler based on the schema.
         * @param schema The workload configuration schema.
         * @return Builder
         */
        Builder withSchema(final Schema schema) {
            // Input validation needs to have been done prior to this.
            pendingTaskQueue = schema.getTasks();
            // Initializing parameters required to build the task scheduler.
            fitnessCalculator = schema.getFitnessCalculator();
            leaseOfferExpirySeconds = schema.getLeaseOfferExpirySeconds();
            isShortFallEvaluationDisabled = schema.isDisableShortfallEvaluation();
            leaseRejectActionName = schema.getLeaseRejectAction();
            return this;
        }

        /**
         * Initialize the location of the Mesos master daemon.
         * @param masterLocation Location of the mesos master in the form <host>:<port>
         * @return Builder
         */
        Builder withMasterLocation(final String masterLocation) {
            this.masterLocation = masterLocation;
            return this;
        }

        /**
         * Indicating whether the framework is done launching all tasks.
         */
        Builder withDone(final AtomicBoolean isDone) {
            this.isDone = isDone;
            return this;
        }

        /**
         * Indicating whether all the launched tasks have completed execution.
         */
        Builder withShutdown(final AtomicBoolean canShutdown) {
            this.canShutdown = canShutdown;
            return this;
        }

        public FelkFramework build() {
            return new FelkFramework(this);
        }
    }

    FelkFramework() {

    }

    private FelkFramework(final Builder builder) {
        initLeaseRejectActions();
        initFitnessCalculators();

        // Assigning unique task ID to every instance of every pending task.
        TaskUtils.TaskIdGenerator.assignTaskIds(builder.pendingTaskQueue);
        // Creating pendingTasks map. For each instance of each task, an entry is made.
        // TODO Unnecessary many objects created. Fix this.
        builder.pendingTaskQueue.forEach(pt -> {
            for (int i = pt.getInstances().get(); i > 0; i--) {
                Task t = new Task(pt);
                t.setInstance(i);
                pendingTasks.put(t.getId(), t);
            }
        });

        // Building the task scheduler.
        // If no fitness calculator specified, then we're not going to be using one.
        TaskScheduler.Builder taskSchedulerBuilder = new TaskScheduler.Builder();
        if ((builder.fitnessCalculator != null) && !(builder.fitnessCalculator.trim().equals(""))) {
            taskSchedulerBuilder.withFitnessCalculator(
                    fitnessCalculators.get(builder.fitnessCalculator));
        } else {
            throw new IllegalArgumentException("Invalid fitness calculator specified!");
        }

        // If lease offer expiry not specified (or is 0), then not explicitly specifying
        // (using default 120 seconds).
        if (builder.leaseOfferExpirySeconds > 0) {
            taskSchedulerBuilder.withLeaseOfferExpirySecs(builder.leaseOfferExpirySeconds);
        }
        if (builder.isShortFallEvaluationDisabled) {
            taskSchedulerBuilder.disableShortfallEvaluation();
        }

        // Default lease reject action is 'decline'. Right now, we have only one kind of action
        // that can be specified.
        taskSchedulerBuilder.withLeaseRejectAction(leaseRejectActions.get(builder
                .leaseRejectActionName));

        taskScheduler = taskSchedulerBuilder.build();
        leasesQueue = new LinkedBlockingQueue<>();
        launchedTasks = new HashMap<>();
        runningTasks = new HashMap<>();
        mesosMaster = builder.masterLocation;
        isDone = builder.isDone;
        canShutdown = builder.canShutdown;
        onTaskLaunch = (taskId, vmHostname) -> {
            // Remove the task from the list of pending tasks.
            pendingTasks.remove(taskId);
            // Register the TaskID of the task and the hostname of the vm that this task was
            // assigned to.
            launchedTasks.put(taskId, vmHostname);
        };

        onTaskRunning = (taskId, slaveId) -> {
            // Adding the task to the list of running tasks.
            runningTasks.put(taskId, slaveId);
        };

        onTaskComplete = taskId -> {
            // Need to inform Fenzo's task scheduler to unassign the task, which was
            // previously running on a particular host.
            taskScheduler.getTaskUnAssigner().call(taskId, launchedTasks.get(taskId));
            // Task is no longer running.
            runningTasks.remove(taskId);
            // Need to remove the task from the set of launched tasks.
            // As Fenzo has already been informed to unassign the task, it's okay to remove entry
            // from launchedTasks.
            launchedTasks.remove(taskId);
        };

        felkScheduler = new FelkSchedulerImpl(taskScheduler, leasesQueue,
                onTaskRunning, onTaskComplete);

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
     * Running a scheduling loop that iteratively calls
     * {@link TaskScheduler#scheduleOnce(List, List)}, by passing the current set of pending tasks
     * and any new resource offers received from mesos.
     * {@link com.sunybingcloud.felk.sched.FelkScheduler#leasesQueue} would contain the latest
     * unused set of resource offers. Upon receipt of task assignment information from Fenzo's
     * task scheduler, the corresponding tasks are launched on the hosts corresponding to the
     * consumed offer. Upon launching a task, the respective {@link Task#instances} needs to be
     * decremented.
     */
    void execute() {
        List<VirtualMachineLease> newLeases = new ArrayList<>();
        while (true) {
            // Checking whether there are any pending tasks to schedule.
            if (pendingTasks.isEmpty()) {
                System.out.println("All tasks have been launched. No more pending tasks.");
                // There are no more tasks pending execution.
                isDone.set(true);
                // Checking whether there are any tasks that are still executing.
                // Also, checking if launchedTasks is empty. This is because, there could be tasks
                // that were assigned to hosts by Fenzo's task scheduler, and were launched, but we
                // haven't received a status update yet.
                while (true) {
                    if (launchedTasks.isEmpty() && runningTasks.isEmpty()) {
                        System.out.println("All tasks have been launched and have "
                                + "completed execution");
                        System.out.println("Shutting down Felk!");
                        // All launched tasks have completed execution and there are no more tasks
                        // that are currently running.
                        // It is okay to shutdown the framework.
                        // TODO When dealing with a continuous workload, we would need to
                        //      constantly poll the pendingTasks.
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
            // Task assignment information is then used to launch tasks on the hosts. If the task
            // assignment is taken into consideration, where in the tasks are launched using the
            // mesos scheduler driver, then we need to call Fenzo's taskAssigner for each launched.
            SchedulingResult schedulingResult = taskScheduler
                    .scheduleOnce(new ArrayList<>(pendingTasks.values()), newLeases);
            Map<String, VMAssignmentResult> schedulerAssignmentResult;
            if (!schedulingResult.getResultMap().isEmpty()) {
                schedulerAssignmentResult = schedulingResult.getResultMap();
                schedulerAssignmentResult.forEach((vm, tasksAssignment) -> {
                    List<VirtualMachineLease> leasesUsed = tasksAssignment.getLeasesUsed();
                    StringBuilder stringBuilder = new StringBuilder("Launching on host "
                            + "[" + vm + "] tasks [");
                    List<Protos.TaskInfo> taskInfos = new ArrayList<>();
                    // As the slaveID is common for all task assignments to a given host.
                    Protos.SlaveID slaveID = leasesUsed.get(0).getOffer().getSlaveId();
                    tasksAssignment.getTasksAssigned().forEach(assignedTask -> {
                        stringBuilder.append(assignedTask.getTaskId()).append(", ");
                        // Creating TaskInfo object.
                        taskInfos.add(TaskUtils.TaskInfoGenerator.getTaskInfo(slaveID,
                                ((Task) assignedTask.getRequest())));
                        // Adding pTask to launched tasks.
                        onTaskLaunch.call(assignedTask.getTaskId(), vm);
                        // Informing Fenzo that we have accepted this task assignment.
                        taskScheduler.getTaskAssigner().call(assignedTask.getRequest(), vm);
                    });

                    List<Protos.OfferID> offerIDs = new ArrayList<>();
                    leasesUsed.forEach(lease -> offerIDs.add(lease.getOffer().getId()));
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
