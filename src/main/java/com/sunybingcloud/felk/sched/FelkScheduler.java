package com.sunybingcloud.felk.sched;

import com.netflix.fenzo.TaskScheduler;
import com.netflix.fenzo.VirtualMachineLease;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Basic structure and functionality of a FelkScheduler. Default implementations provided
 * for most of the methods specified in {@link Scheduler}, however, when inheriting from
 * {@link FelkScheduler} one needs to provide implementation for {@code resourceOffers()}.
 *
 * Resource offers are received through the callback mechanism. Each offer would contain
 * availability information about the different compute resources of a host in the cluster.
 * In one way or the order, Fenzo's task scheduler needs to be notified of the receipt of
 * new resource offers so that they can be used assign tasks.
 */
public abstract class FelkScheduler implements Scheduler {
    protected TaskScheduler taskScheduler;
    protected BlockingQueue<VirtualMachineLease> leaseQueue;
    // Maintain TaskID and hostname of launched tasks, that are currently running.
    protected Map<String, String> runningTasks;

    /**
     * Maintain TaskID and hostname of launched tasks. TaskID would be retrieved from
     * the {@link com.netflix.fenzo.TaskAssignmentResult} object. Implementer of FelkScheduler should make sure to
     * update this data structure upon receipt of {@link com.netflix.fenzo.VMAssignmentResult}.
     */
    protected Map<String, String> launchedTasks;

    /**
     * Any previous resource offers are invalid. Fenzo scheduler needs to expire all leases.
     */
    @Override
    public void registered(SchedulerDriver driver, Protos.FrameworkID frameworkId, Protos.MasterInfo masterInfo) {
        System.out.println("Felk registered!");
        taskScheduler.expireAllLeases();
    }

    /**
     * Similar to {@code registered()}, Fenzo's scheduler needs to expire all leases.
     */
    @Override
    public void reregistered(SchedulerDriver driver, Protos.MasterInfo masterInfo) {
        System.out.println("Felk reregistered!");
        taskScheduler.expireAllLeases();
    }

    /**
     * The resource offer needs to be expireed immediately by Fenzo's task scheduler.
     */
    @Override
    public void offerRescinded(SchedulerDriver driver, Protos.OfferID offerId) {
        System.out.println("Offer [" + offerId.getValue() + "] rescinded.");
        taskScheduler.expireLease(offerId.getValue());
    }

    /**
     * Informing Fenzo's task scheduler if TERMINAL state received for a task.
     * Terminal states imply that the task is no longer executing. See {@link org.apache.mesos.Protos.TaskState}
     * for more information.
     */
    @Override
    public void statusUpdate(SchedulerDriver driver, Protos.TaskStatus status) {
        if (isTerminal(status)) {
            // Need to inform Fenzo's task scheduler to unassign the task, which was
            // previously running on a particular host.
            taskScheduler.getTaskUnAssigner().call(status.getTaskId().getValue(),
                    launchedTasks.get(status.getTaskId().getValue()));

            // Need to update the list of running tasks.
            runningTasks.remove(status.getTaskId().getValue());
        } else if (isActive(status)) {
            // No need to inform Fenzo's task scheduler.
            // Need to add the task to the set of running tasks.
            // TODO: Strip out hostname from slaveID and store only hostname.
            runningTasks.put(status.getTaskId().getValue(), status.getSlaveId().getValue());
        }
    }

    @Override
    public void frameworkMessage(SchedulerDriver driver, Protos.ExecutorID executorId, Protos.SlaveID slaveId, byte[] data) {
        System.out.println("Framework message host [ " + slaveId.getValue() + "] = " + Arrays.toString(data));
    }

    @Override
    public void disconnected(SchedulerDriver driver) {
        System.out.println("Framework disconnected!");
    }

    @Override
    public void slaveLost(SchedulerDriver driver, Protos.SlaveID slaveId) {
        System.out.println("Slave [" + slaveId.getValue() + "] lost!");
    }

    @Override
    public void executorLost(SchedulerDriver driver, Protos.ExecutorID executorId, Protos.SlaveID slaveId, int status) {
        System.out.println("Executor lost!");
    }

    @Override
    public void error(SchedulerDriver driver, String message) {
        System.out.println("Error: " + message);
    }

    /**
     * Indicate whether status is a terminal state. If a task is in a terminal state it
     * implies that the task is no longer running on the cluster.
     * @param taskStatus status of the task
     * @return is the task in a terminal state
     */
    protected boolean isTerminal(Protos.TaskStatus taskStatus) {
       switch (taskStatus.getState()) {
           case TASK_FINISHED:
           case TASK_ERROR:
           case TASK_FAILED:
           case TASK_KILLED:
               return true;
           default:
               return false;
       }
    }

    /**
     * Indicate whether the task is in an active state, implying that the task has just
     * begun executing, or is in staging.
     * @param taskStatus status of the task
     * @return is the task in an active state
     */
    protected boolean isActive(Protos.TaskStatus taskStatus) {
        switch (taskStatus.getState()) {
            case TASK_RUNNING:
            case TASK_STAGING:
            case TASK_STARTING:
                return true;
            default:
                return false;
        }
    }
}
