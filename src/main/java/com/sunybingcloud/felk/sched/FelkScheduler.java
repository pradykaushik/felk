package com.sunybingcloud.felk.sched;

import com.netflix.fenzo.TaskScheduler;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.functions.Action1;
import com.netflix.fenzo.functions.Action2;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import java.util.Arrays;
import java.util.concurrent.BlockingQueue;

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
    private TaskScheduler taskScheduler;
    private BlockingQueue<VirtualMachineLease> leasesQueue;
    private Action2<String, String> onTaskRunning;
    private Action1<String> onTaskComplete;

    /**
     * Set the task scheduler.
     * @param taskScheduler Task scheduler configured based on the user specified config.
     */
    protected void setTaskScheduler(final TaskScheduler taskScheduler) {
        this.taskScheduler = taskScheduler;
    }

    /**
     * Retrieved the task scheduler that is Felk is uses to make scheduling decisions.
     * @return task scheduler.
     */
    protected TaskScheduler getTaskScheduler() {
        return taskScheduler;
    }

    /**
     * Store the new set of resource offers received from the mesos callback mechanism.
     * @param leasesQueue new set of resource offers.
     */
    protected void setLeasesQueue(final BlockingQueue<VirtualMachineLease> leasesQueue) {
        this.leasesQueue = leasesQueue;
    }

    /**
     * Retrieved the new set of resource offers received from mesos.
     * @return new set of resource offers.
     */
    protected BlockingQueue<VirtualMachineLease> getLeasesQueue() {
        return leasesQueue;
    }

    /**
     * Set the action to be taken when an active status message is received for a task.
     */
    protected void setOnTaskRunning(final Action2<String, String> onTaskRunning) {
        this.onTaskRunning = onTaskRunning;
    }

    /**
     * Retrieve the action to be taken when an active status is received for the task.
     */
    protected Action2<String, String> getOnTaskRunning() {
        return onTaskRunning;
    }

    /**
     * Set the action to be taken when a terminal status message is received for a task.
     */
    protected void setOnTaskComplete(final Action1<String> onTaskComplete) {
        this.onTaskComplete = onTaskComplete;
    }

    /**
     * Retrieve the action to be taken when a terminal status message is received for a task.
     */
    protected Action1<String> getOnTaskComplete() {
        return onTaskComplete;
    }

    /**
     * Any previous resource offers are invalid. Fenzo scheduler needs to expire all leases.
     */
    @Override
    public void registered(
            final SchedulerDriver driver,
            final Protos.FrameworkID frameworkId,
            final Protos.MasterInfo masterInfo) {
        System.out.println("Felk registered!");
        taskScheduler.expireAllLeases();
    }

    /**
     * Similar to {@code registered()}, Fenzo's scheduler needs to expire all leases.
     */
    @Override
    public void reregistered(final SchedulerDriver driver, final Protos.MasterInfo masterInfo) {
        System.out.println("Felk reregistered!");
        taskScheduler.expireAllLeases();
    }

    /**
     * The resource offer needs to be expireed immediately by Fenzo's task scheduler.
     */
    @Override
    public void offerRescinded(final SchedulerDriver driver, final Protos.OfferID offerId) {
        System.out.println("Offer [" + offerId.getValue() + "] rescinded.");
        taskScheduler.expireLease(offerId.getValue());
    }

    /**
     * Informing Fenzo's task scheduler if TERMINAL state received for a task.
     * Terminal states imply that the task is no longer executing.
     * See {@link org.apache.mesos.Protos.TaskState} for more information.
     */
    @Override
    public void statusUpdate(final SchedulerDriver driver, final Protos.TaskStatus status) {
        System.out.println("Task Status : task [" + status.getTaskId().getValue() + "], "
                + "state [" + status.getState() + "]");
        if (isTerminal(status)) {
            onTaskComplete.call(status.getTaskId().getValue());
        } else if (isActive(status)) {
            onTaskRunning.call(status.getTaskId().getValue(), status.getSlaveId().getValue());
        }
    }

    /**
     * Message received from the executor.
     */
    @Override
    public void frameworkMessage(
            final SchedulerDriver driver,
            final Protos.ExecutorID executorId,
            final Protos.SlaveID slaveId,
            final byte[] data) {
        System.out.println("Framework message host [ " + slaveId.getValue()
                + "] = " + Arrays.toString(data));
    }

    /**
     * Invoked when the scheduler becomes disconnected with the mesos master.
     */
    @Override
    public void disconnected(final SchedulerDriver driver) {
        System.out.println("Framework disconnected!");
    }

    /**
     * Invoked when a slave has been determined unreachable.
     */
    @Override
    public void slaveLost(final SchedulerDriver driver, final Protos.SlaveID slaveId) {
        System.out.println("Slave [" + slaveId.getValue() + "] lost!");
    }

    /**
     * Invoked when an executor has exited/terminated.
     */
    @Override
    public void executorLost(
            final SchedulerDriver driver,
            final Protos.ExecutorID executorId,
            final Protos.SlaveID slaveId,
            final int status) {
        System.out.println("Executor lost!");
    }

    /**
     * Invoked when there is an unrecoverable error in the scheduler or scheduler driver.
     */
    @Override
    public void error(final SchedulerDriver driver, final String message) {
        System.out.println("Error: " + message);
    }

    /**
     * Indicate whether status is a terminal state. If a task is in a terminal state it
     * implies that the task is no longer running on the cluster.
     * @param taskStatus status of the task
     * @return is the task in a terminal state
     */
    protected boolean isTerminal(final Protos.TaskStatus taskStatus) {
        switch (taskStatus.getState()) {
            case TASK_FINISHED:
            case TASK_FAILED:
            case TASK_KILLED:
            case TASK_LOST:
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
    protected boolean isActive(final Protos.TaskStatus taskStatus) {
        switch (taskStatus.getState()) {
            case TASK_RUNNING:
                return true;
            default:
                return false;
        }
    }
}
