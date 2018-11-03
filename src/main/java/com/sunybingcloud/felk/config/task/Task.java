package com.sunybingcloud.felk.config.task;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.VMTaskFitnessCalculator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Task implements TaskRequest {
    @JsonProperty
    private String name;

    @JsonProperty
    private double cpu;

    @JsonProperty
    private int ram;

    @JsonProperty
    private double watts;

    @JsonProperty("class_to_watts")
    private Map<String, Double> classToWatts;

    @JsonProperty
    private String image;

    @JsonProperty("cmd")
    private String command;

    @JsonProperty("inst")
    private AtomicInteger instances;

    /**
     * Refers to the instance number for this task. Value would be in the range
     * (0, {@code instances}]. This would act as a key to retrieve the corresponding TaskID.
     */
    private int instance;

    /**
     * TaskIds for each instance of the task.
     */
    private Map<Integer, String> taskIDs = new HashMap<>();

    /**
     * Dummy constructor just to help with jackson object construction.
     */
    public Task() {

    }

    public Task(final Task otherTask) {
        name = otherTask.name;
        cpu = otherTask.cpu;
        ram = otherTask.ram;
        watts = otherTask.watts;
        classToWatts = otherTask.classToWatts;
        image = otherTask.image;
        command = otherTask.command;
        instances = otherTask.instances;
        instance = otherTask.instance;
        taskIDs = otherTask.taskIDs;
    }

    /**
     * Retrieve the name of the task.
     */
    public String getName() {
        return name;
    }

    /**
     * Retrieve the command that needs to be executed for the task.
     */
    public String getCommand() {
        return command;
    }

    /**
     * Retrieve the image tag
     */
    public String getImage() {
        return image;
    }

    /**
     * Retrieve all the TaskIds for all the instances of this task.
     */
    public Map<Integer, String> getTaskIDs() {
        return taskIDs;
    }

    /**
     * Retrieve the total number of instances of the task.
     */
    public AtomicInteger getInstances() {
        return instances;
    }

    /**
     * Retrieve the instance number for this task.
     */
    public int getInstance() {
        return instance;
    }

    /**
     * Set the instance number for this task.
     */
    public void setInstance(final int instance) {
        this.instance = instance;
    }

    /**
     * Retrieve the task identifier.
     * The task identifier needs to be unique across all tasks and also
     *  across task instances.
     * @return a task identifier
     */
    public String getId() {
        return taskIDs.get(instance);
    }

    /**
     * Retrieve the number of CPUs requested by the task.
     * @return number of CPUs requested by the task.
     */
    public double getCPUs() {
        return cpu;
    }

    /**
     * Retrieve the amount of memory requested by the task.
     * @return amount of memory (in MB) requested by the task.
     */
    public double getMemory() {
        return ram;
    }

    /**
     * Retrieve the amount of disk requested by the task.
     * @return amount of disk (in MG) requested by the task.
     */
    public double getDisk() {
        // To be implemented when we handle tasks that require disk space.
        return 0.0;
    }

    /**
     * Retrieve Network bandwidth requested by the task.
     * @return Mega bits per second in network bandwidth requested by the task.
     */
    public double getNetworkMbps() {
        // To be implemented when we handle tasks that require network
        // bandwidth.
        return 0.0;
    }

    /**
     * Retrieve the name of the task group that this task belongs to.
     * @return name of the task group that this task belongs to.
     */
    public String taskGroupName() {
        // To be implemented when we handle task groups.
        return null;
    }

    /**
     * Retrieve the list of custom named resource sets requested by the task.
     * @return list of named resource set requests.
     */
    public Map<String, NamedResourceSetRequest> getCustomNamedResources() {
        // To be implemented when we deal with arbitrary resources.
        // Especially when we're dealing Watts as a resource.
        return null;
    }

    /**
     * Retrieve a list of hard constraints that must be satisfied in order for the task to be
     * assigned to a host.
     * @return list of hard constraints.
     */
    public List<? extends ConstraintEvaluator> getHardConstraints() {
        // To be implemented when we want to specify a set of constraints that
        // have to be satisfied by the host for task assignment.
        return null;
    }

    /**
     * Retrieve a list of soft constraints that the task requests. Soft constraints need not be
     * satisfied for task assignment, but hosts that satisfy the soft constraints are given
     * priority during task assignment.
     * @return list of soft constraints.
     */
    public List<? extends VMTaskFitnessCalculator> getSoftConstraints() {
        // To be implemented when we want some of the resource constraints to
        // not be hard constraints. Soft constraints don't have to be satisfied by
        // the host in order for task assignment.
        return null;
    }

    /**
     * Retrieve the resources that were assigned to the task.
     * @return list of assigned resources.
     */
    public AssignedResources getAssignedResources() {
        // To be implemented when we dealing with fault-tolerance.
        // Inform the current instance of Fenzo of the resource assignments made
        // by a prior instance of Fenzo (situation possible when framework restarts).
        return null;
    }

    /**
     * Set the assigned resources for this task. These are resources other than the supported
     * resources such as CPUs, Memory, etc., and any scalar resources. This will specifically
     * need to be set for tasks that are being added into Fenzo as running from a prior instance
     * of the scheduler running, for example, after the framework hosting this instance of Fenzo
     * restarts. That is, they were not assigned by this instance of Fenzo.
     * @param assignedResources The assigned resources to set for this task.
     */
    public void setAssignedResources(final AssignedResources assignedResources) {
        // To be implemented when we dealing with fault-tolerance.
        // Inform the current instance of Fenzo of the resource assignments made
        // by a prior instance of Fenzo (situation possible when framework restarts).
    }

    /**
     * Retrieve the scalar resources requested by the task, other than cpu, mem, networkMbps, and
     * disk.
     * @return A {@link Map} of scalar resources requested, with resource name as the key and the
     * amount requested as the value.
     */
    public Map<String, Double> getScalarRequests() {
        // To be implemented when we consider the Task to require scalar resources,
        // other than cpu, memory, networkMbps, and disk.
        return null;
    }

    /**
     * Retrieve the number of ports that the task requests.
     * @return number of requested ports.
     */
    public int getPorts() {
        // To be implemented when Task specification contains the requested
        // number of ports.
        return 0;
    }

    /**
     * Concatenating the values of all the members to create a unique identifier for a task. The
     * unique identifier is not the TaskID but comprises of all the attributes (specified in the
     * workload config) of a task.
     * @return hashCode of the unique identifier string.
     */
    @Override
    public int hashCode() {
        return (getId() + name + cpu + ram + watts + classToWatts.toString()
                + image + command + instances).hashCode();
    }

    /**
     * If the unique identifiers for a task (not just the TaskID) of two tasks are the same, then
     * the tasks are the same. As mentioned in {@code hashCode}, the unique identifier is not
     * just the TaskID, but comprises of all the attributes (specified in the workload config) of a
     * task.
     * @param o another task object.
     * @return whether the tasks are the same.
     */
    @Override
    public boolean equals(final Object o) {
        if (!(o instanceof Task)) {
            return false;
        }
        // Different instances of the same task are considered different tasks.
        Task ot = (Task) o;
        return (getId() + name + cpu + ram + watts + classToWatts.toString()
                + image + command + instances) .equals(ot.getId() + ot.name + ot.cpu + ot.ram
                + ot.watts + ot.classToWatts.toString() + ot.image + ot.command + instances);
    }

    /**
     * String representation of the task that contains the information of the different
     * attributes of the task, specified in the workload config.
     * @return string representation of the task.
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("Task [");
        builder.append("name: ");
        builder.append(name);
        builder.append(", ");
        builder.append("cpu: ");
        builder.append(cpu);
        builder.append(", ");
        builder.append("ram: ");
        builder.append(ram);
        builder.append(", ");
        builder.append("watts: ");
        builder.append(watts);
        builder.append(", ");
        if (classToWatts != null) {
            builder.append("class_to_watts: ");
            builder.append(classToWatts.toString());
            builder.append(", ");
        }
        builder.append("image: ");
        builder.append(image);
        builder.append(", ");
        builder.append("cmd: ");
        builder.append(command);
        builder.append(", ");
        builder.append("inst: ");
        builder.append(instance);
        builder.append(", ");
        builder.append("total_insts: ");
        builder.append(instances);
        builder.append("]");
        return builder.toString();
    }
}
