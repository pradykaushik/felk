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
   * Refers to the instance number for this task. Value would be in the range (0, {@code instances}].
   * This would act as a key to retrieve the corresponding TaskID.
   */
  private int instance;

  /**
   * TaskIds for each instance of the task.
   */
  Map<Integer, String> taskIDs = new HashMap<>();

  /**
   * Dummy constructor just to help with jackson object construction.
   */
  public Task() {}

  public Task(Task otherTask) {
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
  public void setInstance(int instance) {
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

  public double getDisk() {
    // To be implemented when we handle tasks that require disk space.
    return 0.0;
  }

  public double getNetworkMbps() {
    // To be implemented when we handle tasks that require network
    // bandwidth.
    return 0.0;
  }

  public String taskGroupName() {
    // To be implemented when we handle task groups.
    return null;
  }

  public Map<String, NamedResourceSetRequest> getCustomNamedResources() {
    // To be implemented when we deal with arbitrary resources.
    // Especially when we're dealing Watts as a resource.
    return null;
  }

  public List<? extends ConstraintEvaluator> getHardConstraints() {
    // To be implemented when we want to specify a set of constraints that
    // have to be satisfied by the host for task assignment.
    return null;
  }

  public List<? extends VMTaskFitnessCalculator> getSoftConstraints() {
    // To be implemented when we want some of the resource constraints to
    // not be hard constraints. Soft constraints don't have to be satisfied by
    // the host in order for task assignment.
    return null;
  }

  public AssignedResources getAssignedResources() {
    // To be implemented when we dealing with fault-tolerance.
    // Inform the current instance of Fenzo of the resource assignments made
    // by a prior instance of Fenzo (situation possible when framework restarts).
    return null;
  }

  public void setAssignedResources(AssignedResources assignedResources) {
    // To be implemented when we dealing with fault-tolerance.
    // Inform the current instance of Fenzo of the resource assignments made
    // by a prior instance of Fenzo (situation possible when framework restarts).
  }

  public Map<String, Double> getScalarRequests() {
    // To be implemented when we consider the Task to require scalar resources,
    // other than cpu, memory, networkMbps, and disk.
    return null;
  }

  public int getPorts() {
    // To be implemented when Task specification contains the requested
    // number of ports.
    return 0;
  }

  @Override
  public int hashCode() {
    return (getId() + name + cpu + ram + watts + classToWatts.toString() + image + command + instances).hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (! (o instanceof Task)) {
      return false;
    }
    // Different instances of the same task are considered different tasks.
    Task ot = (Task) o;
    return (getId() + name + cpu + ram + watts + classToWatts.toString() + image + command + instances)
            .equals(ot.getId() + ot.name + ot.cpu + ot.ram + ot.watts + ot.classToWatts.toString() +
                    ot.image + ot.command + instances);
  }

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
