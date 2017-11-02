package com.sunybingcloud.felk.config.task;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.VMTaskFitnessCalculator;

import java.util.List;
import java.util.Map;

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
  private int instances;

  private String taskID;

  public Task(String name, double cpu, int ram, double watts,
              Map<String, Double> classToWatts, String image,
              String command, int instances) {
    this.name = name;
    this.cpu = cpu;
    this.ram = ram;
    this.watts = watts;
    this.classToWatts = classToWatts;
    this.image = image;
    this.command = command;
    this.instances = instances;
    this.taskID = TaskUtils.createTaskID(this.name, this.instances);
  }

  /**
   * Retrieve the task identifier.
   * The task identifier needs to be unique across all tasks and also
   *  across task instances.
   * @return a task identifier
   */
  public String getId() {
    return taskID;
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
    builder.append(instances);
    builder.append("]");
    return builder.toString();
  }
}
