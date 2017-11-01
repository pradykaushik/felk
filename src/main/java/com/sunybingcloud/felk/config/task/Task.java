package com.sunybingcloud.felk.config.task;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Task {

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
