package com.sunybingcloud.felk.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.sunybingcloud.felk.config.task.Task;

import java.util.ArrayList;
import java.util.Collection;

public class Schema {
  @JsonProperty
  private Collection<Task> tasks = new ArrayList<>();

  public Collection<Task> getTasks() {return tasks;}

  @Override
  public String toString() {
    return "Schema: {'tasks': " + tasks.toString() + "}";
  }
}
