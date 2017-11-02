package com.sunybingcloud.felk.config.task;

public final class TaskUtils {
  /**
   * Create a task identifier that is unique across tasks,
   *  and across all the instances of the same task.
   * @param taskName name of the task specified in the input schema.
   * @param instance instance to which this task corresponds to.
   * @return a unique identifier for the task.
   */
  static String createTaskID(String taskName, int instance) {
    // This is a very naive way of creating identifiers.
    return taskName + "-" + instance;
  }
}
