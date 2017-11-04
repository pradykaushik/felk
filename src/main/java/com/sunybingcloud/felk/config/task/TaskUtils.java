package com.sunybingcloud.felk.config.task;

import org.apache.mesos.Protos;

import java.util.concurrent.atomic.AtomicInteger;

public final class TaskUtils {

  static final class TaskIdGenerator {
    /**
     * Create a task identifier that is unique across tasks,
     *  and across all the instances of the same task.
     * @param taskName name of the task specified in the input schema.
     * @param instance instance to which this task corresponds to.
     * @return a unique identifier for the task.
     */
    static String createTaskID(String taskName, AtomicInteger instance) {
      // This is a very naive way of creating identifiers.
      return "Felk-" + taskName + "-" + instance;
    }
  }

  public static final class TaskInfoGenerator {
    public static Protos.TaskInfo getTaskInfo(final Protos.SlaveID slaveID, final Task pendingTask) {
      Protos.TaskID protosTaskID = Protos.TaskID.newBuilder().setValue(pendingTask.getId()).build();
      return Protos.TaskInfo.newBuilder()
          .setName(pendingTask.getName())
          .setTaskId(protosTaskID)
          .setSlaveId(slaveID)
          .addResources(Protos.Resource.newBuilder()
            .setName("cpus")
            .setType(Protos.Value.Type.SCALAR)
            .setScalar(Protos.Value.Scalar.newBuilder().setValue(pendingTask.getCPUs())))
          .addResources(Protos.Resource.newBuilder()
            .setName("mem")
            .setType(Protos.Value.Type.SCALAR)
            .setScalar(Protos.Value.Scalar.newBuilder().setValue(pendingTask.getMemory())))
          .setCommand(Protos.CommandInfo.newBuilder().setValue(pendingTask.getCommand()).build())
          .build();
    }
  }
}
