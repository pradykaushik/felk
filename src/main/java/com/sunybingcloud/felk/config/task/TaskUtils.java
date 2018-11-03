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
package com.sunybingcloud.felk.config.task;

import org.apache.mesos.Protos;

import java.util.Collection;

public final class TaskUtils {
    public static final class TaskIdGenerator {
        /**
         * Create a task identifier that is unique across tasks,
         *  and across all the instances of the same task.
         * @param taskName name of the task specified in the input schema.
         * @param instance instance to which this task corresponds to.
         * @return a unique identifier for the task.
         */
        static String createTaskID(final String taskName, final int instance) {
            // This is a very naive way of creating identifiers.
            return "Felk-" + taskName + "-" + instance;
        }

        /**
         * Assign IDs to all tasks. This method should be called when the tasks don't have an
         * identifier yet.
         */
        public static void assignTaskIds(final Collection<Task> tasks) {
            tasks.forEach(t -> {
                for (int i = t.getInstances().get(); i > 0; i--) {
                    t.getTaskIDs().put(i, createTaskID(t.getName(), i));
                }
            });
        }
    }

    public static final class TaskInfoGenerator {
        public static Protos.TaskInfo getTaskInfo(
                final Protos.SlaveID slaveID, final Task pendingTask) {
            Protos.TaskID protosTaskID = Protos.TaskID.newBuilder()
                    .setValue(pendingTask.getId()).build();
            return Protos.TaskInfo.newBuilder()
                    .setName(pendingTask.getName())
                    .setTaskId(protosTaskID)
                    .setSlaveId(slaveID)
                    .addResources(Protos.Resource.newBuilder()
                            .setName("cpus")
                            .setType(Protos.Value.Type.SCALAR)
                            .setScalar(Protos.Value.Scalar.newBuilder()
                                    .setValue(pendingTask.getCPUs())))
                    .addResources(Protos.Resource.newBuilder()
                            .setName("mem")
                            .setType(Protos.Value.Type.SCALAR)
                            .setScalar(Protos.Value.Scalar.newBuilder()
                                    .setValue(pendingTask.getMemory())))
                    .setCommand(Protos.CommandInfo.newBuilder()
                            .setValue(pendingTask.getCommand()).build())
                    .setContainer(Protos.ContainerInfo.newBuilder()
                            .setType(Protos.ContainerInfo.Type.DOCKER)
                            .setDocker(Protos.ContainerInfo.DockerInfo.newBuilder()
                                    .setImage(pendingTask.getImage())
                                    .setNetwork(Protos.ContainerInfo.DockerInfo.Network.BRIDGE))
                            .build())
                    .build();
        }
    }
}
