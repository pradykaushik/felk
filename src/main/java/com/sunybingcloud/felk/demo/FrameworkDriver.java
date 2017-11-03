package com.sunybingcloud.felk.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.fenzo.TaskScheduler;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.functions.Action1;
import com.netflix.fenzo.plugins.BinPackingFitnessCalculators;
import com.sunybingcloud.felk.cli.CLIBuilder;
import com.sunybingcloud.felk.cli.UsageException;
import com.sunybingcloud.felk.config.Schema;
import com.sunybingcloud.felk.config.task.Task;
import com.sunybingcloud.felk.sched.FelkScheduler;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class FrameworkDriver {

  private Collection<Task> taskQueue;
  private TaskScheduler taskScheduler;
  private MesosSchedulerDriver mesosSchedulerDriver;
  private String mesosMaster;
  private final AtomicReference<MesosSchedulerDriver> mesosSchedulerDriverRef = new AtomicReference<>();
  private Scheduler felkScheduler;

  public class LeaseRejectActionBuilder {
    private Action1<VirtualMachineLease> leaseRejectAction;
    private Map<String, Action1<VirtualMachineLease>> leaseRejectActionsDirectory = createLeaseRejectActions();

    /**
     * Defining different leaseRejectActions associated with a name. This is done in order for the user
     * to be able to specify the lease reject action in the configuration file.
     * @return a mapping from a name to a lease reject action.
     */
    private Map<String, Action1<VirtualMachineLease>> createLeaseRejectActions() {
      Map<String, Action1<VirtualMachineLease>> actions = new HashMap<>();
      actions.put("decline", virtualMachineLease ->
          mesosSchedulerDriverRef.get().declineOffer(virtualMachineLease.getOffer().getId()));
      return actions;
    }

    public LeaseRejectActionBuilder withLeaseRejectAction(String actionName) {
      leaseRejectAction = leaseRejectActionsDirectory.get(actionName);
      return this;
    }

    public Action1<VirtualMachineLease> build() {
      return leaseRejectAction;
    }
  }

  private final class Builder {
    private Map<String, VMTaskFitnessCalculator> fitnessCalculators = createFitnessCalculatorRegistry();
    private TaskScheduler taskScheduler;
    private TaskScheduler.Builder taskSchedulerBuilder = new TaskScheduler.Builder();
    private Collection<Task> taskQueue;

    private Map<String, VMTaskFitnessCalculator> createFitnessCalculatorRegistry() {
      Map<String, VMTaskFitnessCalculator> fcr = new HashMap<>();
      fcr.put("cpuBinPacker", BinPackingFitnessCalculators.cpuBinPacker);
      fcr.put("memBinPacker", BinPackingFitnessCalculators.memoryBinPacker);
      fcr.put("cpuMemBinPacker", BinPackingFitnessCalculators.cpuMemBinPacker);
      return fcr;
    }

    /**
     * Build the taskScheduler based on the schema.
     * @param schema The workload configuration schema.
     * @return Builder
     */
    public Builder withSchema(Schema schema) {
      // Input validation needs to have been done prior to this.
      taskQueue = schema.getTasks();
      // If no fitness calculator specified, then we're not going to be using one.
      String fc;
      if ((fc = schema.getFitnessCalculator()) != null) {
        taskSchedulerBuilder.withFitnessCalculator(fitnessCalculators.get(fc));
      }
      // If lease offer expiry not specified (or is 0), then not explicitly specifying (using default 120 seconds).
      if (schema.getLeaseOfferExpirySeconds() > 0) {
        taskSchedulerBuilder.withLeaseOfferExpirySecs(schema.getLeaseOfferExpirySeconds());
      }
      if (schema.isDisableShortfallEvaluation()) taskSchedulerBuilder.disableShortfallEvaluation();

      // Default lease reject action is 'decline'.
      // Right now, we have only one kind of action that can be specified.
      // Modify code when we have non-default lease reject actions specifiable.
      taskSchedulerBuilder.withLeaseRejectAction(new LeaseRejectActionBuilder()
          .withLeaseRejectAction(schema.getLeaseRejectAction()).build());
      return this;
    }

    public FrameworkDriver build() {
      return new FrameworkDriver(this);
    }
  }

  public FrameworkDriver() {}

  public FrameworkDriver(Builder builder) {
    taskQueue = builder.taskQueue;
    taskScheduler = builder.taskSchedulerBuilder.build();
    felkScheduler = new FelkScheduler(taskScheduler);
  }

  public FrameworkDriver buildSchedulerDriver(String mesosMaster) {
    // Creating FrameworkInfo
    Protos.FrameworkInfo frameworkInfo = Protos.FrameworkInfo.newBuilder()
        .setName("felk")
        .setUser("")
        .build();
    mesosSchedulerDriver = new MesosSchedulerDriver(felkScheduler, frameworkInfo, mesosMaster);
    mesosSchedulerDriverRef.set(mesosSchedulerDriver);
    return this;
  }

  public void execute() {
    // Need to launch felk to execute all the tasks using Fenzo's task scheduler.
    System.out.println("Executing...");
  }

  public static void main(String[] args) {
    CommandLineParser commandLineParser = new DefaultParser();
    CommandLine commandLine;
    // Parsing command line arguments
    if (args.length == 0 ) {
      throw new UsageException();
    } else {
      try {
        commandLine = commandLineParser.parse(CLIBuilder.options, args);
      } catch (ParseException pe) {
        pe.printStackTrace();
        throw new UsageException(pe.getMessage());
      }
    }
    // Printing possible command line options and exiting.
    if (commandLine.hasOption("help")) {
      HelpFormatter helpFormatter = new HelpFormatter();
      helpFormatter.printHelp("java -jar build/libs/felk-1.0-SNAPSHOT.jar [-h] --config FILE --master MASTER_LOCATION", CLIBuilder.options);
      System.exit(0);
    }

    String configurationFilename = commandLine.getOptionValue("config");
    String masterLocation = commandLine.getOptionValue("master");

    Schema schema = null;
    if (configurationFilename != null) {
      File workloadFile = new File(configurationFilename);
      // Reading JSON from workloadFile and converting it to type com.sunybingcloud.felk.config.Schema.
      try {
        schema = new ObjectMapper().readValue(workloadFile, Schema.class);
        System.out.println(schema);
      } catch (IOException ioe) {
        ioe.printStackTrace();
      }
    }

    if (masterLocation != null) {
      System.out.println("Master location: " + masterLocation);
    }

    if (schema == null) {
      System.out.println("Something went wrong!!");
      System.exit(0);
    }

    // Launching
    // Issue: Unnecessary duplicate object of FrameworkDriver created in the beginning.
    //  This was a workaround to solve the issue of the non-static inner classes.
    // TODO: Fix this.
    new FrameworkDriver().new Builder()
        .withSchema(schema)
        .build()
        .buildSchedulerDriver(masterLocation)
        .execute();
  }
}
