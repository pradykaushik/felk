package com.sunybingcloud.felk.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sunybingcloud.felk.cli.CLIBuilder;
import com.sunybingcloud.felk.cli.UsageException;
import com.sunybingcloud.felk.config.Schema;
import org.apache.commons.cli.*;

import java.io.File;
import java.io.IOException;

public class SampleScheduler {
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

    if (configurationFilename != null) {
      File workloadFile = new File(configurationFilename);
      // Reading JSON from workloadFile and converting it to type com.sunybingcloud.felk.config.Schema.
      try {
        Schema schema = new ObjectMapper().readValue(workloadFile, Schema.class);
        System.out.println(schema);
      } catch (IOException ioe) {
        ioe.printStackTrace();
      }
    }

    if (masterLocation != null) {
      System.out.println("Master location: " + masterLocation);
    }
  }
}
