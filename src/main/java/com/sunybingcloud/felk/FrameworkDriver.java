package com.sunybingcloud.felk;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sunybingcloud.felk.cli.CLIBuilder;
import com.sunybingcloud.felk.cli.UsageException;
import com.sunybingcloud.felk.config.Schema;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public final class FrameworkDriver {
    public static void main(final String[] args) {
        CommandLineParser commandLineParser = new DefaultParser();
        CommandLine commandLine;
        // Parsing command line arguments
        if (args.length == 0) {
            throw new UsageException();
        } else {
            try {
                commandLine = commandLineParser.parse(CLIBuilder.getOptions(), args);
            } catch (ParseException pe) {
                pe.printStackTrace();
                throw new UsageException(pe.getMessage());
            }
        }
        // Printing possible command line options and exiting.
        if (commandLine.hasOption("help")) {
            HelpFormatter helpFormatter = new HelpFormatter();
            helpFormatter.printHelp("java -jar build/libs/felk-1.0-SNAPSHOT.jar [-h]"
                    + " --config FILE " + "--master MASTER_LOCATION", CLIBuilder.getOptions());
            System.exit(0);
        }

        String configurationFilename = commandLine.getOptionValue("config");
        String masterLocation = commandLine.getOptionValue("master");

        Schema schema = null;
        if (configurationFilename != null) {
            File workloadFile = new File(configurationFilename);
            // Reading JSON from workloadFile and converting it to type
            // com.sunybingcloud.felk.config.Schema.
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

        // Indicating whether Felk has launched all the tasks.
        AtomicBoolean isDone = new AtomicBoolean(false);
        // Indicating whether all the tasks have completed execution, in which case we can shutdown
        // the framework.
        AtomicBoolean canShutdown = new AtomicBoolean(false);
        // Launching
        // Issue: Unnecessary duplicate object of FelkFramework created in the beginning.
        //  This was a workaround to solve the issue of the non-static inner classes.
        // TODO Fix this.
        FelkFramework framework = new FelkFramework().new Builder()
                .withSchema(schema)
                .withMasterLocation(masterLocation)
                .withDone(isDone)
                .withShutdown(canShutdown)
                .build();

        // Running the framework.
        System.out.println("Felk Starting...");
        new Thread(framework::execute).start();

        // Shutting down the framework once all tasks have been scheduled and all tasks have
        // completed execution.
        while (true) {
            if (isDone.get() && canShutdown.get()) {
                break;
            }
        }
        framework.shutdown();
        System.exit(0);
    }

    private FrameworkDriver() {

    }
}
