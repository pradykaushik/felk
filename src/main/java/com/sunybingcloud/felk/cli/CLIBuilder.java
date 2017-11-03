package com.sunybingcloud.felk.cli;

import org.apache.commons.cli.Options;

public final class CLIBuilder {
  public static Options options = new Options();

  static {
    // Defining command line options.
    options.addOption("m", "master", true, "Location of the Mesos master. <Hostname>:<Port>");
    options.addOption("c", "config", true, "Pathname of the configuration file");
    options.addOption("h", "help", false, "Usage information");
  }
}
