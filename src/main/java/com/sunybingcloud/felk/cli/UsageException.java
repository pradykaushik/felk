package com.sunybingcloud.felk.cli;

public class UsageException extends RuntimeException {
  private String message;

  public UsageException(String message) {
    this.message = message;
  }

  public UsageException() {
    this.message = "Usage Error!" + "\n" + CLIBuilder.options.toString();
  }

  @Override
  public String getMessage() {
    return message;
  }
}
