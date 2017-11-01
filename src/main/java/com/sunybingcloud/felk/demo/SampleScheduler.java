package com.sunybingcloud.felk.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sunybingcloud.felk.config.Schema;

import java.io.File;
import java.io.IOException;

public class SampleScheduler {
  public static void main(String[] args) {
    File workloadFile = new File("sample_workload.json");
    // Reading JSON from workloadFile and converting it to type com.sunybingcloud.felk.config.Schema.
    try {
      Schema schema = new ObjectMapper().readValue(workloadFile, Schema.class);
      System.out.println(schema);
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
  }
}
