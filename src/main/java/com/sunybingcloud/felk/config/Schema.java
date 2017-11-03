package com.sunybingcloud.felk.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sunybingcloud.felk.config.task.Task;

import java.util.ArrayList;
import java.util.Collection;

public class Schema {
  @JsonProperty
  private Collection<Task> tasks = new ArrayList<>();

  @JsonProperty("fitness_calc")
  private String fitnessCalculator;

  @JsonProperty("lease_offer_expiry_sec")
  private long leaseOfferExpirySeconds;

  @JsonProperty("disable_shortfall_eval")
  private boolean disableShortfallEvaluation;

  @JsonProperty("lease_reject_action")
  private String leaseRejectAction;

  public Collection<Task> getTasks() {return tasks;}
  public String getFitnessCalculator() {return fitnessCalculator;}
  public long getLeaseOfferExpirySeconds() {return leaseOfferExpirySeconds;}
  public boolean isDisableShortfallEvaluation() {return disableShortfallEvaluation;}
  public String getLeaseRejectAction() {return leaseRejectAction;}

  @Override
  public String toString() {
    try {
      return new ObjectMapper().writeValueAsString(this);
    } catch (JsonProcessingException jpe) {
      jpe.printStackTrace();
    }
    return "Schema: {}";
  }
}
