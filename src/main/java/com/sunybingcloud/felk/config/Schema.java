package com.sunybingcloud.felk.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sunybingcloud.felk.config.task.Task;

import java.util.ArrayList;
import java.util.Collection;

public final class Schema {
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

    /**
     * Retrieve the tasks submitted for execution.
     * @return tasks that are submitted for execution.
     */
    public Collection<Task> getTasks() {
        return tasks;
    }

    /**
     * Retrieve the fitness calculator that is being used, as specified by the user.
     * @return fitness calculator being used by Fenzo's task scheduler.
     */
    public String getFitnessCalculator() {
        return fitnessCalculator;
    }

    /**
     * Retrieve the lease offer expiry specified by the user. Any resource offers that remain
     * unused after this time would be rejected by Fenzo's task scheduler. This ensures that the
     * task scheduler will not hoard unuseful offers.
     * @return lease offer expiry (seconds).
     */
    public long getLeaseOfferExpirySeconds() {
        return leaseOfferExpirySeconds;
    }

    /**
     * Check whether shortfall evaluation for auto-scaling needs has been disabled. Shortfall
     * evaluation is useful for evaluating the actual resources needed to scale-up by the pending
     * tasks.
     * @return whether shortfall evaluation has been disabled.
     */
    public boolean isDisableShortfallEvaluation() {
        return disableShortfallEvaluation;
    }

    /**
     * Retrieve the name of the method that Fenzo's task scheduler will call to notify you that a
     * resource offer has been rejected. This name is then mapped to an action in
     * {@link com.sunybingcloud.felk.FelkFramework}
     * @return name of the lease reject action.
     */
    public String getLeaseRejectAction() {
        return leaseRejectAction;
    }

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
