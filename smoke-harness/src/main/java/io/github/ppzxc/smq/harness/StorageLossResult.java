package io.github.ppzxc.smq.harness;

import io.github.ppzxc.smq.Durability;
import java.time.Instant;

final class StorageLossResult {

    static final String JVM_TERMINATION = "JVM_TERMINATION";
    static final String SIMULATED_STORAGE_LOSS = "SIMULATED_STORAGE_LOSS";

    final String scenario;
    String faultType;
    final Durability durability;
    final String requestId;
    String completionRecord = "";
    String recoveryObservation = "not observed";
    String status = "PENDING";
    Instant shutdownAt;

    StorageLossResult(
            final String scenario, final String faultType, final Durability durability, final String requestId) {
        this.scenario = scenario;
        this.faultType = faultType;
        this.durability = durability;
        this.requestId = requestId;
    }
}
