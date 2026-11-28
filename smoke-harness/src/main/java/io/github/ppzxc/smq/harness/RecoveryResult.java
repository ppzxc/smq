package io.github.ppzxc.smq.harness;

import io.github.ppzxc.smq.Durability;
import java.time.Instant;

final class RecoveryResult {

    final Durability durability;
    final String scenario;
    final String requestId;
    final String correlationId;
    String classification = "UNKNOWN";
    String requestStarted = "";
    String observedResponse = "";
    String recoveryObservation = "not observed";
    Instant shutdownAt;

    RecoveryResult(
            final Durability durability, final String scenario, final String requestId, final String correlationId) {
        this.durability = durability;
        this.scenario = scenario;
        this.requestId = requestId;
        this.correlationId = correlationId;
    }
}
