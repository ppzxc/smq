package io.github.ppzxc.smq.harness.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;

public final class KafkaPlan {

    public static final String SERVER_IMAGE = "apache/kafka:4.4.0";

    private KafkaPlan() {}

    public static String clientVersion() {
        return KafkaProducer.class.getModule().getDescriptor().rawVersion().orElse("unknown");
    }

    public static String json() {
        return "{\"schemaVersion\":1"
                + ",\"serverImage\":\"" + SERVER_IMAGE + "\""
                + ",\"clientVersion\":\"" + clientVersion() + "\""
                + ",\"brokerMode\":\"single-node KRaft\""
                + ",\"sharedConfig\":{\"replicationFactor\":1"
                + ",\"min.insync.replicas\":\"1\",\"acks\":\"all\"}"
                + ",\"flushConfigurations\":["
                + "{\"name\":\"default\",\"flush.messages\":\"9223372036854775807\"},"
                + "{\"name\":\"flush-every-message\",\"flush.messages\":\"1\"}]"
                + ",\"requestInFlightDisposition\":\"UNKNOWN\"}"
                + System.lineSeparator();
    }
}
