package io.github.ppzxc.smq.harness.rabbit;

import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public final class RabbitPlan {

    public static final String SERVER_VERSION = "4.4.1";
    public static final String SERVER_IMAGE = "rabbitmq:4.4.1-management";
    public static final String CLIENT_VERSION = "5.36.0";
    public static final long RECOVERY_QUEUE_STABILIZATION_MILLIS = 2_000;

    public enum QueueKind {
        CLASSIC("classic", Map.of()),
        QUORUM("quorum", Map.of("x-queue-type", "quorum", "x-quorum-initial-group-size", 1));

        private final String queueType;
        private final Map<String, Object> arguments;

        QueueKind(final String queueType, final Map<String, Object> arguments) {
            this.queueType = queueType;
            this.arguments = arguments;
        }

        public String queueType() {
            return this.queueType;
        }

        public Map<String, Object> arguments() {
            return this.arguments;
        }
    }

    private RabbitPlan() {}

    public static void declareQueue(final Channel channel, final String name, final QueueKind kind) throws IOException {
        channel.queueDeclare(name, true, false, false, kind.arguments());
    }

    public static void write(final Path output) throws IOException {
        Files.createDirectories(output.getParent());
        Files.writeString(output, json());
    }

    private static String json() {
        return """
                {
                  "schemaVersion":1,
                  "status":"PLANNED",
                  "broker":{"image":"rabbitmq:4.4.1-management","rabbitmqVersion":"4.4.1"},
                  "client":{"artifact":"com.rabbitmq:amqp-client","javaClientVersion":"5.36.0"},
                  "recoveryQueueStabilizationMillis":2000,
                  "completionCriteria":{
                    "publish":"routed persistent messages count complete after publisher confirm",
                    "routingErrors":"mandatory publishes with ReturnListener are errors",
                    "settlement":"consumer ACK sends are not publisher confirms"
                  },
                  "configurations":[
                    {"queueType":"classic","durable":true,"exclusive":false,"autoDelete":false,"arguments":{},"deliveryMode":2},
                    {"queueType":"quorum","durable":true,"exclusive":false,"autoDelete":false,"arguments":{"x-queue-type":"quorum","x-quorum-initial-group-size":1},"deliveryMode":2}
                  ],
                  "recoveryPoints":["after-publisher-confirm","after-delivery-before-ack","publish-request-in-progress"]
                }
                """;
    }
}
