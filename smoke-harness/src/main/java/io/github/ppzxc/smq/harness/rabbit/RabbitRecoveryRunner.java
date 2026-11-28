package io.github.ppzxc.smq.harness.rabbit;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.ShutdownSignalException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public final class RabbitRecoveryRunner {

    public record Observation(
            RabbitPlan.QueueKind queueKind,
            String queueName,
            String scenario,
            String classification,
            String correlationId,
            long queueStableForMs,
            boolean confirmObservedBeforeCrash,
            String confirmationObservedAt,
            boolean deliveryObservedBeforeCrash,
            String deliveryObservedAt,
            boolean ackSentBeforeCrash,
            boolean recovered,
            boolean redeliveredAfterRestart,
            boolean requestSentToSocket,
            String requestState,
            String requestStartedAt,
            String requestSentAt,
            String shutdownAt,
            String restartAt,
            String recoveryObservation,
            String recoveryObservationStatus,
            String ackSentAt,
            boolean ackApplicationIndependentlyObserved,
            String ackApplicationObservation,
            int readyMessagesAfterAckObservation) {}

    private final RabbitRecoveryOptions options;
    private String actualServerVersion = "unconnected";
    private String containerId = "unknown";
    private String containerImage = "unknown";
    private String dataMount = "unknown";
    private int restartCount;

    public RabbitRecoveryRunner(final RabbitRecoveryOptions options) {
        this.options = options;
    }

    public String actualServerVersion() {
        return this.actualServerVersion;
    }

    public String containerId() {
        return this.containerId;
    }

    public String containerImage() {
        return this.containerImage;
    }

    public String dataMount() {
        return this.dataMount;
    }

    public int restartCount() {
        return this.restartCount;
    }

    public void run(final List<Observation> observations) throws Exception {
        this.inspectContainer();
        this.awaitBrokerReady();
        final List<String> failures = new ArrayList<>();
        for (final RabbitPlan.QueueKind kind : RabbitPlan.QueueKind.values()) {
            final Observation confirmed = this.confirmedPublishSurvivesRestart(kind);
            observations.add(confirmed);
            if (!confirmed.recovered()) {
                failures.add(confirmed.recoveryObservation());
            }
            final Observation delivered = this.unackedDeliveryReturnsAfterRestart(kind);
            observations.add(delivered);
            if (!delivered.recovered()) {
                failures.add(delivered.recoveryObservation());
            }
            observations.add(this.inFlightPublishIsClassifiedUnknown(kind));
        }
        if (!failures.isEmpty()) {
            throw new IllegalStateException(String.join("; ", failures));
        }
    }

    private Observation confirmedPublishSurvivesRestart(final RabbitPlan.QueueKind kind) throws Exception {
        final String queue = this.queueName(kind, "confirmed");
        final String messageId = UUID.randomUUID().toString();
        final Connection publisherConnection = this.connect();
        final Channel channel = publisherConnection.createChannel();
        channel.confirmSelect();
        RabbitPlan.declareQueue(channel, queue, kind);
        Thread.sleep(RabbitPlan.RECOVERY_QUEUE_STABILIZATION_MILLIS);
        final AtomicBoolean returned = new AtomicBoolean();
        this.publish(channel, queue, messageId, returned);
        channel.waitForConfirmsOrDie(30_000);
        if (returned.get()) throw new IllegalStateException("confirmed recovery publish was unroutable");
        final String confirmedAt = Instant.now().toString();
        final String shutdownAt = Instant.now().toString();
        this.forceRestart();
        final String restartAt = Instant.now().toString();
        publisherConnection.abort();
        final RecoveryRead read = this.readAfterRestart(queue, kind, messageId, 20_000);
        return new Observation(
                kind,
                queue,
                "after-publisher-confirm",
                "CONFIRMED",
                messageId,
                RabbitPlan.RECOVERY_QUEUE_STABILIZATION_MILLIS,
                true,
                confirmedAt,
                false,
                "not-delivered",
                false,
                read.recovered(),
                read.redelivered(),
                false,
                "publisher confirm observed before broker stop",
                "not-applicable",
                "not-applicable",
                shutdownAt,
                restartAt,
                read.recovered()
                        ? "confirmed persistent message recovered with correlation ID " + read.correlationId()
                        : missingObservation(kind, "confirmed persistent message", read),
                read.recoveryObservationStatus(),
                read.ackSentAt(),
                read.ackAppliedIndependently(),
                read.ackApplicationObservation(),
                read.readyMessages());
    }

    private Observation unackedDeliveryReturnsAfterRestart(final RabbitPlan.QueueKind kind) throws Exception {
        final String queue = this.queueName(kind, "delivered");
        final String messageId = UUID.randomUUID().toString();
        final Connection connection = this.connect();
        boolean delivered = false;
        final Channel channel = connection.createChannel();
        channel.confirmSelect();
        RabbitPlan.declareQueue(channel, queue, kind);
        Thread.sleep(RabbitPlan.RECOVERY_QUEUE_STABILIZATION_MILLIS);
        final AtomicBoolean returned = new AtomicBoolean();
        this.publish(channel, queue, messageId, returned);
        channel.waitForConfirmsOrDie(30_000);
        if (returned.get()) throw new IllegalStateException("confirmed recovery publish was unroutable");
        final String confirmedAt = Instant.now().toString();
        final GetResponse response = awaitMessage(channel, queue);
        delivered = messageId.equals(response.getProps().getMessageId());
        if (!delivered) {
            throw new IllegalStateException("pre-crash delivery had an unexpected correlation ID");
        }
        if (response.getEnvelope().isRedeliver()) {
            throw new IllegalStateException("first delivery was unexpectedly marked redelivered");
        }
        final String deliveredAt = Instant.now().toString();
        final String shutdownAt = Instant.now().toString();
        this.forceRestart();
        final String restartAt = Instant.now().toString();
        connection.abort();
        final RecoveryRead read = this.readAfterRestart(queue, kind, messageId, 20_000);
        return new Observation(
                kind,
                queue,
                "delivery-before-settlement",
                "UNACKED_DELIVERY_RECOVERED",
                messageId,
                RabbitPlan.RECOVERY_QUEUE_STABILIZATION_MILLIS,
                true,
                confirmedAt,
                delivered,
                deliveredAt,
                false,
                read.recovered(),
                read.redelivered(),
                false,
                "message delivered before crash; no consumer ACK was sent",
                "not-applicable",
                "not-applicable",
                shutdownAt,
                restartAt,
                read.recovered()
                        ? "unacked delivery recovered; redelivered flag observed as " + read.redelivered()
                        : missingObservation(kind, "confirmed unacked delivery", read),
                read.recoveryObservationStatus(),
                read.ackSentAt(),
                read.ackAppliedIndependently(),
                read.ackApplicationObservation(),
                read.readyMessages());
    }

    private Observation inFlightPublishIsClassifiedUnknown(final RabbitPlan.QueueKind kind) throws Exception {
        final String queue = this.queueName(kind, "in-flight");
        final String messageId = UUID.randomUUID().toString();
        final Connection connection = this.connect();
        final Channel channel = connection.createChannel();
        channel.confirmSelect();
        RabbitPlan.declareQueue(channel, queue, kind);
        Thread.sleep(RabbitPlan.RECOVERY_QUEUE_STABILIZATION_MILLIS);
        this.docker("pause", this.options.container());
        final CountDownLatch requestSent = new CountDownLatch(1);
        final AtomicBoolean confirmed = new AtomicBoolean();
        final AtomicBoolean returned = new AtomicBoolean();
        final AtomicReference<Throwable> publishFailure = new AtomicReference<>();
        final AtomicReference<String> requestStartedAt = new AtomicReference<>("not-started");
        final AtomicReference<String> requestSentAt = new AtomicReference<>("not-sent");
        final Thread publisher = new Thread(
                () -> {
                    try {
                        requestStartedAt.set(Instant.now().toString());
                        this.publish(channel, queue, messageId, returned);
                        requestSentAt.set(Instant.now().toString());
                        requestSent.countDown();
                        confirmed.set(channel.waitForConfirms(30_000));
                    } catch (final Throwable throwable) {
                        publishFailure.set(throwable);
                        requestSent.countDown();
                    }
                },
                "rabbit-in-flight-publisher");
        publisher.setDaemon(true);
        publisher.start();
        final boolean requestWasSent = requestSent.await(10, TimeUnit.SECONDS);
        final String shutdownAt = Instant.now().toString();
        this.killPausedContainer();
        this.startAndAwaitContainer();
        final String restartAt = Instant.now().toString();
        publisher.join(5_000);
        if (publisher.isAlive()) {
            connection.abort();
            publisher.join(5_000);
        } else {
            connection.abort();
        }
        if (confirmed.get()) {
            throw new IllegalStateException("in-flight request unexpectedly received a confirm during broker stop");
        }
        if (!requestWasSent) {
            throw new IllegalStateException(
                    "publisher did not reach the confirm wait before broker stop", publishFailure.get());
        }
        final RecoveryRead read = this.readAfterRestart(queue, kind, messageId, 2_000);
        return new Observation(
                kind,
                queue,
                "publish-request-in-progress",
                "UNKNOWN",
                messageId,
                RabbitPlan.RECOVERY_QUEUE_STABILIZATION_MILLIS,
                false,
                "not-observed",
                false,
                "not-delivered",
                false,
                read.recovered(),
                read.redelivered(),
                true,
                "BASIC_PUBLISH_SENT|WAITING_FOR_CONFIRM",
                requestStartedAt.get(),
                requestSentAt.get(),
                shutdownAt,
                restartAt,
                read.recovered()
                        ? "unconfirmed request message was present after restart; outcome remains unknown"
                        : "unconfirmed request message was absent after restart; outcome remains unknown",
                read.recoveryObservationStatus(),
                read.ackSentAt(),
                read.ackAppliedIndependently(),
                read.ackApplicationObservation(),
                read.readyMessages());
    }

    private RecoveryRead readAfterRestart(
            final String queue, final RabbitPlan.QueueKind kind, final String expectedId, final long waitMillis)
            throws IOException, TimeoutException, InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(waitMillis);
        boolean queueObserved = false;
        boolean emptyReadObserved = false;
        boolean queueNotFoundObserved = false;
        while (System.nanoTime() < deadline) {
            String id = "";
            boolean redelivered = false;
            String ackSentAt = null;
            boolean ackApplied = false;
            String ackObservation = "consumer ACK was not sent";
            int readyMessages = 0;
            try (Connection connection = this.connect();
                    Channel channel = connection.createChannel()) {
                channel.queueDeclarePassive(queue);
                queueObserved = true;
                final GetResponse response = channel.basicGet(queue, false);
                if (response != null) {
                    id = response.getProps().getMessageId();
                    if (!expectedId.equals(id)) {
                        throw new IllegalStateException("recovered message correlation ID did not match request");
                    }
                    redelivered = response.getEnvelope().isRedeliver();
                    channel.basicAck(response.getEnvelope().getDeliveryTag(), false);
                    ackSentAt = Instant.now().toString();
                } else {
                    emptyReadObserved = true;
                    readyMessages = channel.queueDeclarePassive(queue).getMessageCount();
                }
            } catch (final IOException | TimeoutException exception) {
                queueNotFoundObserved |= isQueueNotFound(exception);
                Thread.sleep(100);
                continue;
            }

            if (ackSentAt != null) {
                try (Connection observer = this.connect();
                        Channel channel = observer.createChannel()) {
                    channel.queueDeclarePassive(queue);
                    final GetResponse afterAck = channel.basicGet(queue, false);
                    if (afterAck == null) {
                        readyMessages = channel.queueDeclarePassive(queue).getMessageCount();
                        ackApplied = readyMessages == 0;
                        ackObservation = ackApplied
                                ? "no message was redelivered after ACK send and consumer connection close"
                                : "ACK sent but ready-message count after connection close was " + readyMessages;
                    } else {
                        ackObservation = "message reappeared after ACK send and consumer connection close";
                        channel.basicAck(afterAck.getEnvelope().getDeliveryTag(), false);
                        readyMessages = channel.queueDeclarePassive(queue).getMessageCount();
                    }
                    channel.queueDelete(queue);
                }
                return new RecoveryRead(
                        true, redelivered, id, "RECOVERED", ackSentAt, ackApplied, ackObservation, readyMessages);
            }
            if (readyMessages > 0) {
                Thread.sleep(100);
                continue;
            }
            Thread.sleep(100);
        }
        final String status;
        if (emptyReadObserved && queueObserved) {
            status = "OBSERVED_EMPTY";
        } else if (queueNotFoundObserved) {
            status = "QUEUE_NOT_FOUND";
        } else {
            status = "RECOVERY_TIMEOUT";
        }
        final String ackObservation = "consumer ACK was not sent";
        return new RecoveryRead(false, false, "", status, null, false, ackObservation, 0);
    }

    private void publish(
            final Channel channel, final String queue, final String messageId, final AtomicBoolean returned)
            throws IOException {
        channel.addReturnListener((code, text, exchange, routingKey, properties, body) -> returned.set(true));
        final AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                .contentType("text/plain")
                .deliveryMode(2)
                .messageId(messageId)
                .build();
        channel.basicPublish("", queue, true, properties, messageId.getBytes(StandardCharsets.UTF_8));
    }

    private Connection connect() throws IOException, TimeoutException {
        final ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(this.options.host());
        factory.setPort(this.options.port());
        factory.setUsername(System.getenv().getOrDefault("RABBITMQ_USERNAME", "guest"));
        factory.setPassword(System.getenv().getOrDefault("RABBITMQ_PASSWORD", "guest"));
        factory.setAutomaticRecoveryEnabled(false);
        factory.setTopologyRecoveryEnabled(false);
        factory.setConnectionTimeout(10_000);
        final Connection connection = factory.newConnection("smq-rabbit-recovery-harness");
        final Object version = connection.getServerProperties().get("version");
        this.actualServerVersion = version == null ? "unknown" : version.toString();
        if (!RabbitPlan.SERVER_VERSION.equals(this.actualServerVersion)) {
            connection.abort();
            throw new IllegalStateException("RabbitMQ version mismatch: expected " + RabbitPlan.SERVER_VERSION
                    + ", got " + this.actualServerVersion);
        }
        return connection;
    }

    private void inspectContainer() throws IOException, InterruptedException {
        this.containerId = this.docker("inspect", "--format", "{{.Id}}", this.options.container())
                .trim();
        this.containerImage = this.docker("inspect", "--format", "{{.Config.Image}}", this.options.container())
                .trim();
        this.dataMount = this.docker(
                        "inspect",
                        "--format",
                        "{{range .Mounts}}{{.Destination}}:{{.Source}};{{end}}",
                        this.options.container())
                .trim();
        if (!RabbitPlan.SERVER_IMAGE.equals(this.containerImage)) {
            throw new IllegalStateException(
                    "container image must be " + RabbitPlan.SERVER_IMAGE + ", got " + this.containerImage);
        }
        if (!this.dataMount.contains("/var/lib/rabbitmq:")) {
            throw new IllegalStateException("container must mount persistent data at /var/lib/rabbitmq");
        }
        final String version = this.docker("exec", this.options.container(), "rabbitmqctl", "version")
                .trim();
        this.actualServerVersion = version;
        if (!RabbitPlan.SERVER_VERSION.equals(version)) {
            throw new IllegalStateException(
                    "RabbitMQ version mismatch: expected " + RabbitPlan.SERVER_VERSION + ", got " + version);
        }
    }

    private String queueName(final RabbitPlan.QueueKind kind, final String scenario) {
        return "smq-recovery-" + kind.queueType() + "-" + scenario + "-" + UUID.randomUUID();
    }

    private static String missingObservation(
            final RabbitPlan.QueueKind kind, final String what, final RecoveryRead read) {
        return switch (read.recoveryObservationStatus()) {
            case "OBSERVED_EMPTY" ->
                what + " was absent after an available queue returned empty reads for " + kind.queueType();
            case "QUEUE_NOT_FOUND" -> what + " queue was not found after restart for " + kind.queueType();
            default -> "recovery observation timed out before " + what + " could be checked for " + kind.queueType();
        };
    }

    private void forceRestart() throws IOException, InterruptedException {
        this.docker("kill", "--signal=KILL", this.options.container());
        this.docker("start", this.options.container());
        this.awaitBrokerReady();
        this.restartCount++;
    }

    private void killPausedContainer() throws IOException, InterruptedException {
        this.docker("kill", "--signal=KILL", this.options.container());
    }

    private void startAndAwaitContainer() throws IOException, InterruptedException {
        this.docker("start", this.options.container());
        this.awaitBrokerReady();
        this.restartCount++;
    }

    private void awaitBrokerReady() throws IOException, InterruptedException {
        for (int attempt = 0; attempt < 60; attempt++) {
            final Process process = new ProcessBuilder(
                            "docker", "exec", this.options.container(), "rabbitmq-diagnostics", "-q", "ping")
                    .redirectErrorStream(true)
                    .start();
            final String output = new String(process.getInputStream().readAllBytes());
            if (process.waitFor() == 0) {
                try {
                    this.connect().abort();
                    return;
                } catch (final TimeoutException | IOException exception) {
                    Thread.sleep(1000);
                    continue;
                }
            }
            Thread.sleep(1000);
        }
        throw new IllegalStateException(
                "RabbitMQ AMQP listener did not become ready after restarting the same container");
    }

    private String docker(final String... arguments) throws IOException, InterruptedException {
        final List<String> command = new ArrayList<>();
        command.add("docker");
        command.addAll(List.of(arguments));
        final Process process =
                new ProcessBuilder(command).redirectErrorStream(true).start();
        final String output = new String(process.getInputStream().readAllBytes());
        final int exit = process.waitFor();
        if (exit != 0) {
            throw new IOException(String.join(" ", command) + " failed with exit code " + exit + ": " + output.trim());
        }
        return output;
    }

    private static GetResponse awaitMessage(final Channel channel, final String queue)
            throws IOException, InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            final GetResponse response = channel.basicGet(queue, false);
            if (response != null) return response;
            Thread.sleep(10);
        }
        throw new IllegalStateException("confirmed message was not delivered");
    }

    private static boolean isQueueNotFound(final Throwable exception) {
        Throwable cause = exception;
        while (cause != null) {
            if (cause instanceof ShutdownSignalException signal
                    && signal.getReason() instanceof AMQP.Channel.Close close
                    && close.getReplyCode() == 404) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    private record RecoveryRead(
            boolean recovered,
            boolean redelivered,
            String correlationId,
            String recoveryObservationStatus,
            String ackSentAt,
            boolean ackAppliedIndependently,
            String ackApplicationObservation,
            int readyMessages) {}
}
