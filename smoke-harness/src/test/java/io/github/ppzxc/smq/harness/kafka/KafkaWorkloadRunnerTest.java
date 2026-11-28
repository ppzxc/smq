package io.github.ppzxc.smq.harness.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class KafkaWorkloadRunnerTest {

    @Test
    void reportsDeliveryLatencyInNanoseconds() {
        assertEquals(1_000_000L, KafkaWorkloadRunner.deliveryLatencyNanos(100L, 101L));
    }

    @Test
    void clampsClockAdjustmentToZeroLatency() {
        assertEquals(0L, KafkaWorkloadRunner.deliveryLatencyNanos(101L, 100L));
    }

    @Test
    void reportsGroupOffsetsByTopicAndPartition() {
        assertEquals(
                Map.of("orders:0", 7L),
                KafkaClientSupport.offsetObservation(
                        Map.of(new TopicPartition("orders", 0), new OffsetAndMetadata(7L))));
    }
}
