package io.github.ppzxc.smq.harness;

import io.github.ppzxc.smq.Durability;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.SplittableRandom;

final class SoakWorkload {

    record Round(int messageCount, int chunkSize, List<byte[]> payloads) {}

    private SoakWorkload() {}

    static Round round(final long seed, final int round) {
        final SplittableRandom random = new SplittableRandom(seed ^ (0x9E3779B97F4A7C15L * round));
        final int messageCount = 2 + random.nextInt(4);
        final int chunkSize = 1 + random.nextInt(messageCount);
        final int payloadSize = 8 + random.nextInt(25);
        final List<byte[]> payloads = new ArrayList<>(messageCount);
        for (int message = 0; message < messageCount; message++) {
            final byte[] payload = new byte[payloadSize];
            for (int index = 0; index < payload.length; index++) {
                payload[index] = (byte) random.nextInt(256);
            }
            payloads.add(payload);
        }
        return new Round(messageCount, chunkSize, List.copyOf(payloads));
    }

    static String digest(final SoakOptions options) {
        final MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (final NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is required by the Java platform", exception);
        }
        final String settings = options.seed() + ":"
                + options.budget() + ":"
                + options.delayMillis() + ":"
                + options.durabilities().stream().map(Durability::name).toList();
        digest.update(settings.getBytes(StandardCharsets.UTF_8));
        for (int round = 1; round <= options.budget(); round++) {
            final Round plan = round(options.seed(), round);
            digest.update((round + ":" + plan.messageCount() + ":" + plan.chunkSize() + ":")
                    .getBytes(StandardCharsets.UTF_8));
            for (final byte[] payload : plan.payloads()) {
                digest.update(payload);
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }
}
