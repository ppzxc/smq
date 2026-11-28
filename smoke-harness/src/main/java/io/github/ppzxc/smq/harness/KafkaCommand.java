package io.github.ppzxc.smq.harness;

import io.github.ppzxc.smq.harness.kafka.KafkaPlan;
import io.github.ppzxc.smq.harness.kafka.KafkaRunOptions;
import io.github.ppzxc.smq.harness.kafka.KafkaRunner;

public final class KafkaCommand {

    private KafkaCommand() {}

    public static int execute(final String[] args) {
        if (args.length == 2 && "plan".equals(args[1])) {
            System.out.println(KafkaPlan.json());
            return 0;
        }
        if (args.length >= 2 && "run".equals(args[1])) {
            final KafkaRunOptions options;
            try {
                options = KafkaRunOptions.parse(args);
            } catch (final IllegalArgumentException exception) {
                System.err.println(exception.getMessage());
                return 2;
            }
            return new KafkaRunner().run(options);
        }
        System.err.println("usage: kafka plan | kafka run [options]");
        return 2;
    }
}
