package io.github.ppzxc.smq;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

public final class SigkillWriterMain {

    private SigkillWriterMain() {}

    @PolicyException(
            rule = "PMD.SystemPrintln",
            reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings({"PMD.SystemPrintln", "PMD.CloseResource"})
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "This process intentionally runs until killed; closing would invalidate the crash fixture.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    public static void main(final String[] args) {
        final Smq smq;
        final Queue q;
        smq = Smq.sqlite(Path.of(args[0])).durability(Durability.PROCESS_CRASH).open();
        q = smq.declare("q", QueueOptions.defaults());
        System.out.println("READY");
        System.out.flush();

        for (long i = 0; ; i++) {
            final MessageId id;
            id = q.publish(OutgoingMessage.of(("m" + i).getBytes(StandardCharsets.UTF_8)));
            System.out.println(id.value());
            System.out.flush();
        }
    }
}
