package io.github.ppzxc.smq.storage;

import io.github.ppzxc.smq.MessageId;
import io.github.ppzxc.smq.Outcome;

public record Result(MessageId id, Outcome outcome) {
}
