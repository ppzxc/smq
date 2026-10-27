package io.github.ppzxc.smq.benchmark.random;

import java.util.List;

/**
 * 어긴 불변식 하나.
 *
 * @param tags 관련 태그. 없으면 빈 목록
 * @param messageIds 관련 메시지 id. 큐가 다르면 detail이 밝힌다. 없으면 빈 목록
 * @param detail 사람이 읽는 설명
 */
record Violation(Invariant invariant, List<Tag> tags, List<Long> messageIds, String detail) {

    Violation {
        tags = List.copyOf(tags);
        messageIds = List.copyOf(messageIds);
    }
}
