package io.github.ppzxc.smq.benchmark.random;

import java.util.Map;

/** 메시지의 바이트 동일성 비교 대상. payload는 체크섬으로, 헤더는 사용자 헤더 전체로 본다. */
record Content(long payloadChecksum, Map<String, String> headers) {

    Content {
        headers = Map.copyOf(headers);
    }
}
