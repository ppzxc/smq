package kr.nanoit.smq.prototype;

/** 정산과 연장의 메시지별 결과. */
public enum Outcome {
    /** 반영됐다. */
    OK,
    /** 낡은 연산. 메시지가 이미 다른 리스로 넘어갔거나 큐에 없어서 효력이 없다. 실패가 아니다. */
    STALE
}
