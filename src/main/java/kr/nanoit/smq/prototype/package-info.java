/**
 * PROTOTYPE, 버려질 코드. 이슈 "공개 Java API의 모양"(ppzxc/smq#9)에 반응을 받으려고 만든 스텁이다.
 *
 * <p>prototype 스킬의 두 갈래(LOGIC의 HTML 상태 기계, UI 변형)는 이 질문에 맞지 않는다. 티켓이 "코드 스텁으로 만들어
 * 보고 반응을 받는다"고 했으므로 컴파일되는 Java 스텁과 사용 예제를 만들었다. 메서드 본문은 전부
 * {@code UnsupportedOperationException}이다.
 *
 * <p>구성:
 *
 * <ul>
 *   <li>이 패키지: 두 변형이 공유하는 타입. {@code Smq}, {@code Queue}의 공통 부분, 메시지, 결과, 예외.
 *   <li>{@code a}: 변형 A. 정산 상태를 기억하는 {@code Chunk} 핸들에 ack/nack/reject를 직접 건다.
 *   <li>{@code b}: 변형 B. {@code Chunk}는 불변 값이고, 정산은 {@code Settlement} 값을 만들어 {@code
 *       queue.settle(...)}에 넘긴다.
 * </ul>
 *
 * <p>각 변형의 {@code Example}이 반응할 대상이다. {@code // QUESTION:} 주석은 아직 정하지 않은 기본값이다.
 */
package kr.nanoit.smq.prototype;
