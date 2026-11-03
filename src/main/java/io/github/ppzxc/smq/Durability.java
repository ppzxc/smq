package io.github.ppzxc.smq;

/** SQLite 저장소가 커밋한 결과를 어떤 장애까지 견디게 할지 고른다. 인메모리 저장소에는 해당 없다. */
public enum Durability {
    /**
     * 프로세스가 죽거나 전원이 나가거나 OS가 멈춰도 커밋한 메시지를 잃지 않는다. 커밋마다 fsync하므로 처리량은 더 낮다. 이 보장은 저장
     * 장치가 fsync를 지킬 때만 성립한다.
     */
    POWER_LOSS,
    /** 기본값. 프로세스가 죽어도 커밋한 메시지를 잃지 않지만, 전원 장애나 OS 멈춤 때는 마지막 커밋 몇 개를 잃을 수 있다. 대신 처리량이 높다. */
    PROCESS_CRASH
}
