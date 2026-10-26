package io.github.ppzxc.smq;

/** SQLite 저장소가 커밋을 디스크에 얼마나 단단히 남길지 고른다. 인메모리 저장소에는 해당 없다. */
public enum Durability {
    /**
     * 프로세스가 죽거나 전원이 나가거나 OS가 멈춰도 커밋한 메시지를 잃지 않는다
     * ({@code synchronous=FULL}). 기본값이고, 처리량은 더 낮다.
     */
    POWER_LOSS,
    /**
     * 프로세스가 죽어도 커밋한 메시지를 잃지 않지만, 전원 장애나 OS 멈춤 때는 마지막 커밋 몇 개를 잃을 수 있다
     * ({@code synchronous=NORMAL}, WAL). 대신 처리량이 높다.
     */
    PROCESS_CRASH
}
