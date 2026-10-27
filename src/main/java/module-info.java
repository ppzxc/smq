/** smq. 저장소 SPI와 구현은 비공개 내부 계약이다(ADR-0002). */
module io.github.ppzxc.smq {
    requires java.sql;
    requires org.xerial.sqlitejdbc;

    exports io.github.ppzxc.smq;
}
