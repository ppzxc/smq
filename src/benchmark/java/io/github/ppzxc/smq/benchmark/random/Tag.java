package io.github.ppzxc.smq.benchmark.random;

/** 프로듀서가 메시지마다 붙이는 식별 태그. 시드와 프로세스 안에서 순번이 유일하다. */
record Tag(long seed, int process, long seq) {

    /** {@link #toString()}의 역. 형식이 틀리면 {@link IllegalArgumentException}. */
    static Tag parse(String raw) {
        try {
            String[] parts = raw.split("/");
            if (parts.length != 3) {
                throw new IllegalArgumentException("태그 형식이 아니다: " + raw);
            }
            return new Tag(Long.parseLong(parts[0]), Integer.parseInt(parts[1]), Long.parseLong(parts[2]));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("태그 형식이 아니다: " + raw, e);
        }
    }

    @Override
    public String toString() {
        return seed + "/" + process + "/" + seq;
    }
}
