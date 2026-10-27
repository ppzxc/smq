package io.github.ppzxc.smq.benchmark;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 하네스가 쓰는 최소 JSON. 객체, 배열, 문자열, 숫자, 불리언, null만 다룬다. 쓰기는 한 줄을 내고, 읽기는 숫자를 모두
 * {@link Double}로 돌려준다(정수 표현은 2^53까지 정확하다).
 */
public final class Json {

    private Json() {}

    public static String write(Object value) {
        StringBuilder sb = new StringBuilder();
        write(sb, value);
        return sb.toString();
    }

    private static void write(StringBuilder sb, Object v) {
        switch (v) {
            case null -> sb.append("null");
            case String s -> string(sb, s);
            case Boolean b -> sb.append(b);
            case Double d -> number(sb, d);
            case Float f -> number(sb, f.doubleValue());
            case Number n -> sb.append(n.longValue());
            case Map<?, ?> m -> {
                sb.append('{');
                boolean first = true;
                for (Map.Entry<?, ?> e : m.entrySet()) {
                    if (!first) sb.append(',');
                    first = false;
                    string(sb, String.valueOf(e.getKey()));
                    sb.append(':');
                    write(sb, e.getValue());
                }
                sb.append('}');
            }
            case List<?> l -> {
                sb.append('[');
                for (int i = 0; i < l.size(); i++) {
                    if (i > 0) sb.append(',');
                    write(sb, l.get(i));
                }
                sb.append(']');
            }
            case long[] a -> {
                sb.append('[');
                for (int i = 0; i < a.length; i++) {
                    if (i > 0) sb.append(',');
                    sb.append(a[i]);
                }
                sb.append(']');
            }
            default -> throw new IllegalArgumentException("지원하지 않는 JSON 값: " + v.getClass());
        }
    }

    private static void number(StringBuilder sb, double d) {
        if (Double.isNaN(d) || Double.isInfinite(d)) throw new IllegalArgumentException("유한하지 않은 수: " + d);
        if (d == Math.rint(d) && Math.abs(d) < 1e15) sb.append((long) d);
        else sb.append(d);
    }

    private static void string(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) sb.append(String.format("\\u%04x", (int) c));
                    else sb.append(c);
                }
            }
        }
        sb.append('"');
    }

    public static Object parse(String text) {
        Parser p = new Parser(text);
        p.skip();
        Object v = p.value();
        p.skip();
        if (p.pos != text.length()) throw p.error("끝에 남은 내용");
        return v;
    }

    private static final class Parser {
        final String s;
        int pos;

        Parser(String s) {
            this.s = s;
        }

        IllegalArgumentException error(String what) {
            return new IllegalArgumentException("JSON 오류(" + what + ") 위치 " + pos);
        }

        void skip() {
            while (pos < s.length() && Character.isWhitespace(s.charAt(pos))) pos++;
        }

        char peek() {
            if (pos >= s.length()) throw error("입력이 끝남");
            return s.charAt(pos);
        }

        void expect(char c) {
            if (peek() != c) throw error("'" + c + "' 필요");
            pos++;
        }

        Object value() {
            skip();
            char c = peek();
            return switch (c) {
                case '{' -> object();
                case '[' -> array();
                case '"' -> string();
                case 't', 'f', 'n' -> literal();
                default -> number();
            };
        }

        Map<String, Object> object() {
            expect('{');
            Map<String, Object> m = new LinkedHashMap<>();
            skip();
            if (peek() == '}') {
                pos++;
                return m;
            }
            while (true) {
                skip();
                String k = string();
                skip();
                expect(':');
                m.put(k, value());
                skip();
                if (peek() == ',') {
                    pos++;
                    continue;
                }
                expect('}');
                return m;
            }
        }

        List<Object> array() {
            expect('[');
            List<Object> l = new ArrayList<>();
            skip();
            if (peek() == ']') {
                pos++;
                return l;
            }
            while (true) {
                l.add(value());
                skip();
                if (peek() == ',') {
                    pos++;
                    continue;
                }
                expect(']');
                return l;
            }
        }

        String string() {
            expect('"');
            StringBuilder sb = new StringBuilder();
            while (true) {
                char c = peek();
                pos++;
                if (c == '"') return sb.toString();
                if (c != '\\') {
                    sb.append(c);
                    continue;
                }
                char e = peek();
                pos++;
                switch (e) {
                    case '"', '\\', '/' -> sb.append(e);
                    case 'n' -> sb.append('\n');
                    case 'r' -> sb.append('\r');
                    case 't' -> sb.append('\t');
                    case 'b' -> sb.append('\b');
                    case 'f' -> sb.append('\f');
                    case 'u' -> {
                        if (pos + 4 > s.length()) throw error("\\u 이스케이프");
                        sb.append((char) Integer.parseInt(s.substring(pos, pos + 4), 16));
                        pos += 4;
                    }
                    default -> throw error("알 수 없는 이스케이프");
                }
            }
        }

        Object literal() {
            if (s.startsWith("true", pos)) {
                pos += 4;
                return Boolean.TRUE;
            }
            if (s.startsWith("false", pos)) {
                pos += 5;
                return Boolean.FALSE;
            }
            if (s.startsWith("null", pos)) {
                pos += 4;
                return null;
            }
            throw error("알 수 없는 값");
        }

        Double number() {
            int start = pos;
            while (pos < s.length() && "+-0123456789.eE".indexOf(s.charAt(pos)) >= 0) pos++;
            if (start == pos) throw error("값 필요");
            try {
                return Double.parseDouble(s.substring(start, pos));
            } catch (NumberFormatException ex) {
                throw error("잘못된 수");
            }
        }
    }
}
