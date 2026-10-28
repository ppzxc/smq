package io.github.ppzxc.smq.benchmark;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JsonTest {

    @Test
    void writesAndParsesNestedValues() {
        Map<String, Object> in = new LinkedHashMap<>();
        in.put("name", "a \"q\" \\ \n 한글");
        in.put("n", 12L);
        in.put("x", 1.5);
        in.put("ok", true);
        in.put("none", null);
        in.put("list", List.of(1L, 2L, 3L));
        in.put("obj", Map.of("k", "v"));

        String text = Json.write(in);

        assertFalse(text.contains("\n"), "한 줄이어야 한다");
        @SuppressWarnings("unchecked")
        Map<String, Object> out = (Map<String, Object>) Json.parse(text);
        assertEquals("a \"q\" \\ \n 한글", out.get("name"));
        assertEquals(12.0, out.get("n"));
        assertEquals(1.5, out.get("x"));
        assertEquals(true, out.get("ok"));
        assertTrue(out.containsKey("none"));
        assertNull(out.get("none"));
        assertEquals(List.of(1.0, 2.0, 3.0), out.get("list"));
        assertEquals(Map.of("k", "v"), out.get("obj"));
    }

    @Test
    void integralNumbersAreWrittenWithoutFraction() {
        assertEquals("[3,4.5]", Json.write(List.of(3.0, 4.5)));
    }

    @Test
    void rejectsMalformedInput() {
        assertThrows(IllegalArgumentException.class, () -> Json.parse("{\"a\":"));
        assertThrows(IllegalArgumentException.class, () -> Json.parse("{} x"));
    }

    @Test
    void reportWritesSamplesNextToTheRepresentativeValue() {
        Report r = new Report(Map.of("os", "linux"), Map.of("a", 5.0, "b", 4.0), Map.of("a", List.of(9.0, 1.0, 5.0)));
        Map<?, ?> m = (Map<?, ?>) Json.parse(r.toJson());
        assertEquals(5.0, ((Map<?, ?>) m.get("entries")).get("a"));
        assertEquals(List.of(9.0, 1.0, 5.0), ((Map<?, ?>) m.get("samples")).get("a"));
        Report back = Report.fromJson(r.toJson());
        assertEquals(r.entries(), back.entries());
        assertEquals(r.samples(), back.samples());
        assertEquals(back, Report.fromJson(back.toPrettyJson()));
    }

    @Test
    void reportWithoutSamplesFieldIsReadAsSingleSamples() {
        Report old = Report.fromJson("{\"environment\":{\"os\":\"x\"},\"entries\":{\"a\":5,\"b\":4}}");
        assertEquals(Map.of(), old.samples());
        assertEquals(Map.of("a", List.of(5.0), "b", List.of(4.0)), old.sampleMap());
    }

    @Test
    void reportWithoutSamplesWritesNoSamplesField() {
        assertFalse(new Report(Map.of(), Map.of("a", 1.0)).toJson().contains("samples"));
    }

    @Test
    void committedBaselineIsStillReadable() throws Exception {
        Report b = Report.fromJson(Files.readString(Path.of("benchmark/baseline.json")));
        assertFalse(b.entries().isEmpty());
        assertTrue(b.samples().isEmpty());
    }
}
