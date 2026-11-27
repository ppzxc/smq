package io.github.ppzxc.smq.benchmark;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class JsonTest {

    private static String nestedJson() {
        final Map<String, Object> input;
        input = new LinkedHashMap<>();
        input.put("name", "a \"q\" \\ \n 한글");
        input.put("n", 12L);
        input.put("x", 1.5);
        input.put("ok", true);
        input.put("none", null);
        input.put("list", List.of(1L, 2L, 3L));
        input.put("obj", Map.of("k", "v"));
        return Json.write(input);
    }

    private static Stream<Arguments> nestedValues() {
        return Stream.of(
                Arguments.of("name", "a \"q\" \\ \n 한글"),
                Arguments.of("n", 12.0),
                Arguments.of("x", 1.5),
                Arguments.of("ok", true),
                Arguments.of("none", null),
                Arguments.of("list", List.of(1.0, 2.0, 3.0)),
                Arguments.of("obj", Map.of("k", "v")));
    }

    @Test
    void nestedJsonIsWrittenOnOneLine() {
        assertFalse(nestedJson().contains("\n"), "JSON should be written on one line.");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("nestedValues")
    void nestedJsonPreservesValue(final String key, final Object expected) {
        assertEquals(expected, ((Map<?, ?>) Json.parse(nestedJson())).get(key), "JSON should preserve " + key + ".");
    }

    @Test
    void nestedJsonPreservesNullKey() {
        assertTrue(
                ((Map<?, ?>) Json.parse(nestedJson())).containsKey("none"),
                "JSON should preserve the null-valued key.");
    }

    @Test
    void integralNumbersAreWrittenWithoutFraction() {
        assertEquals("[3,4.5]", Json.write(List.of(3.0, 4.5)), "Write should match the expected value.");
    }

    @Test
    void jsonRejectsIncompleteObject() {
        assertThrows(
                IllegalArgumentException.class,
                () -> Json.parse("{\"a\":"),
                "Parse should throw IllegalArgumentException.");
    }

    @Test
    void jsonRejectsTrailingInput() {
        assertThrows(
                IllegalArgumentException.class,
                () -> Json.parse("{} x"),
                "Parse should throw IllegalArgumentException.");
    }

    private static Report sampleReport() {
        return new Report(Map.of("os", "linux"), Map.of("a", 5.0, "b", 4.0), Map.of("a", List.of(9.0, 1.0, 5.0)));
    }

    private static Report oldReport() {
        return Report.fromJson("{\"environment\":{\"os\":\"x\"},\"entries\":{\"a\":5,\"b\":4}}");
    }

    @Test
    void reportJsonIncludesRepresentativeValue() {
        final Map<?, ?> json;
        json = (Map<?, ?>) Json.parse(sampleReport().toJson());
        assertEquals(5.0, ((Map<?, ?>) json.get("entries")).get("a"), "The representative value should be included.");
    }

    @Test
    void reportJsonIncludesSamples() {
        final Map<?, ?> json;
        json = (Map<?, ?>) Json.parse(sampleReport().toJson());
        assertEquals(
                List.of(9.0, 1.0, 5.0), ((Map<?, ?>) json.get("samples")).get("a"), "The samples should be included.");
    }

    @Test
    void reportJsonRoundTripPreservesEntries() {
        final Report report;
        report = sampleReport();
        assertEquals(
                report.entries(),
                Report.fromJson(report.toJson()).entries(),
                "Report entries should survive the JSON round trip.");
    }

    @Test
    void reportJsonRoundTripPreservesSamples() {
        final Report report;
        report = sampleReport();
        assertEquals(
                report.samples(),
                Report.fromJson(report.toJson()).samples(),
                "Report samples should survive the JSON round trip.");
    }

    @Test
    void prettyReportJsonRoundTrips() {
        final Report report;
        report = Report.fromJson(sampleReport().toJson());
        assertEquals(report, Report.fromJson(report.toPrettyJson()), "The pretty JSON should preserve the report.");
    }

    @Test
    void oldReportHasNoRecordedSamples() {
        assertEquals(Map.of(), oldReport().samples(), "Old reports should have no recorded samples.");
    }

    @Test
    void oldReportTreatsEntriesAsSingleSamples() {
        assertEquals(
                Map.of("a", List.of(5.0), "b", List.of(4.0)),
                oldReport().sampleMap(),
                "Old report entries should become single samples.");
    }

    @Test
    void reportWithoutSamplesWritesNoSamplesField() {
        assertFalse(
                new Report(Map.of(), Map.of("a", 1.0)).toJson().contains("samples"),
                "To json should not contain the expected value.");
    }

    private static Report committedBaseline() throws Exception {
        return Report.fromJson(Files.readString(Path.of("benchmark/baseline.json")));
    }

    @Test
    void committedBaselineHasEntries() throws Exception {
        assertFalse(committedBaseline().entries().isEmpty(), "The committed baseline should contain entries.");
    }

    @Test
    void committedBaselineHasNoSamples() throws Exception {
        assertTrue(committedBaseline().samples().isEmpty(), "The committed baseline should have no samples.");
    }
}
