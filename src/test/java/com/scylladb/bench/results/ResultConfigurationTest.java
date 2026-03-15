package com.scylladb.bench.results;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ResultConfigurationTest {

    @Test
    void defaultsAreCorrect() {
        ResultConfiguration cfg = new ResultConfiguration();
        assertFalse(cfg.measureLatency);
        assertEquals(1, cfg.latencyScale);
        assertEquals(3, cfg.histogramSignificantFigures);
        assertEquals(ResultConfiguration.LATENCY_TYPE_RAW, cfg.latencyTypeToPrint);
    }

    @Test
    void setHdrLatencyUnitsNs() {
        ResultConfiguration cfg = new ResultConfiguration();
        cfg.setHdrLatencyUnits("ns");
        assertEquals(1L, cfg.latencyScale);
    }

    @Test
    void setHdrLatencyUnitsUs() {
        ResultConfiguration cfg = new ResultConfiguration();
        cfg.setHdrLatencyUnits("us");
        assertEquals(1_000L, cfg.latencyScale);
    }

    @Test
    void setHdrLatencyUnitsMs() {
        ResultConfiguration cfg = new ResultConfiguration();
        cfg.setHdrLatencyUnits("ms");
        assertEquals(1_000_000L, cfg.latencyScale);
    }

    @Test
    void setHdrLatencyUnitsUnknownThrows() {
        ResultConfiguration cfg = new ResultConfiguration();
        assertThrows(IllegalArgumentException.class, () -> cfg.setHdrLatencyUnits("seconds"));
    }

    @Test
    void newHistogramHasExpectedSignificantFigures() {
        ResultConfiguration cfg = new ResultConfiguration();
        cfg.minHistogramValue = 1; // HdrHistogram requires lowestDiscernibleValue >= 1
        cfg.histogramSignificantFigures = 2;
        var h = cfg.newHistogram();
        assertNotNull(h);
        assertEquals(2, h.getNumberOfSignificantValueDigits());
    }

    @Test
    void setHistogramConfigurationScalesValues() {
        ResultConfiguration cfg = new ResultConfiguration();
        cfg.setHdrLatencyUnits("us"); // scale = 1000
        cfg.setHistogramConfiguration(1_000_000L, 10_000_000_000L, 2); // 1ms .. 10s in ns
        assertEquals(1_000L, cfg.minHistogramValue); // 1_000_000 / 1_000
        assertEquals(10_000_000L, cfg.maxHistogramValue); // 10_000_000_000 / 1_000
    }

    @Test
    void estimateHdrMemoryIsPositive() {
        ResultConfiguration cfg = new ResultConfiguration();
        cfg.minHistogramValue = 1; // HdrHistogram requires lowestDiscernibleValue >= 1
        cfg.concurrency = 4;
        int mem = cfg.estimateHdrMemory();
        assertTrue(mem > 0);
    }

    @Test
    void estimateHdrMemoryScalesWithConcurrency() {
        ResultConfiguration cfg1 = new ResultConfiguration();
        cfg1.minHistogramValue = 1;
        cfg1.concurrency = 1;

        ResultConfiguration cfg2 = new ResultConfiguration();
        cfg2.minHistogramValue = 1;
        cfg2.concurrency = 4;

        assertEquals(cfg1.estimateHdrMemory() * 4, cfg2.estimateHdrMemory());
    }
}
