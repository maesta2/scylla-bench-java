package com.scylladb.bench.results;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class MergedResultTest {

    private ResultConfiguration noLatency() {
        ResultConfiguration cfg = new ResultConfiguration();
        cfg.measureLatency = false;
        return cfg;
    }

    private ResultConfiguration withLatency() {
        ResultConfiguration cfg = new ResultConfiguration();
        cfg.measureLatency = true;
        cfg.minHistogramValue = 1; // HdrHistogram requires lowestDiscernibleValue >= 1
        return cfg;
    }

    @Test
    void initialStateIsZero() {
        MergedResult mr = new MergedResult(noLatency());
        assertEquals(0, mr.operations);
        assertEquals(0, mr.clusteringRows);
        assertEquals(0, mr.errors);
        assertEquals(0.0, mr.operationsPerSecond);
        assertNull(mr.criticalErrors);
    }

    @Test
    void addAccumulatesCounters() {
        MergedResult mr = new MergedResult(noLatency());

        TestResult r = new TestResult();
        r.elapsedTime = Duration.ofSeconds(1);
        r.operations = 100;
        r.clusteringRows = 200;
        r.errors = 3;

        mr.add(r);
        assertEquals(100, mr.operations);
        assertEquals(200, mr.clusteringRows);
        assertEquals(3, mr.errors);
    }

    @Test
    void addCalculatesOpsPerSecond() {
        MergedResult mr = new MergedResult(noLatency());

        TestResult r = new TestResult();
        r.elapsedTime = Duration.ofSeconds(2);
        r.operations = 200;
        r.clusteringRows = 400;
        r.errors = 0;

        mr.add(r);
        assertEquals(100.0, mr.operationsPerSecond, 1.0);
        assertEquals(200.0, mr.clusteringRowsPerSecond, 1.0);
    }

    @Test
    void addZeroElapsedTimeDoesNotAddThroughput() {
        MergedResult mr = new MergedResult(noLatency());

        TestResult r = new TestResult();
        r.elapsedTime = Duration.ZERO;
        r.operations = 50;

        mr.add(r);
        assertEquals(50, mr.operations);
        assertEquals(0.0, mr.operationsPerSecond);
    }

    @Test
    void addMergesCriticalErrors() {
        MergedResult mr = new MergedResult(noLatency());

        TestResult r = new TestResult();
        r.elapsedTime = Duration.ofSeconds(1);
        r.addCriticalError(new RuntimeException("boom"));

        mr.add(r);
        assertNotNull(mr.criticalErrors);
        assertEquals(1, mr.criticalErrors.size());
    }

    @Test
    void addMultipleResultsAccumulates() {
        MergedResult mr = new MergedResult(noLatency());

        for (int i = 0; i < 3; i++) {
            TestResult r = new TestResult();
            r.elapsedTime = Duration.ofSeconds(1);
            r.operations = 10;
            mr.add(r);
        }
        assertEquals(30, mr.operations);
    }

    @Test
    void histogramsInitializedWhenMeasureLatencyTrue() {
        MergedResult mr = new MergedResult(withLatency());
        assertNotNull(mr.rawLatency);
        assertNotNull(mr.coFixedLatency);
        assertNotNull(mr.rawReadLatency);
        assertNotNull(mr.rawWriteLatency);
    }

    @Test
    void histogramsNullWhenMeasureLatencyFalse() {
        MergedResult mr = new MergedResult(noLatency());
        assertNull(mr.rawLatency);
        assertNull(mr.coFixedLatency);
    }

    @Test
    void getDisplayLatencyReturnsRawByDefault() {
        MergedResult mr = new MergedResult(withLatency());
        assertSame(mr.rawLatency, mr.getDisplayLatency());
    }

    @Test
    void getDisplayLatencyReturnsCoFixedWhenConfigured() {
        ResultConfiguration cfg = withLatency();
        cfg.latencyTypeToPrint = ResultConfiguration.LATENCY_TYPE_CO_FIXED;
        MergedResult mr = new MergedResult(cfg);
        assertSame(mr.coFixedLatency, mr.getDisplayLatency());
    }
}
