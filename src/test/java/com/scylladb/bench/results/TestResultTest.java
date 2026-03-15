package com.scylladb.bench.results;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class TestResultTest {

    @Test
    void defaultState() {
        TestResult r = new TestResult();
        assertFalse(r.isFinal);
        assertEquals(Duration.ZERO, r.elapsedTime);
        assertEquals(0, r.operations);
        assertEquals(0, r.clusteringRows);
        assertEquals(0, r.errors);
        assertNull(r.criticalErrors);
    }

    @Test
    void addCriticalErrorInitializesList() {
        TestResult r = new TestResult();
        RuntimeException ex = new RuntimeException("error");
        r.addCriticalError(ex);
        assertNotNull(r.criticalErrors);
        assertEquals(1, r.criticalErrors.size());
        assertSame(ex, r.criticalErrors.get(0));
    }

    @Test
    void addCriticalErrorAppends() {
        TestResult r = new TestResult();
        r.addCriticalError(new RuntimeException("first"));
        r.addCriticalError(new RuntimeException("second"));
        assertEquals(2, r.criticalErrors.size());
    }
}
