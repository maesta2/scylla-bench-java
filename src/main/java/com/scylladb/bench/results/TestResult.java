package com.scylladb.bench.results;

import org.HdrHistogram.Histogram;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Single benchmark result snapshot - either a per-interval partial result or
 * the final total.
 */
public class TestResult {

    public boolean isFinal;
    public Duration elapsedTime = Duration.ZERO;
    public long operations;
    public long clusteringRows;
    public long errors;
    public List<Exception> criticalErrors;

    // Overall latency histograms
    public Histogram rawLatency;
    public Histogram coFixedLatency;

    // Mixed-mode per-operation-type histograms
    public Histogram rawReadLatency;
    public Histogram coFixedReadLatency;
    public Histogram rawWriteLatency;
    public Histogram coFixedWriteLatency;

    public void addCriticalError(Exception e) {
        if (criticalErrors == null)
            criticalErrors = new ArrayList<>();
        criticalErrors.add(e);
    }
}
