package com.scylladb.bench.results;

import org.HdrHistogram.Histogram;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Merged/aggregated result across all threads for a single reporting interval
 * or the final summary.
 */
public class MergedResult {

    public Duration time = Duration.ZERO;
    public long operations;
    public long clusteringRows;
    public long errors;
    public double operationsPerSecond;
    public double clusteringRowsPerSecond;
    public List<Exception> criticalErrors;

    public Histogram rawLatency;
    public Histogram coFixedLatency;
    public Histogram rawReadLatency;
    public Histogram coFixedReadLatency;
    public Histogram rawWriteLatency;
    public Histogram coFixedWriteLatency;

    private final ResultConfiguration config;

    public MergedResult(ResultConfiguration config) {
        this.config = config;
        if (config.measureLatency) {
            rawLatency = config.newHistogram();
            coFixedLatency = config.newHistogram();
            rawReadLatency = config.newHistogram();
            coFixedReadLatency = config.newHistogram();
            rawWriteLatency = config.newHistogram();
            coFixedWriteLatency = config.newHistogram();
        }
    }

    public void add(TestResult r) {
        time = time.plus(r.elapsedTime);
        operations += r.operations;
        clusteringRows += r.clusteringRows;
        errors += r.errors;
        if (r.elapsedTime.toNanos() > 0) {
            double secs = r.elapsedTime.toNanos() / 1e9;
            operationsPerSecond += r.operations / secs;
            clusteringRowsPerSecond += r.clusteringRows / secs;
        }
        if (r.criticalErrors != null) {
            if (criticalErrors == null)
                criticalErrors = new ArrayList<>();
            criticalErrors.addAll(r.criticalErrors);
        }
        if (config.measureLatency) {
            mergeHistogram(rawLatency, r.rawLatency);
            mergeHistogram(coFixedLatency, r.coFixedLatency);
            mergeHistogram(rawReadLatency, r.rawReadLatency);
            mergeHistogram(coFixedReadLatency, r.coFixedReadLatency);
            mergeHistogram(rawWriteLatency, r.rawWriteLatency);
            mergeHistogram(coFixedWriteLatency, r.coFixedWriteLatency);
        }
    }

    private void mergeHistogram(Histogram dest, Histogram src) {
        if (dest == null || src == null)
            return;
        dest.add(src);
    }

    public Histogram getDisplayLatency() {
        return config.latencyTypeToPrint == ResultConfiguration.LATENCY_TYPE_CO_FIXED
                ? coFixedLatency
                : rawLatency;
    }
}
