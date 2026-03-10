package com.scylladb.bench.results;

import org.HdrHistogram.Histogram;

import java.time.Duration;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Tracks benchmark results for a single worker thread.
 * Mirrors scylla-bench's TestThreadResult with a Go-channel equivalent
 * (BlockingQueue).
 */
public class TestThreadResult {

    /** Accumulated totals over the entire test lifetime. Final=true. */
    public final TestResult fullResult;
    /** Delta result for the current reporting interval. */
    private TestResult partialResult;
    /** Channel-equivalent: partial results + final result are put here. */
    public final LinkedBlockingQueue<TestResult> queue = new LinkedBlockingQueue<>(100_000);

    /**
     * Set to true when the reporter detects a critical error and all threads should
     * stop.
     */
    public static volatile boolean globalErrorFlag = false;

    private final ResultConfiguration config;

    public TestThreadResult(ResultConfiguration config) {
        this.config = config;
        this.fullResult = newResult(true);
        this.partialResult = newResult(false);
    }

    private TestResult newResult(boolean isFinal) {
        TestResult r = new TestResult();
        r.isFinal = isFinal;
        if (config.measureLatency) {
            r.rawLatency = config.newHistogram();
            r.coFixedLatency = config.newHistogram();
            r.rawReadLatency = config.newHistogram();
            r.coFixedReadLatency = config.newHistogram();
            r.rawWriteLatency = config.newHistogram();
            r.coFixedWriteLatency = config.newHistogram();
        }
        return r;
    }

    public void incOps() {
        fullResult.operations++;
        partialResult.operations++;
    }

    public void addRows(long n) {
        fullResult.clusteringRows += n;
        partialResult.clusteringRows += n;
    }

    public void incRows() {
        addRows(1);
    }

    public void incErrors() {
        fullResult.errors++;
        partialResult.errors++;
    }

    public void submitCriticalError(Exception e) {
        fullResult.addCriticalError(e);
        globalErrorFlag = true;
    }

    public void recordRawLatency(Duration d) {
        if (!config.measureLatency)
            return;
        long val = clamp(d.toNanos() / config.latencyScale);
        fullResult.rawLatency.recordValue(val);
        partialResult.rawLatency.recordValue(val);
    }

    public void recordCoFixedLatency(Duration d) {
        if (!config.measureLatency)
            return;
        long val = clamp(d.toNanos() / config.latencyScale);
        fullResult.coFixedLatency.recordValue(val);
        partialResult.coFixedLatency.recordValue(val);
    }

    public void recordReadRawLatency(Duration d) {
        if (!config.measureLatency)
            return;
        long val = clamp(d.toNanos() / config.latencyScale);
        fullResult.rawReadLatency.recordValue(val);
        partialResult.rawReadLatency.recordValue(val);
    }

    public void recordReadCoFixedLatency(Duration d) {
        if (!config.measureLatency)
            return;
        long val = clamp(d.toNanos() / config.latencyScale);
        fullResult.coFixedReadLatency.recordValue(val);
        partialResult.coFixedReadLatency.recordValue(val);
    }

    public void recordWriteRawLatency(Duration d) {
        if (!config.measureLatency)
            return;
        long val = clamp(d.toNanos() / config.latencyScale);
        fullResult.rawWriteLatency.recordValue(val);
        partialResult.rawWriteLatency.recordValue(val);
    }

    public void recordWriteCoFixedLatency(Duration d) {
        if (!config.measureLatency)
            return;
        long val = clamp(d.toNanos() / config.latencyScale);
        fullResult.coFixedWriteLatency.recordValue(val);
        partialResult.coFixedWriteLatency.recordValue(val);
    }

    /** Enqueue the current partial snapshot and reset for next interval. */
    public void flushPartialResult(Duration elapsed) {
        partialResult.elapsedTime = elapsed;
        queue.offer(partialResult);
        partialResult = newResult(false);
    }

    /** Enqueue the final total result (marks end of this thread's work). */
    public void flushFinalResult(Duration elapsed) {
        fullResult.elapsedTime = elapsed;
        fullResult.isFinal = true;
        queue.offer(fullResult);
    }

    private long clamp(long val) {
        return Math.min(val, config.maxHistogramValue);
    }

    /** Block and get the next result from this thread. */
    public TestResult nextResult() throws InterruptedException {
        return queue.take();
    }
}
