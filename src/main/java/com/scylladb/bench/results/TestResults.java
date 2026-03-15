package com.scylladb.bench.results;

import org.HdrHistogram.Histogram;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Orchestrates result collection from all worker threads, prints periodic
 * partial results,
 * and produces the final summary. Mirrors scylla-bench's TestResults.
 */
public class TestResults {

    private static final String WITH_LATENCY_FMT = "%8s %7s %7s %6s %-8s %-8s %-8s %-8s %-8s %-8s %-8s%n";
    private static final String WITHOUT_LATENCY_FMT = "%8s %7s %7s %6s%n";

    private List<TestThreadResult> threadResults;
    private Instant startTime;
    private MergedResult totalResult;
    private final ResultConfiguration config;

    public TestResults(ResultConfiguration config) {
        this.config = config;
    }

    public void init(int concurrency) {
        threadResults = new ArrayList<>(concurrency);
        for (int i = 0; i < concurrency; i++) {
            threadResults.add(new TestThreadResult(config));
        }
    }

    public void setStartTime() {
        startTime = Instant.now();
    }

    public TestThreadResult getThreadResult(int idx) {
        return threadResults.get(idx);
    }

    public void printResultsHeader() {
        if (config.measureLatency) {
            System.out.printf(WITH_LATENCY_FMT,
                    "time", "ops/s", "rows/s", "errors",
                    "max", "99.9th", "99th", "95th", "90th", "median", "mean");
        } else {
            System.out.printf(WITHOUT_LATENCY_FMT, "time", "ops/s", "rows/s", "errors");
        }
    }

    /**
     * Collects results from all worker threads, printing partial results
     * periodically,
     * until all threads report their final result. Stores the merged final as
     * totalResult.
     */
    public void collectResults() {
        // Keep reading one result per thread per round until all are final
        boolean done = false;
        while (!done) {
            MergedResult round = new MergedResult(config);
            done = true;

            for (TestThreadResult ttr : threadResults) {
                try {
                    TestResult res = ttr.nextResult();
                    if (!res.isFinal) {
                        done = false;
                    } else {
                        // This thread finished; drain until we get the final (might already be it)
                        while (!res.isFinal) {
                            res = ttr.nextResult();
                        }
                    }
                    round.add(res);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }

            if (!done) {
                round.time = Duration.between(startTime, Instant.now());
                printPartialResult(round);
            } else {
                totalResult = round;
            }
        }
        // When done, use the final results (FullResult) from each thread for accurate
        // totals
        MergedResult totals = new MergedResult(config);
        for (TestThreadResult ttr : threadResults) {
            totals.add(ttr.fullResult);
        }
        totals.time = Duration.between(startTime, Instant.now());
        // Compute per-second rates using wallclock time
        double secs = totals.time.toNanos() / 1e9;
        if (secs > 0) {
            totals.operationsPerSecond = totals.operations / secs;
            totals.clusteringRowsPerSecond = totals.clusteringRows / secs;
        }
        totalResult = totals;
    }

    private void printPartialResult(MergedResult r) {
        long elapsed = r.time.getSeconds();
        long opsPerSec = (long) (r.operationsPerSecond);
        long rowsPerSec = (long) (r.clusteringRowsPerSecond);

        if (config.measureLatency) {
            Histogram h = r.getDisplayLatency();
            System.out.printf(WITH_LATENCY_FMT,
                    elapsed + "s",
                    opsPerSec, rowsPerSec, r.errors,
                    fmtLatency(h.getMaxValue()),
                    fmtLatency(h.getValueAtPercentile(99.9)),
                    fmtLatency(h.getValueAtPercentile(99.0)),
                    fmtLatency(h.getValueAtPercentile(95.0)),
                    fmtLatency(h.getValueAtPercentile(90.0)),
                    fmtLatency(h.getValueAtPercentile(50.0)),
                    fmtLatency((long) h.getMean()));
        } else {
            System.out.printf(WITHOUT_LATENCY_FMT,
                    elapsed + "s", opsPerSec, rowsPerSec, r.errors);
        }
    }

    public void printTotalResults() {
        System.out.println("\nResults");
        System.out.printf("Time (avg):\t\t%s%n", totalResult.time);
        System.out.printf("Total ops:\t\t%d%n", totalResult.operations);
        System.out.printf("Total rows:\t\t%d%n", totalResult.clusteringRows);
        if (totalResult.errors > 0) {
            System.out.printf("Total errors:\t\t%d%n", totalResult.errors);
        }
        System.out.printf("Operations/s:\t\t%.1f%n", totalResult.operationsPerSecond);
        System.out.printf("Rows/s:\t\t\t%.1f%n", totalResult.clusteringRowsPerSecond);

        if (config.measureLatency) {
            printLatency("raw latency", totalResult.rawLatency);
            printLatency("c-o fixed latency", totalResult.coFixedLatency);

            if (totalResult.rawReadLatency != null && totalResult.rawReadLatency.getTotalCount() > 0) {
                printLatency("raw read latency", totalResult.rawReadLatency);
                printLatency("c-o fixed read latency", totalResult.coFixedReadLatency);
            }
            if (totalResult.rawWriteLatency != null && totalResult.rawWriteLatency.getTotalCount() > 0) {
                printLatency("raw write latency", totalResult.rawWriteLatency);
                printLatency("c-o fixed write latency", totalResult.coFixedWriteLatency);
            }
        }

        if (totalResult.criticalErrors != null && !totalResult.criticalErrors.isEmpty()) {
            System.err.println("\nCritical errors:");
            for (Exception e : totalResult.criticalErrors) {
                System.err.println("  " + e.getMessage());
            }
        }
    }

    private void printLatency(String name, Histogram h) {
        if (h == null)
            return;
        long scale = config.latencyScale;
        System.out.printf("%s:%n", name);
        System.out.printf("  max:\t\t%s%n", fmtNs(h.getMaxValue() * scale));
        System.out.printf("  99.9th:\t%s%n", fmtNs(h.getValueAtPercentile(99.9) * scale));
        System.out.printf("  99th:\t\t%s%n", fmtNs(h.getValueAtPercentile(99.0) * scale));
        System.out.printf("  95th:\t\t%s%n", fmtNs(h.getValueAtPercentile(95.0) * scale));
        System.out.printf("  90th:\t\t%s%n", fmtNs(h.getValueAtPercentile(90.0) * scale));
        System.out.printf("  median:\t%s%n", fmtNs(h.getValueAtPercentile(50.0) * scale));
        System.out.printf("  mean:\t\t%s%n", fmtNs((long) (h.getMean() * scale)));
    }

    private String fmtLatency(long scaledVal) {
        long ns = scaledVal * config.latencyScale;
        return fmtNs(ns);
    }

    /** Format nanoseconds as a human-readable duration string. */
    private static String fmtNs(long ns) {
        if (ns >= 1_000_000_000L)
            return String.format("%.2fs", ns / 1e9);
        if (ns >= 1_000_000L)
            return String.format("%.2fms", ns / 1e6);
        if (ns >= 1_000L)
            return String.format("%.2fμs", ns / 1e3);
        return ns + "ns";
    }

    public int getFinalStatus() {
        if (totalResult != null && totalResult.criticalErrors != null
                && !totalResult.criticalErrors.isEmpty()) {
            return 1;
        }
        return 0;
    }
}
