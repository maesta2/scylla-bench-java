package com.scylladb.bench.workloads;

/**
 * Common interface for all benchmark workload generators.
 * Mirrors scylla-bench's workloads.Generator interface.
 */
public interface WorkloadGenerator {
    long nextPartitionKey();

    long nextClusteringKey();

    boolean isPartitionDone();

    boolean isDone();

    void restart();

    /**
     * Returns the next token range for scan workloads.
     * Throws UnsupportedOperationException for non-scan workloads.
     */
    default TokenRange nextTokenRange() {
        throw new UnsupportedOperationException(
                getClass().getSimpleName() + " does not support nextTokenRange()");
    }
}
