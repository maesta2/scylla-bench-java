package com.scylladb.bench.workloads;

import java.util.Random;

/**
 * Chooses partition and clustering keys randomly with a uniform distribution.
 * Mirrors scylla-bench's RandomUniform workload.
 */
public class UniformWorkload implements WorkloadGenerator {

    private final Random rng;
    private final long partitionCount;
    private final long partitionOffset;
    private final long clusteringRowCount;

    public UniformWorkload(int threadId, long partitionCount, long partitionOffset,
            long clusteringRowCount) {
        this.rng = new Random((long) System.nanoTime() * (threadId + 1));
        this.partitionCount = partitionCount;
        this.partitionOffset = partitionOffset;
        this.clusteringRowCount = clusteringRowCount;
    }

    @Override
    public long nextPartitionKey() {
        return Math.abs(rng.nextLong() % partitionCount) + partitionOffset;
    }

    @Override
    public long nextClusteringKey() {
        if (clusteringRowCount == 0)
            return 0;
        return Math.abs(rng.nextLong() % clusteringRowCount);
    }

    @Override
    public boolean isPartitionDone() {
        return false;
    }

    @Override
    public boolean isDone() {
        return false;
    }

    @Override
    public void restart() {
        /* uniform is endless */ }
}
