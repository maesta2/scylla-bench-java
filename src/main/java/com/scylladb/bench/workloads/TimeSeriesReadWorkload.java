package com.scylladb.bench.workloads;

import java.time.Instant;
import java.util.Random;

/**
 * Time-series read workload: reads rows from the range that has been written so
 * far.
 * Mirrors scylla-bench's TimeSeriesReader workload.
 */
public class TimeSeriesReadWorkload implements WorkloadGenerator {

    public static final int DISTRIBUTION_UNIFORM = 0;
    public static final int DISTRIBUTION_HNORMAL = 1;

    private final int threadId;
    private final int concurrency;
    private final long partitionCount;
    private final long partitionOffset;
    private final long clusteringRowCount;
    private final long writeRate;
    private final int distribution;
    private final Instant startTime;
    private final Random rng;

    public TimeSeriesReadWorkload(int threadId, int concurrency,
            long partitionCount, long partitionOffset,
            long clusteringRowCount, long writeRate,
            int distribution, Instant startTime) {
        this.threadId = threadId;
        this.concurrency = concurrency;
        this.partitionCount = partitionCount;
        this.partitionOffset = partitionOffset;
        this.clusteringRowCount = clusteringRowCount;
        this.writeRate = writeRate;
        this.distribution = distribution;
        this.startTime = startTime;
        this.rng = new Random(System.nanoTime() * (threadId + 1));
    }

    @Override
    public long nextPartitionKey() {
        return (Math.abs(rng.nextLong()) % partitionCount) + partitionOffset;
    }

    @Override
    public long nextClusteringKey() {
        long elapsed = Instant.now().toEpochMilli() - startTime.toEpochMilli(); // ms
        long totalWritten = (elapsed * writeRate) / 1000;

        if (totalWritten <= 0)
            return 0;

        long ckIndex;
        if (distribution == DISTRIBUTION_HNORMAL) {
            // Half-normal: latest rows are most likely
            double u = rng.nextGaussian();
            ckIndex = (long) (Math.abs(u) / 4.0 * totalWritten);
            ckIndex = Math.min(ckIndex, totalWritten - 1);
        } else {
            ckIndex = Math.abs(rng.nextLong()) % totalWritten;
        }

        long position = ckIndex + (ckIndex / clusteringRowCount) * clusteringRowCount;
        return -(startTime.toEpochMilli() * 1_000_000L
                + (1_000_000_000L * position / writeRate));
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
        /* endless */ }
}
