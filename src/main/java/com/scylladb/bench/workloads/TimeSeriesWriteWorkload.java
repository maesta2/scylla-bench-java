package com.scylladb.bench.workloads;

import java.time.Instant;

/**
 * Time-series write workload: prepends newer rows (lower ck) to partitions.
 * Mirrors scylla-bench's TimeSeriesWrite workload.
 */
public class TimeSeriesWriteWorkload implements WorkloadGenerator {

    private final long pkStride;
    private final long pkOffset;
    private final long pkCount;
    private final long ckCount;
    private final Instant startTime;
    private final long periodNanos;

    private long pkPosition;
    private long pkGeneration;
    private long ckPosition;
    private boolean moveToNextPartition;

    public TimeSeriesWriteWorkload(int threadId, int threadCount,
            long pkCount, long basicPkOffset, long ckCount,
            Instant startTime, long rate) {
        this.pkStride = threadCount;
        this.pkOffset = threadId + basicPkOffset;
        this.pkCount = pkCount;
        this.ckCount = ckCount;
        this.startTime = startTime;
        long perPartition = pkCount / threadCount;
        this.periodNanos = perPartition == 0 ? 1 : (1_000_000_000L * perPartition) / rate;
        this.pkPosition = pkOffset - pkStride;
        this.pkGeneration = 0;
        this.ckPosition = 0;
        this.moveToNextPartition = false;
    }

    @Override
    public long nextPartitionKey() {
        pkPosition += pkStride;
        if (pkPosition >= pkCount + pkOffset) {
            pkPosition = pkOffset;
            ckPosition++;
            if (ckPosition >= ckCount) {
                pkGeneration++;
                ckPosition = 0;
            }
        }
        moveToNextPartition = false;
        return (pkPosition << 32) | pkGeneration;
    }

    @Override
    public long nextClusteringKey() {
        moveToNextPartition = true;
        long position = ckPosition + pkGeneration * ckCount;
        return -(startTime.toEpochMilli() * 1_000_000L + periodNanos * position);
    }

    @Override
    public boolean isPartitionDone() {
        return moveToNextPartition;
    }

    @Override
    public boolean isDone() {
        return false;
    }

    @Override
    public void restart() {
        /* time series is endless */ }
}
