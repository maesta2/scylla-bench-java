package com.scylladb.bench.workloads;

/**
 * Sequentially visits all partitions and clustering rows.
 * The full row space is split evenly across concurrent threads.
 * Mirrors scylla-bench's SequentialVisitAll workload.
 */
public class SequentialWorkload implements WorkloadGenerator {

    private final long rowCount;
    private final long clusteringRowCount;
    private final long startPartition;
    private final long startClusteringRow;

    private long nextPartition;
    private long nextClusteringRow;
    private long processedRowCount;

    public SequentialWorkload(long rowOffset, long rowCount, long clusteringRowCount) {
        this.rowCount = rowCount;
        this.clusteringRowCount = clusteringRowCount;
        this.startPartition = rowOffset / clusteringRowCount;
        this.startClusteringRow = rowOffset % clusteringRowCount;
        this.nextPartition = startPartition;
        this.nextClusteringRow = startClusteringRow;
        this.processedRowCount = 0;
    }

    @Override
    public long nextPartitionKey() {
        if (nextClusteringRow < clusteringRowCount) {
            return nextPartition;
        }
        nextClusteringRow = 0;
        nextPartition++;
        return nextPartition;
    }

    @Override
    public long nextClusteringKey() {
        long ck = nextClusteringRow;
        nextClusteringRow++;
        processedRowCount++;
        return ck;
    }

    @Override
    public boolean isPartitionDone() {
        return nextClusteringRow == clusteringRowCount;
    }

    @Override
    public boolean isDone() {
        return processedRowCount >= rowCount;
    }

    @Override
    public void restart() {
        nextPartition = startPartition;
        nextClusteringRow = startClusteringRow;
        processedRowCount = 0;
    }
}
