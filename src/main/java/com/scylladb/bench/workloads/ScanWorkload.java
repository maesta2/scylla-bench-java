package com.scylladb.bench.workloads;

/**
 * Scan workload: splits the token space into sub-ranges and iterates over them.
 * Mirrors scylla-bench's RangeScan workload.
 *
 * The algorithm follows the efficient range scan technique where the Murmur3
 * token space [-2^63, 2^63-1] is divided into equal-width sequential ranges.
 */
public class ScanWorkload implements WorkloadGenerator {

    private final int rangeCount;
    private final int startOffset;
    private final int count;

    private int current;
    private int processed;

    public ScanWorkload(int rangeCount, int startOffset, int count) {
        this.rangeCount = rangeCount;
        this.startOffset = startOffset;
        this.count = count;
        this.current = startOffset;
        this.processed = 0;
    }

    @Override
    public TokenRange nextTokenRange() {
        int idx = current;
        current++;
        if (current >= startOffset + count) {
            current = startOffset; // wrap for multi-iteration
        }
        processed++;
        return computeRange(idx, rangeCount);
    }

    /**
     * Computes the [start, end] token range for the given range index out of total
     * ranges.
     * Divides [-2^63, 2^63-1] into equal-width segments.
     */
    /**
     * Divides the Murmur3 token space [-2^63, 2^63-1] into {@code total} equal
     * segments
     * and returns the {@code index}-th segment using BigInteger arithmetic to avoid
     * overflow.
     */
    public static TokenRange computeRange(int index, int total) {
        if (total == 1) {
            return new TokenRange(TokenRange.MIN_TOKEN, TokenRange.MAX_TOKEN);
        }

        java.math.BigInteger minToken = java.math.BigInteger.valueOf(Long.MIN_VALUE);
        java.math.BigInteger maxToken = java.math.BigInteger.valueOf(Long.MAX_VALUE);
        // Full token space = 2^64 values
        java.math.BigInteger tokenRange = maxToken.subtract(minToken).add(java.math.BigInteger.ONE);
        java.math.BigInteger step = tokenRange.divide(java.math.BigInteger.valueOf(total));

        java.math.BigInteger rangeStart = minToken.add(
                step.multiply(java.math.BigInteger.valueOf(index)));
        java.math.BigInteger rangeEnd = (index == total - 1)
                ? maxToken
                : rangeStart.add(step).subtract(java.math.BigInteger.ONE);

        return new TokenRange(rangeStart.longValue(), rangeEnd.longValue());
    }

    @Override
    public long nextPartitionKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long nextClusteringKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isPartitionDone() {
        return false;
    }

    @Override
    public boolean isDone() {
        return processed >= count;
    }

    @Override
    public void restart() {
        current = startOffset;
        processed = 0;
    }
}
