package com.scylladb.bench.workloads;

/**
 * A token range [start, end] inclusive for full-table range scans.
 * For Murmur3 partitioner tokens are in [-2^63, 2^63-1].
 */
public final class TokenRange {
    public static final long MIN_TOKEN = Long.MIN_VALUE;
    public static final long MAX_TOKEN = Long.MAX_VALUE;

    private final long start;
    private final long end;

    public TokenRange(long start, long end) {
        this.start = start;
        this.end = end;
    }

    public long start() {
        return start;
    }

    public long end() {
        return end;
    }

    @Override
    public String toString() {
        return "TokenRange[" + start + ", " + end + "]";
    }
}
