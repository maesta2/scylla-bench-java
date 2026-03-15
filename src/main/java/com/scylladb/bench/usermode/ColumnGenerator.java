package com.scylladb.bench.usermode;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generates column values according to population/size distributions.
 *
 * Supported distribution strings:
 *   fixed:N             – always returns N
 *   uniform:MIN..MAX    – uniform random in [MIN, MAX]
 *   gaussian:MIN..MAX   – normal, mean=(MIN+MAX)/2, stddev=(mean-MIN)/3
 *   seq:MIN..MAX        – sequential, wraps at MAX
 *
 * Magnitude suffixes: k=1000, m=1_000_000, b=1_000_000_000
 */
public class ColumnGenerator {

    public enum DistType { FIXED, UNIFORM, GAUSSIAN, SEQ }

    private final String columnName;
    private final String columnType;   // CQL type string (text, blob, bigint, int, boolean, …)

    // population distribution controls the identity seed (how many distinct values)
    private final DistType popType;
    private final long popMin;
    private final long popMax;

    // size distribution (for text/blob)
    private final DistType sizeType;
    private final long sizeMin;
    private final long sizeMax;

    // cluster distribution (number of clustering rows per partition)
    private final DistType clusterType;
    private final long clusterMin;
    private final long clusterMax;

    // sequential counter
    private long seqCurrent;

    public ColumnGenerator(String name, String cqlType, UserProfile.ColumnSpec spec) {
        this.columnName = name;
        this.columnType = cqlType == null ? "text" : cqlType.toLowerCase();

        UserProfile.ColumnSpec s = spec != null ? spec : new UserProfile.ColumnSpec();

        long[] pop = parseDist(s.population != null ? s.population : "uniform:1..1000000");
        popType = distType(s.population != null ? s.population : "uniform:1..1000000");
        popMin  = pop[0];
        popMax  = pop[1];

        long[] size = parseDist(s.size != null ? s.size : "fixed:4");
        sizeType = distType(s.size != null ? s.size : "fixed:4");
        sizeMin  = size[0];
        sizeMax  = size[1];

        long[] cluster = parseDist(s.cluster != null ? s.cluster : "fixed:1");
        clusterType = distType(s.cluster != null ? s.cluster : "fixed:1");
        clusterMin  = cluster[0];
        clusterMax  = cluster[1];

        seqCurrent = popMin;
    }

    /** Returns the number of clustering rows to create for the current partition. */
    public long nextClusterCount() {
        return sample(clusterType, clusterMin, clusterMax);
    }

    /**
     * Generates a value for this column as a Java object suitable for driver binding.
     * The returned type matches the declared CQL type:
     *   text/ascii/varchar → String
     *   blob               → byte[]
     *   bigint/counter     → Long
     *   int                → Integer
     *   smallint           → Short
     *   tinyint            → Byte
     *   boolean            → Boolean
     *   double             → Double
     *   float              → Float
     *   uuid/timeuuid      → java.util.UUID
     *   timestamp          → java.time.Instant
     */
    public Object generateValue() {
        long seed = nextSeed();
        return buildValue(seed);
    }

    /** Generates a deterministic value for a given seed (for re-reads). */
    public Object generateValue(long seed) {
        return buildValue(seed);
    }

    /** Returns the next population seed. */
    public long nextSeed() {
        return sample(popType, popMin, popMax);
    }

    // ── value builders ────────────────────────────────────────────────────

    private Object buildValue(long seed) {
        switch (columnType) {
            case "text": case "ascii": case "varchar":
                return buildString(seed);
            case "blob":
                return buildBytes(seed);
            case "bigint": case "counter":
                return seed;
            case "int":
                return (int)(seed & 0xFFFFFFFFL);
            case "smallint":
                return (short)(seed & 0xFFFFL);
            case "tinyint":
                return (byte)(seed & 0xFFL);
            case "boolean":
                return (seed & 1L) == 0;
            case "double":
                return Double.longBitsToDouble(seed);
            case "float":
                return Float.intBitsToFloat((int)(seed & 0xFFFFFFFFL));
            case "uuid":
                return new java.util.UUID(seed, ~seed);
            case "timeuuid":
                // v1 UUID: embed seed as timestamp (100-ns intervals since Oct 15, 1582)
                long ts = seed & 0x0FFFFFFFFFFFFFFFL;
                long msb = (ts & 0x0FFFL) << 48 | ((ts >> 12) & 0xFFFFL) << 32 | (ts >> 28) & 0xFFFFFFFFL;
                msb = (msb & ~0xF000L) | 0x1000L; // version 1
                long lsb = 0xC000000000000000L | (seed & 0x3FFFFFFFFFFFFFFFL);
                return new java.util.UUID(msb, lsb);
            case "timestamp":
                return java.time.Instant.ofEpochMilli(Math.abs(seed) % (System.currentTimeMillis() + 1));
            default:
                // fallback: return as string
                return Long.toString(seed);
        }
    }

    private String buildString(long seed) {
        long sz = sample(sizeType, sizeMin, sizeMax);
        // cap at 64KB
        int len = (int) Math.min(sz, 65536);
        StringBuilder sb = new StringBuilder(len);
        Random rnd = new Random(seed);
        for (int i = 0; i < len; i++) {
            sb.append((char)('a' + rnd.nextInt(26)));
        }
        return sb.toString();
    }

    private byte[] buildBytes(long seed) {
        long sz = sample(sizeType, sizeMin, sizeMax);
        int len = (int) Math.min(sz, 65536);
        byte[] buf = new byte[len];
        new Random(seed).nextBytes(buf);
        return buf;
    }

    // ── distribution helpers ─────────────────────────────────────────────

    private long sample(DistType type, long min, long max) {
        if (min == max || type == DistType.FIXED) return min;
        switch (type) {
            case UNIFORM:
                return min + (Math.abs(ThreadLocalRandom.current().nextLong()) % (max - min + 1));
            case GAUSSIAN: {
                double mean = (min + max) / 2.0;
                double stddev = (mean - min) / 3.0;
                long v = (long)(ThreadLocalRandom.current().nextGaussian() * stddev + mean);
                return Math.max(min, Math.min(max, v));
            }
            case SEQ: {
                long v = seqCurrent++;
                if (seqCurrent > max) seqCurrent = min;
                return v;
            }
            default: return min;
        }
    }

    private static DistType distType(String spec) {
        if (spec == null) return DistType.FIXED;
        String lower = spec.toLowerCase().trim();
        if (lower.startsWith("uniform")) return DistType.UNIFORM;
        if (lower.startsWith("gaussian")) return DistType.GAUSSIAN;
        if (lower.startsWith("seq")) return DistType.SEQ;
        return DistType.FIXED;
    }

    /** Parses "type:MIN..MAX" or "type:N" → [min, max]. */
    public static long[] parseDist(String spec) {
        if (spec == null || spec.isEmpty()) return new long[]{1, 1};
        int colon = spec.indexOf(':');
        if (colon < 0) {
            // plain integer
            return new long[]{parseMag(spec), parseMag(spec)};
        }
        String val = spec.substring(colon + 1).trim();
        int dotdot = val.indexOf("..");
        if (dotdot < 0) {
            long n = parseMag(val);
            return new long[]{n, n};
        }
        long min = parseMag(val.substring(0, dotdot).trim());
        long max = parseMag(val.substring(dotdot + 2).trim());
        return new long[]{min, max};
    }

    /** Parses "10k", "5m", "1b", or plain long. */
    public static long parseMag(String s) {
        s = s.trim().toLowerCase();
        if (s.endsWith("b")) return Long.parseLong(s.substring(0, s.length()-1)) * 1_000_000_000L;
        if (s.endsWith("m")) return Long.parseLong(s.substring(0, s.length()-1)) * 1_000_000L;
        if (s.endsWith("k")) return Long.parseLong(s.substring(0, s.length()-1)) * 1_000L;
        return Long.parseLong(s);
    }

    public String getColumnName() { return columnName; }
    public String getColumnType() { return columnType; }
}
