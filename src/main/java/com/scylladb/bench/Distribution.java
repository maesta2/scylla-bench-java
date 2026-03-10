package com.scylladb.bench;

import java.util.Random;

/**
 * Distribution for random value generation (e.g., clustering-row-size).
 * Supports fixed values and uniform random ranges, matching scylla-bench's
 * distribution syntax.
 */
public abstract class Distribution {

    public abstract long generate();

    @Override
    public abstract String toString();

    /** Always returns the same value. */
    public static class Fixed extends Distribution {
        private final long value;

        public Fixed(long value) {
            if (value < 1)
                throw new IllegalArgumentException(
                        "Fixed distribution value must be positive, got: " + value);
            this.value = value;
        }

        @Override
        public long generate() {
            return value;
        }

        @Override
        public String toString() {
            return "fixed:" + value;
        }
    }

    /** Returns a uniformly-distributed long in [min, max). */
    public static class Uniform extends Distribution {
        private final long min;
        private final long max; // exclusive
        private final Random rng = new Random();

        public Uniform(long min, long max) {
            if (min >= max)
                throw new IllegalArgumentException(
                        "Uniform distribution requires min < max, got: " + min + ".." + max);
            this.min = min;
            this.max = max;
        }

        @Override
        public long generate() {
            return min + Math.abs(rng.nextLong() % (max - min));
        }

        @Override
        public String toString() {
            return "uniform:" + min + ".." + max;
        }
    }

    /**
     * Parses a distribution expression.
     * Accepted formats:
     * {@code N} → fixed:N
     * {@code fixed:N} → fixed:N
     * {@code uniform:A..B} → uniform in [A, B)
     */
    public static Distribution parse(String s) {
        if (s == null || s.isBlank()) {
            throw new IllegalArgumentException("Empty distribution string");
        }
        s = s.trim();

        // Try to parse as a plain integer (fixed distribution)
        try {
            long v = Long.parseLong(s);
            return new Fixed(v);
        } catch (NumberFormatException ignored) {
            /* fall through */ }

        if (s.startsWith("fixed:")) {
            long v = Long.parseLong(s.substring(6));
            return new Fixed(v);
        }

        if (s.startsWith("uniform:")) {
            String range = s.substring(8);
            int sep = range.indexOf("..");
            if (sep < 0)
                throw new IllegalArgumentException(
                        "uniform distribution requires 'min..max' format, got: " + s);
            long min = Long.parseLong(range.substring(0, sep));
            long max = Long.parseLong(range.substring(sep + 2));
            return new Uniform(min, max);
        }

        throw new IllegalArgumentException("Unrecognised distribution format: " + s);
    }
}
