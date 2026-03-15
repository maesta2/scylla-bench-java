package com.scylladb.bench;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * Rate limiter to control the maximum request rate, matching scylla-bench's
 * RateLimiter.
 */
public interface RateLimiter {
    void await();

    Instant expected();

    static RateLimiter create(int maxRate, Duration timeOffset) {
        if (maxRate == 0)
            return new Unlimited();
        return new MaxRate(maxRate, timeOffset);
    }

    /** No rate limiting. */
    class Unlimited implements RateLimiter {
        @Override
        public void await() {
        }

        @Override
        public Instant expected() {
            return Instant.MIN;
        }
    }

    /** Limits throughput to at most `rate` ops/second using a sliding window. */
    class MaxRate implements RateLimiter {
        private final long periodNanos; // nanoseconds between successive operations
        private final long startNanos; // absolute start nanos (System.nanoTime())
        private long completedOps;

        private MaxRate(int rate, Duration timeOffset) {
            this.periodNanos = 1_000_000_000L / rate;
            this.startNanos = System.nanoTime() + timeOffset.toNanos();
            this.completedOps = 0;
        }

        @Override
        public void await() {
            completedOps++;
            long nextNanos = startNanos + periodNanos * completedOps;
            long sleepNanos = nextNanos - System.nanoTime();
            if (sleepNanos > 0) {
                try {
                    TimeUnit.NANOSECONDS.sleep(sleepNanos);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        @Override
        public Instant expected() {
            long epochNanos = (startNanos + periodNanos * completedOps)
                    - System.nanoTime()
                    + System.currentTimeMillis() * 1_000_000L;
            return Instant.ofEpochSecond(0, epochNanos);
        }
    }
}
