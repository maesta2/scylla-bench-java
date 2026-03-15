package com.scylladb.bench;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class RateLimiterTest {

    @Test
    void createWithZeroRateReturnsUnlimited() {
        RateLimiter rl = RateLimiter.create(0, Duration.ZERO);
        assertInstanceOf(RateLimiter.Unlimited.class, rl);
    }

    @Test
    void createWithPositiveRateReturnsMaxRate() {
        RateLimiter rl = RateLimiter.create(100, Duration.ZERO);
        assertInstanceOf(RateLimiter.MaxRate.class, rl);
    }

    @Test
    void unlimitedAwaitReturnsImmediately() {
        RateLimiter rl = new RateLimiter.Unlimited();
        // Should not block
        long start = System.nanoTime();
        rl.await();
        long elapsed = System.nanoTime() - start;
        assertTrue(elapsed < 50_000_000L, "Unlimited await should be near-instant");
    }

    @Test
    void unlimitedExpectedReturnsInstantMin() {
        RateLimiter rl = new RateLimiter.Unlimited();
        assertEquals(Instant.MIN, rl.expected());
    }

    @Test
    void maxRateExpectedReturnsNonNullInstant() {
        RateLimiter rl = RateLimiter.create(1000, Duration.ZERO);
        Instant exp = rl.expected();
        assertNotNull(exp);
    }

    @Test
    void maxRateDoesNotBlockForHighRate() {
        // At 100_000 ops/s the period is 10 µs - calling await once should not sleep
        RateLimiter rl = RateLimiter.create(100_000, Duration.ZERO);
        long start = System.nanoTime();
        rl.await();
        long elapsed = System.nanoTime() - start;
        assertTrue(elapsed < 200_000_000L, "Should not block for a long time");
    }
}
