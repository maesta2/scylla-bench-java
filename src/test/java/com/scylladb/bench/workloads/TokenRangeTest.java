package com.scylladb.bench.workloads;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TokenRangeTest {

    @Test
    void constructorAndGetters() {
        TokenRange tr = new TokenRange(Long.MIN_VALUE, Long.MAX_VALUE);
        assertEquals(Long.MIN_VALUE, tr.start());
        assertEquals(Long.MAX_VALUE, tr.end());
    }

    @Test
    void toStringFormat() {
        TokenRange tr = new TokenRange(-100, 200);
        assertEquals("TokenRange[-100, 200]", tr.toString());
    }

    @Test
    void minMaxConstants() {
        assertEquals(Long.MIN_VALUE, TokenRange.MIN_TOKEN);
        assertEquals(Long.MAX_VALUE, TokenRange.MAX_TOKEN);
    }

    @Test
    void singlePointRange() {
        TokenRange tr = new TokenRange(5, 5);
        assertEquals(5, tr.start());
        assertEquals(5, tr.end());
    }
}
