package com.scylladb.bench;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DistributionTest {

    // --- Fixed ---

    @Test
    void fixedAlwaysReturnsValue() {
        Distribution.Fixed d = new Distribution.Fixed(42);
        for (int i = 0; i < 10; i++) {
            assertEquals(42, d.generate());
        }
    }

    @Test
    void fixedToString() {
        assertEquals("fixed:7", new Distribution.Fixed(7).toString());
    }

    @Test
    void fixedRejectsZeroValue() {
        assertThrows(IllegalArgumentException.class, () -> new Distribution.Fixed(0));
    }

    @Test
    void fixedRejectsNegativeValue() {
        assertThrows(IllegalArgumentException.class, () -> new Distribution.Fixed(-5));
    }

    // --- Uniform ---

    @Test
    void uniformGeneratesValuesInRange() {
        Distribution.Uniform d = new Distribution.Uniform(10, 20);
        for (int i = 0; i < 100; i++) {
            long v = d.generate();
            assertTrue(v >= 10 && v < 20, "Value out of range: " + v);
        }
    }

    @Test
    void uniformToString() {
        assertEquals("uniform:5..15", new Distribution.Uniform(5, 15).toString());
    }

    @Test
    void uniformRejectsMinEqualMax() {
        assertThrows(IllegalArgumentException.class, () -> new Distribution.Uniform(5, 5));
    }

    @Test
    void uniformRejectsMinGreaterThanMax() {
        assertThrows(IllegalArgumentException.class, () -> new Distribution.Uniform(10, 5));
    }

    // --- parse ---

    @Test
    void parsePlainIntegerCreatesFixed() {
        Distribution d = Distribution.parse("100");
        assertInstanceOf(Distribution.Fixed.class, d);
        assertEquals(100, d.generate());
    }

    @Test
    void parseFixedPrefix() {
        Distribution d = Distribution.parse("fixed:50");
        assertInstanceOf(Distribution.Fixed.class, d);
        assertEquals(50, d.generate());
    }

    @Test
    void parseUniform() {
        Distribution d = Distribution.parse("uniform:1..10");
        assertInstanceOf(Distribution.Uniform.class, d);
    }

    @Test
    void parseTrimsWhitespace() {
        Distribution d = Distribution.parse("  fixed:3  ");
        assertEquals(3, d.generate());
    }

    @Test
    void parseNullThrows() {
        assertThrows(IllegalArgumentException.class, () -> Distribution.parse(null));
    }

    @Test
    void parseBlankThrows() {
        assertThrows(IllegalArgumentException.class, () -> Distribution.parse("  "));
    }

    @Test
    void parseUnknownFormatThrows() {
        assertThrows(IllegalArgumentException.class, () -> Distribution.parse("gaussian:1..5"));
    }

    @Test
    void parseUniformMissingSeparatorThrows() {
        assertThrows(IllegalArgumentException.class, () -> Distribution.parse("uniform:510"));
    }
}
