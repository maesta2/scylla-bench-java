package com.scylladb.bench;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.jupiter.api.Assertions.*;

class DataGeneratorTest {

    // --- generate ---

    @Test
    void generateSizeZeroReturnsEmptyArray() {
        byte[] data = DataGenerator.generate(1, 2, 0, true);
        assertNotNull(data);
        assertEquals(0, data.length);
    }

    @Test
    void generateNoValidationReturnsZeroArray() {
        byte[] data = DataGenerator.generate(1, 2, 16, false);
        assertNotNull(data);
        assertEquals(16, data.length);
        for (byte b : data)
            assertEquals(0, b);
    }

    @Test
    void generateSmallSizeHasCorrectLength() {
        // size < HEADER_SIZE (24)
        byte[] data = DataGenerator.generate(5, 10, 10, true);
        assertEquals(10, data.length);
    }

    @Test
    void generateSmallSizeFirstByteIsSize() {
        byte[] data = DataGenerator.generate(5, 10, 10, true);
        assertEquals(10, data[0] & 0xFF);
    }

    @Test
    void generateMediumSizeHasCorrectLength() {
        // 24 <= size < 57
        byte[] data = DataGenerator.generate(1, 2, 30, true);
        assertEquals(30, data.length);
    }

    @Test
    void generateMediumSizeHasCorrectHeader() {
        long pk = 7, ck = 13;
        byte[] data = DataGenerator.generate(pk, ck, 30, true);
        ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        assertEquals(30L, buf.getLong());
        assertEquals(pk, buf.getLong());
        assertEquals(ck, buf.getLong());
    }

    @Test
    void generateFullSizeHasCorrectLength() {
        // size >= 57
        byte[] data = DataGenerator.generate(3, 4, 100, true);
        assertEquals(100, data.length);
    }

    @Test
    void generateFullSizeHasCorrectHeader() {
        long pk = 99, ck = 42;
        byte[] data = DataGenerator.generate(pk, ck, 100, true);
        ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        assertEquals(100L, buf.getLong());
        assertEquals(pk, buf.getLong());
        assertEquals(ck, buf.getLong());
    }

    // --- validate ---

    @Test
    void validateNullReturnsNull() {
        assertNull(DataGenerator.validate(1, 2, null));
    }

    @Test
    void validateEmptyReturnsNull() {
        assertNull(DataGenerator.validate(1, 2, new byte[0]));
    }

    @Test
    void validateRoundtripSmallSize() {
        byte[] data = DataGenerator.generate(1, 2, 10, true);
        assertNull(DataGenerator.validate(1, 2, data));
    }

    @Test
    void validateRoundtripMediumSize() {
        byte[] data = DataGenerator.generate(5, 6, 30, true);
        assertNull(DataGenerator.validate(5, 6, data));
    }

    @Test
    void validateRoundtripFullSize() {
        byte[] data = DataGenerator.generate(7, 8, 100, true);
        assertNull(DataGenerator.validate(7, 8, data));
    }

    @Test
    void validateDetectsWrongPk() {
        byte[] data = DataGenerator.generate(1, 2, 100, true);
        String err = DataGenerator.validate(99, 2, data);
        assertNotNull(err);
        assertTrue(err.contains("pk"));
    }

    @Test
    void validateDetectsWrongCk() {
        byte[] data = DataGenerator.generate(1, 2, 100, true);
        String err = DataGenerator.validate(1, 99, data);
        assertNotNull(err);
        assertTrue(err.contains("ck"));
    }

    @Test
    void validateDetectsTamperedPayload() {
        byte[] data = DataGenerator.generate(1, 2, 100, true);
        // Corrupt the payload byte just after the header
        data[24] ^= 0xFF;
        String err = DataGenerator.validate(1, 2, data);
        assertNotNull(err);
    }

    @Test
    void validateDetectsSizeMismatch() {
        byte[] data = DataGenerator.generate(1, 2, 100, true);
        // Truncate
        byte[] truncated = new byte[80];
        System.arraycopy(data, 0, truncated, 0, 80);
        String err = DataGenerator.validate(1, 2, truncated);
        assertNotNull(err);
        assertTrue(err.contains("size"));
    }
}
