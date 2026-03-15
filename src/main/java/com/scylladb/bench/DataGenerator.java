package com.scylladb.bench;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generates and validates benchmark data, porting scylla-bench's GenerateData /
 * ValidateData.
 *
 * Data layout (little-endian):
 * size < 24 bytes: [int8: size][int64: pk^ck] (truncated to `size` bytes)
 * 24 <= size < 57: [int64: size][int64: pk][int64: ck][zeros...]
 * size >= 57: [int64: size][int64: pk][int64: ck][random payload][sha256
 * checksum]
 */
public class DataGenerator {

    public static final long HEADER_SIZE = 24L; // 8+8+8 bytes: size + pk + ck
    public static final long SHA256_SIZE = 32L; // java.security.SHA-256 output
    public static final long MIN_FULL_SIZE = HEADER_SIZE + SHA256_SIZE + 1; // 57 bytes

    private DataGenerator() {
    }

    /**
     * Generates a data blob for (pk, ck) of exactly {@code size} bytes.
     * When validateData=false returns an all-zero array (fast path).
     */
    public static byte[] generate(long pk, long ck, long size, boolean validateData) {
        if (size == 0)
            return new byte[0];
        if (!validateData)
            return new byte[(int) size];

        ByteBuffer buf = ByteBuffer.allocate((int) (size + SHA256_SIZE + 16));
        buf.order(ByteOrder.LITTLE_ENDIAN);

        if (size < HEADER_SIZE) {
            buf.put((byte) size);
            buf.putLong(pk ^ ck);
        } else {
            buf.putLong(size);
            buf.putLong(pk);
            buf.putLong(ck);

            if (size < MIN_FULL_SIZE) {
                // Fill zeros after header
                for (long i = HEADER_SIZE; i < size; i++) {
                    buf.put((byte) 0);
                }
            } else {
                int payloadLen = (int) (size - HEADER_SIZE - SHA256_SIZE);
                byte[] payload = new byte[payloadLen];
                ThreadLocalRandom.current().nextBytes(payload);
                buf.put(payload);
                buf.put(sha256(payload));
            }
        }

        byte[] result = new byte[(int) size];
        buf.flip();
        int toCopy = Math.min((int) size, buf.limit());
        buf.get(result, 0, toCopy);
        return result;
    }

    /**
     * Validates that {@code data} was generated for the given (pk, ck).
     * Returns null on success or an error message on failure.
     */
    public static String validate(long pk, long ck, byte[] data) {
        if (data == null || data.length == 0)
            return null;

        ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        long size = data.length;

        long storedSize;
        if (size < HEADER_SIZE) {
            storedSize = buf.get() & 0xFF; // unsigned byte
        } else {
            storedSize = buf.getLong();
        }

        if (size != storedSize) {
            return String.format("actual size (%d) != stored size (%d)", size, storedSize);
        }

        if (size < MIN_FULL_SIZE) {
            // Regenerate and compare
            byte[] expected = generate(pk, ck, size, true);
            if (!Arrays.equals(data, expected)) {
                return String.format("value mismatch for pk=%d ck=%d", pk, ck);
            }
            return null;
        }

        // Validate pk
        long storedPk = buf.getLong();
        if (storedPk != pk) {
            return String.format("actual pk (%d) != stored pk (%d)", pk, storedPk);
        }

        // Validate ck
        long storedCk = buf.getLong();
        if (storedCk != ck) {
            return String.format("actual ck (%d) != stored ck (%d)", ck, storedCk);
        }

        // Validate SHA-256 checksum over payload
        int payloadLen = (int) (size - HEADER_SIZE - SHA256_SIZE);
        byte[] payload = new byte[payloadLen];
        buf.get(payload);

        byte[] storedChecksum = new byte[(int) SHA256_SIZE];
        buf.get(storedChecksum);

        byte[] calculatedChecksum = sha256(payload);
        if (!Arrays.equals(calculatedChecksum, storedChecksum)) {
            return String.format("checksum mismatch for pk=%d ck=%d", pk, ck);
        }

        return null; // success
    }

    private static byte[] sha256(byte[] data) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(data);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }
}
