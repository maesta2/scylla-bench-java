package com.scylladb.bench.workloads;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SequentialWorkloadTest {

    @Test
    void firstPartitionAndClusteringKeyFromZeroOffset() {
        SequentialWorkload w = new SequentialWorkload(0, 6, 3);
        assertEquals(0, w.nextPartitionKey());
        assertEquals(0, w.nextClusteringKey());
    }

    @Test
    void traversesAllRowsInOrder() {
        // 2 partitions x 3 clustering rows
        SequentialWorkload w = new SequentialWorkload(0, 6, 3);

        long[][] expected = {
                { 0, 0 }, { 0, 1 }, { 0, 2 },
                { 1, 0 }, { 1, 1 }, { 1, 2 }
        };

        for (long[] pair : expected) {
            long pk = w.nextPartitionKey();
            long ck = w.nextClusteringKey();
            assertEquals(pair[0], pk, "pk mismatch");
            assertEquals(pair[1], ck, "ck mismatch");
        }
        assertTrue(w.isDone());
    }

    @Test
    void isPartitionDoneAfterLastClusteringRow() {
        SequentialWorkload w = new SequentialWorkload(0, 3, 3);
        w.nextPartitionKey();
        w.nextClusteringKey(); // ck=0
        assertFalse(w.isPartitionDone());
        w.nextPartitionKey();
        w.nextClusteringKey(); // ck=1
        assertFalse(w.isPartitionDone());
        w.nextPartitionKey();
        w.nextClusteringKey(); // ck=2
        assertTrue(w.isPartitionDone());
    }

    @Test
    void isDoneAfterAllRows() {
        SequentialWorkload w = new SequentialWorkload(0, 2, 1);
        assertFalse(w.isDone());
        w.nextPartitionKey();
        w.nextClusteringKey();
        assertFalse(w.isDone());
        w.nextPartitionKey();
        w.nextClusteringKey();
        assertTrue(w.isDone());
    }

    @Test
    void restartResetsState() {
        SequentialWorkload w = new SequentialWorkload(0, 2, 2);
        w.nextPartitionKey();
        w.nextClusteringKey();
        w.nextPartitionKey();
        w.nextClusteringKey();
        assertTrue(w.isDone());

        w.restart();
        assertFalse(w.isDone());
        assertEquals(0, w.nextPartitionKey());
        assertEquals(0, w.nextClusteringKey());
    }

    @Test
    void nonZeroOffsetStartsAtCorrectPosition() {
        // offset=3 means pk=1 (3/3), ck=0 (3%3)
        SequentialWorkload w = new SequentialWorkload(3, 3, 3);
        assertEquals(1, w.nextPartitionKey());
        assertEquals(0, w.nextClusteringKey());
    }

    @Test
    void partitionAdvancesAfterClusteringRowsExhausted() {
        SequentialWorkload w = new SequentialWorkload(0, 4, 2);
        // partition 0: ck 0,1
        w.nextPartitionKey();
        w.nextClusteringKey();
        w.nextPartitionKey();
        w.nextClusteringKey();
        // partition 1 should be next
        assertEquals(1, w.nextPartitionKey());
    }
}
