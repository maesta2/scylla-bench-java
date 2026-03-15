package com.scylladb.bench;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.scylladb.bench.results.TestResults;
import com.scylladb.bench.results.TestThreadResult;
import com.scylladb.bench.workloads.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Core benchmark execution engine.
 * Mirrors scylla-bench's modes.go logic translated to Java.
 */
public class BenchmarkRunner {

    private static final Logger LOG = LoggerFactory.getLogger(BenchmarkRunner.class);
    private static final Duration REPORT_INTERVAL = Duration.ofSeconds(1);

    // ----- configuration -----
    public final String keyspaceName;
    public final String tableName;
    public final String counterTableName;
    public final String mode;
    public final int concurrency;
    public final int rowsPerRequest;
    public final boolean provideUpperBound;
    public final boolean inRestriction;
    public final String[] selectOrderByParsed;
    public final boolean noLowerBound;
    public final boolean bypassCache;
    public final boolean validateData;
    public final boolean lwtEnabled;
    public final Distribution clusteringRowSizeDist;
    public final int maxErrorsAtRow;
    public final int maxErrors;
    public final int retryNumber;
    public final Duration retryMin;
    public final Duration retryMax;
    public final String retryHandler;
    public final Duration errorToTimeoutCutoff;
    public final DefaultConsistencyLevel consistencyLevel;
    public final Duration readTimeout;
    public final Duration writeTimeout;
    public final Duration casTimeout;

    // ----- session -----
    private final CqlSession session;

    // ----- prepared statements (lazily initialized) -----
    private PreparedStatement writePS;
    private PreparedStatement batchWritePS;
    private PreparedStatement counterUpdatePS;
    private PreparedStatement scanPS;

    // ----- global coordination -----
    public final AtomicBoolean stopAll;
    private final AtomicLong globalMixedOpCount = new AtomicLong(0);
    private final java.util.concurrent.atomic.AtomicInteger totalErrors = new java.util.concurrent.atomic.AtomicInteger(
            0);
    private volatile boolean totalErrorsPrinted = false;

    public BenchmarkRunner(CqlSession session, Main config, AtomicBoolean stopAll) {
        this.session = session;
        this.stopAll = stopAll;

        keyspaceName = config.keyspaceName;
        tableName = config.tableName;
        counterTableName = "test_counters";
        mode = config.mode;
        concurrency = config.concurrency;
        rowsPerRequest = config.rowsPerRequest;
        provideUpperBound = config.provideUpperBound;
        inRestriction = config.inRestriction;
        noLowerBound = config.noLowerBound;
        bypassCache = config.bypassCache;
        validateData = config.validateData;
        lwtEnabled = config.lwtEnabled;
        clusteringRowSizeDist = config.parsedClusteringRowSize;
        maxErrorsAtRow = config.maxErrorsAtRow;
        maxErrors = config.maxErrors;
        retryNumber = config.retryNumber;
        retryMin = config.parsedRetryMin;
        retryMax = config.parsedRetryMax;
        retryHandler = config.retryHandler;
        errorToTimeoutCutoff = config.timeout.isZero()
                ? Duration.ofSeconds(1)
                : config.timeout.dividedBy(5);
        consistencyLevel = config.parsedConsistencyLevel;
        readTimeout  = config.readTimeout;
        writeTimeout = config.writeTimeout;
        casTimeout   = config.casTimeout;

        List<String> orderBys = new ArrayList<>();
        for (String chunk : config.selectOrderBy.split(",")) {
            switch (chunk.trim().toLowerCase()) {
                case "none":
                    orderBys.add("");
                    break;
                case "asc":
                    orderBys.add("ORDER BY ck ASC");
                    break;
                case "desc":
                    orderBys.add("ORDER BY ck DESC");
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Invalid select-order-by value: " + chunk);
            }
        }
        selectOrderByParsed = orderBys.toArray(new String[0]);
    }

    // ======================== DATABASE SCHEMA ========================

    public void prepareDatabase(int replicationFactor, boolean truncateTable) {
        execute(String.format(
                "CREATE KEYSPACE IF NOT EXISTS %s " +
                        "WITH REPLICATION = { 'class':'NetworkTopologyStrategy','replication_factor':%d }",
                keyspaceName, replicationFactor));

        switch (mode) {
            case "counter_update":
            case "counter_read":
                execute("CREATE TABLE IF NOT EXISTS " + keyspaceName + "." + counterTableName +
                        " (pk bigint, ck bigint, c1 counter, c2 counter, c3 counter, " +
                        "c4 counter, c5 counter, PRIMARY KEY(pk, ck)) WITH compression = { }");
                break;
            default:
                execute("CREATE TABLE IF NOT EXISTS " + keyspaceName + "." + tableName +
                        " (pk bigint, ck bigint, v blob, PRIMARY KEY(pk, ck)) WITH compression = { }");
        }

        if (truncateTable) {
            switch (mode) {
                case "write":
                case "mixed":
                    LOG.info("Truncating table '{}'", tableName);
                    execute("TRUNCATE TABLE " + keyspaceName + "." + tableName);
                    break;
                case "counter_update":
                    LOG.info("Truncating table '{}'", counterTableName);
                    execute("TRUNCATE TABLE " + keyspaceName + "." + counterTableName);
                    break;
            }
        }
    }

    private void execute(String cql) {
        session.execute(SimpleStatement.newInstance(cql)
                .setConsistencyLevel(consistencyLevel));
    }

    // ======================== CONCURRENCY ========================

    /**
     * Launches {@code concurrency} threads, each running the given worker function
     * with its own rate limiter. Returns TestResults for subsequent reporting.
     */
    public TestResults runConcurrently(int maxRate, TestResults testResults,
            WorkerFunction worker) {
        testResults.setStartTime();
        testResults.printResultsHeader();

        long timeOffsetUnit = (maxRate != 0) ? 1_000_000_000L / maxRate : 0L;
        int perThreadRate = (maxRate != 0 && concurrency > 0) ? maxRate / concurrency : maxRate;

        List<Thread> threads = new ArrayList<>(concurrency);
        for (int i = 0; i < concurrency; i++) {
            final int threadId = i;
            TestThreadResult ttr = testResults.getThreadResult(i);
            Duration offset = Duration.ofNanos(timeOffsetUnit * threadId);
            RateLimiter rl = RateLimiter.create(perThreadRate, offset);

            Thread t = new Thread(() -> worker.run(threadId, ttr, rl),
                    "benchmark-worker-" + i);
            t.setDaemon(true);
            threads.add(t);
        }
        threads.forEach(Thread::start);
        return testResults;
    }

    @FunctionalInterface
    public interface WorkerFunction {
        void run(int threadId, TestThreadResult result, RateLimiter rl);
    }

    // ======================== INNER TEST LOOP ========================

    /**
     * Core test execution loop for a single worker thread.
     * Mirrors scylla-bench's RunTest.
     */
    public void runTest(TestThreadResult threadResult, WorkloadGenerator workload,
            RateLimiter rl, long iterations,
            TestFunc test) {
        Instant start = Instant.now();
        Instant partialStart = start;
        TestIterator iter = new TestIterator(workload, iterations, stopAll);
        int errorsAtRow = 0;

        while (!iter.isDone()) {
            rl.await();

            Instant expectedStart = rl.expected();
            if (expectedStart == Instant.MIN)
                expectedStart = Instant.now();

            long rawNanos;
            boolean isError = false;
            Exception err = null;
            boolean doNotRegister = false;

            Instant opEnd;
            long opStartNanos = System.nanoTime();
            try {
                rawNanos = test.execute(threadResult);
                opEnd = Instant.now();
            } catch (DoNotRegisterException e) {
                opEnd = Instant.now();
                rawNanos = System.nanoTime() - opStartNanos;
                doNotRegister = true;
            } catch (Exception e) {
                opEnd = Instant.now();
                rawNanos = System.nanoTime() - opStartNanos;
                isError = true;
                err = e;
            }

            if (!doNotRegister) {
                if (!isError) {
                    errorsAtRow = 0;
                    Duration rawLatency = Duration.ofNanos(rawNanos);
                    threadResult.recordRawLatency(rawLatency);
                    threadResult.recordCoFixedLatency(Duration.between(expectedStart, opEnd));
                } else {
                    errorsAtRow++;
                    totalErrors.incrementAndGet();
                    threadResult.incErrors();
                    LOG.error("Benchmark error: {}", err.getMessage(), err);

                    Duration rawLatency = Duration.ofNanos(rawNanos);
                    if (rawLatency.compareTo(errorToTimeoutCutoff) > 0) {
                        threadResult.recordRawLatency(rawLatency);
                        threadResult.recordCoFixedLatency(Duration.between(expectedStart, opEnd));
                    }
                }
            }

            // Check error limits
            if (maxErrorsAtRow > 0 && errorsAtRow >= maxErrorsAtRow) {
                threadResult.submitCriticalError(new RuntimeException(
                        "error limit (maxErrorsAtRow) of " + maxErrorsAtRow + " reached"));
            }
            if (maxErrors > 0 && totalErrors.get() >= maxErrors && !totalErrorsPrinted) {
                synchronized (this) {
                    if (!totalErrorsPrinted) {
                        totalErrorsPrinted = true;
                        threadResult.submitCriticalError(new RuntimeException(
                                "error limit (maxErrors) of " + maxErrors + " reached"));
                    }
                }
            }
            if (TestThreadResult.globalErrorFlag) {
                threadResult.flushPartialResult(Duration.between(partialStart, Instant.now()));
                break;
            }

            // Periodic reporting
            Instant now = Instant.now();
            if (Duration.between(partialStart, now).compareTo(REPORT_INTERVAL) >= 0) {
                threadResult.flushPartialResult(Duration.between(partialStart, now));
                partialStart = partialStart.plus(REPORT_INTERVAL);
            }
        }

        Duration totalElapsed = Duration.between(start, Instant.now());
        threadResult.flushFinalResult(totalElapsed);
    }

    @FunctionalInterface
    public interface TestFunc {
        /**
         * Execute one operation. Returns elapsed nanos. May throw
         * DoNotRegisterException.
         */
        long execute(TestThreadResult rb) throws Exception;
    }

    /**
     * Sentinel exception: operation completed but should not be recorded in stats.
     */
    public static class DoNotRegisterException extends Exception {
        public DoNotRegisterException() {
            super(null, null, true, false);
        }
    }

    /** Tracks iterations over a workload with termination conditions. */
    private static class TestIterator {
        private final WorkloadGenerator workload;
        private final long maxIterations;
        private final AtomicBoolean stopAll;
        private long iteration;

        TestIterator(WorkloadGenerator workload, long maxIterations, AtomicBoolean stopAll) {
            this.workload = workload;
            this.maxIterations = maxIterations;
            this.stopAll = stopAll;
            this.iteration = 0;
        }

        boolean isDone() {
            if (stopAll.get())
                return true;
            if (workload.isDone()) {
                if (maxIterations != 0 && iteration + 1 >= maxIterations)
                    return true;
                workload.restart();
                iteration++;
            }
            return false;
        }
    }

    // ======================== MODE: WRITE ========================

    public void doWrites(TestThreadResult result, WorkloadGenerator workload,
            RateLimiter rl, long iterations) {
        String cql = lwtEnabled
                ? String.format("INSERT INTO %s.%s (pk, ck, v) VALUES (?, ?, ?) IF NOT EXISTS",
                        keyspaceName, tableName)
                : String.format("INSERT INTO %s.%s (pk, ck, v) VALUES (?, ?, ?)",
                        keyspaceName, tableName);
        PreparedStatement ps = prepareOnce(cql);

        runTest(result, workload, rl, iterations, rb -> {
            long pk = workload.nextPartitionKey();
            long ck = workload.nextClusteringKey();
            byte[] v = DataGenerator.generate(pk, ck, clusteringRowSizeDist.generate(), validateData);

            BoundStatement bound = ps.bind(pk, ck, java.nio.ByteBuffer.wrap(v))
                    .setConsistencyLevel(consistencyLevel);
            if (lwtEnabled)
                bound = bound.setSerialConsistencyLevel(DefaultConsistencyLevel.LOCAL_SERIAL);
            return executeWriteWithRetry(bound, cql, () -> {
                rb.incOps();
                rb.incRows();
            });
        });
    }

    // ======================== MODE: BATCHED WRITE ========================

    public void doBatchedWrites(TestThreadResult result, WorkloadGenerator workload,
            RateLimiter rl, long iterations) {
        String cql = lwtEnabled
                ? String.format("INSERT INTO %s.%s (pk, ck, v) VALUES (?, ?, ?) IF NOT EXISTS",
                        keyspaceName, tableName)
                : String.format("INSERT INTO %s.%s (pk, ck, v) VALUES (?, ?, ?)",
                        keyspaceName, tableName);
        // LWT conditional batches require LOGGED type
        DefaultBatchType batchType = lwtEnabled ? DefaultBatchType.LOGGED : DefaultBatchType.UNLOGGED;
        PreparedStatement ps = prepareOnce(cql);

        runTest(result, workload, rl, iterations, rb -> {
            if (stopAll.get())
                throw new DoNotRegisterException();

            long currentPk = workload.nextPartitionKey();
            BatchStatementBuilder batchBuilder = BatchStatement.builder(batchType);
            int batchSize = 0;

            while (!workload.isPartitionDone() && batchSize < rowsPerRequest) {
                if (stopAll.get())
                    throw new DoNotRegisterException();
                long ck = workload.nextClusteringKey();
                byte[] v = DataGenerator.generate(currentPk, ck,
                        clusteringRowSizeDist.generate(), validateData);
                batchBuilder.addStatement(ps.bind(currentPk, ck, java.nio.ByteBuffer.wrap(v)));
                batchSize++;
            }

            if (batchSize == 0)
                throw new DoNotRegisterException();

            final int finalBatchSize = batchSize;
            BatchStatementBuilder b = batchBuilder.setConsistencyLevel(consistencyLevel);
            if (lwtEnabled)
                b = b.setSerialConsistencyLevel(DefaultConsistencyLevel.LOCAL_SERIAL);
            BatchStatement batch = b.build();
            String desc = String.format("BATCH pk=%d batchSize=%d", currentPk, batchSize);
            return executeWriteWithRetry(batch, desc, () -> {
                rb.incOps();
                rb.addRows(finalBatchSize);
            });
        });
    }

    // ======================== MODE: COUNTER UPDATE ========================

    public void doCounterUpdates(TestThreadResult result, WorkloadGenerator workload,
            RateLimiter rl, long iterations) {
        String cql = "UPDATE " + keyspaceName + "." + counterTableName +
                " SET c1=c1+?,c2=c2+?,c3=c3+?,c4=c4+?,c5=c5+? WHERE pk=? AND ck=?";
        PreparedStatement ps = prepareOnce(cql);

        runTest(result, workload, rl, iterations, rb -> {
            long pk = workload.nextPartitionKey();
            long ck = workload.nextClusteringKey();
            BoundStatement bound = ps.bind(ck, ck + 1, ck + 2, ck + 3, ck + 4, pk, ck)
                    .setConsistencyLevel(consistencyLevel);
            return executeWriteWithRetry(bound, "counter update pk=" + pk + " ck=" + ck, () -> {
                rb.incOps();
                rb.incRows();
            });
        });
    }

    // ======================== MODE: READ ========================

    public void doReads(TestThreadResult result, WorkloadGenerator workload,
            RateLimiter rl, long iterations) {
        doReadsFromTable(tableName, result, workload, rl, iterations, false);
    }

    public void doCounterReads(TestThreadResult result, WorkloadGenerator workload,
            RateLimiter rl, long iterations) {
        doReadsFromTable(counterTableName, result, workload, rl, iterations, false);
    }

    private void doReadsFromTable(String table, TestThreadResult result,
            WorkloadGenerator workload, RateLimiter rl,
            long iterations, boolean forMixed) {
        int[] counter = { 0 };
        int numOrderings = selectOrderByParsed.length;

        runTest(result, workload, rl, iterations, rb -> {
            counter[0]++;
            String orderBy = selectOrderByParsed[counter[0] % numOrderings];
            String cql = buildReadQuery(table, orderBy);
            PreparedStatement ps = session.prepare(SimpleStatement.newInstance(cql)
                    .setConsistencyLevel(consistencyLevel));

            BoundStatement bound;
            if (inRestriction) {
                long pk = workload.nextPartitionKey();
                Object[] args = new Object[rowsPerRequest + 1];
                args[0] = pk;
                for (int i = 1; i <= rowsPerRequest; i++) {
                    args[i] = workload.isPartitionDone() ? 0L : workload.nextClusteringKey();
                }
                bound = ps.bind(args).setConsistencyLevel(consistencyLevel);
            } else if (noLowerBound) {
                bound = ps.bind(workload.nextPartitionKey()).setConsistencyLevel(consistencyLevel);
            } else {
                long pk = workload.nextPartitionKey();
                long ck = workload.nextClusteringKey();
                if (provideUpperBound) {
                    bound = ps.bind(pk, ck, ck + rowsPerRequest).setConsistencyLevel(consistencyLevel);
                } else {
                    bound = ps.bind(pk, ck).setConsistencyLevel(consistencyLevel);
                }
            }

            // LWT serial read: override consistency to LOCAL_SERIAL
            if (lwtEnabled)
                bound = bound.setConsistencyLevel(DefaultConsistencyLevel.LOCAL_SERIAL);

            return executeReadWithRetry(table, bound, cql, rb, forMixed);
        });
    }

    private String buildReadQuery(String table, String orderBy) {
        String fields = table.equals(tableName) ? "pk, ck, v" : "pk, ck, c1, c2, c3, c4, c5";
        String bypassStr = bypassCache ? " BYPASS CACHE" : "";

        if (inRestriction) {
            StringBuilder placeholders = new StringBuilder();
            for (int i = 0; i < rowsPerRequest; i++) {
                if (i > 0)
                    placeholders.append(",");
                placeholders.append("?");
            }
            return String.format("SELECT %s FROM %s.%s WHERE pk=? AND ck IN (%s) %s%s",
                    fields, keyspaceName, table, placeholders, orderBy, bypassStr);
        } else if (provideUpperBound) {
            return String.format("SELECT %s FROM %s.%s WHERE pk=? AND ck>=? AND ck<? %s%s",
                    fields, keyspaceName, table, orderBy, bypassStr);
        } else if (noLowerBound) {
            return String.format("SELECT %s FROM %s.%s WHERE pk=? %s LIMIT %d%s",
                    fields, keyspaceName, table, orderBy, rowsPerRequest, bypassStr);
        } else {
            return String.format("SELECT %s FROM %s.%s WHERE pk=? AND ck>=? %s LIMIT %d%s",
                    fields, keyspaceName, table, orderBy, rowsPerRequest, bypassStr);
        }
    }

    private long executeReadWithRetry(String table, BoundStatement bound, String queryStr,
            TestThreadResult rb, boolean forMixed) throws Exception {
        bound = withTimeout(bound, lwtEnabled ? casTimeout : readTimeout);
        for (int attempt = 0;; attempt++) {
            long startNanos = System.nanoTime();
            try {
                ResultSet rs = session.execute(bound);
                long elapsed = System.nanoTime() - startNanos;

                for (Row row : rs) {
                    rb.incRows();
                    if (validateData && table.equals(tableName)) {
                        long resPk = row.getLong(0);
                        long resCk = row.getLong(1);
                        byte[] val = row.getByteBuffer(2) != null
                                ? getBytes(row.getByteBuffer(2))
                                : new byte[0];
                        String err = DataGenerator.validate(resPk, resCk, val);
                        if (err != null) {
                            rb.incErrors();
                            LOG.error("Data corruption pk={} ck={}: {}", resPk, resCk, err);
                        }
                    } else if (validateData && !table.equals(tableName)) {
                        long resCk = row.getLong(1);
                        long c1 = row.getLong(2), c2 = row.getLong(3), c3 = row.getLong(4),
                                c4 = row.getLong(5), c5 = row.getLong(6);
                        long updateNum = resCk == 0 ? c2 : c1 / resCk;
                        if (c1 != resCk * updateNum || c2 != c1 + updateNum || c3 != c1 + updateNum * 2
                                || c4 != c1 + updateNum * 3 || c5 != c1 + updateNum * 4) {
                            rb.incErrors();
                            LOG.error("Counter data corruption ck={} c1={} c2={} c3={} c4={} c5={}",
                                    resCk, c1, c2, c3, c4, c5);
                        }
                    }
                }

                rb.incOps();
                if (forMixed)
                    rb.recordReadRawLatency(Duration.ofNanos(elapsed));
                return elapsed;

            } catch (Exception e) {
                if ("sb".equals(retryHandler)) {
                    long elapsed = System.nanoTime() - startNanos;
                    if (attempt >= retryNumber)
                        throw e;
                    Duration sleep = getExponentialBackoff(attempt);
                    LOG.warn("Read retry #{} sleeping {}: {}", attempt, sleep, queryStr);
                    Thread.sleep(sleep.toMillis());
                } else {
                    throw e;
                }
            }
        }
    }

    // ======================== MODE: SCAN ========================

    public void doScanTable(TestThreadResult result, WorkloadGenerator workload,
            RateLimiter rl, long iterations) {
        String cql = String.format("SELECT * FROM %s.%s WHERE token(pk)>=? AND token(pk)<=?",
                keyspaceName, tableName);
        PreparedStatement ps = prepareOnce(cql);

        runTest(result, workload, rl, iterations, rb -> {
            TokenRange range = workload.nextTokenRange();
            BoundStatement bound = withTimeout(
                    ps.bind(range.start(), range.end()).setConsistencyLevel(consistencyLevel),
                    readTimeout);

            for (int attempt = 0;; attempt++) {
                long startNanos = System.nanoTime();
                try {
                    ResultSet rs = session.execute(bound);
                    for (Row row : rs)
                        rb.incRows();
                    long elapsed = System.nanoTime() - startNanos;
                    rb.incOps();
                    return elapsed;
                } catch (Exception e) {
                    if ("sb".equals(retryHandler) && attempt < retryNumber) {
                        Thread.sleep(getExponentialBackoff(attempt).toMillis());
                    } else {
                        throw e;
                    }
                }
            }
        });
    }

    // ======================== MODE: MIXED ========================

    public void doMixed(TestThreadResult result, WorkloadGenerator workload,
            RateLimiter rl, long iterations) {
        String writeCql = lwtEnabled
                ? String.format("INSERT INTO %s.%s (pk, ck, v) VALUES (?, ?, ?) IF NOT EXISTS",
                        keyspaceName, tableName)
                : String.format("INSERT INTO %s.%s (pk, ck, v) VALUES (?, ?, ?)",
                        keyspaceName, tableName);
        PreparedStatement writePs = prepareOnce(writeCql);

        runTest(result, workload, rl, iterations, rb -> {
            long opCount = globalMixedOpCount.incrementAndGet();
            Instant expectedStart = rl.expected();
            if (expectedStart == Instant.MIN)
                expectedStart = Instant.now();

            if (opCount % 2 == 0) {
                // Write
                long pk = workload.nextPartitionKey();
                long ck = workload.nextClusteringKey();
                byte[] v = DataGenerator.generate(pk, ck, clusteringRowSizeDist.generate(), validateData);
                BoundStatement bound = writePs.bind(pk, ck, java.nio.ByteBuffer.wrap(v))
                        .setConsistencyLevel(consistencyLevel);
                if (lwtEnabled)
                    bound = bound.setSerialConsistencyLevel(DefaultConsistencyLevel.LOCAL_SERIAL);
                long elapsed = executeWriteWithRetry(bound, "mixed write", () -> {
                    rb.incOps();
                    rb.incRows();
                });
                rb.recordWriteRawLatency(Duration.ofNanos(elapsed));
                rb.recordWriteCoFixedLatency(Duration.between(expectedStart, Instant.now()));
                return elapsed;
            } else {
                // Read
                int[] counter = { 0 };
                String orderBy = selectOrderByParsed[counter[0]++ % selectOrderByParsed.length];
                String readCql = buildReadQuery(tableName, orderBy);
                PreparedStatement readPs = session.prepare(SimpleStatement.newInstance(readCql)
                        .setConsistencyLevel(consistencyLevel));

                long pk = workload.nextPartitionKey();
                long ck = workload.nextClusteringKey();
                BoundStatement bound = provideUpperBound
                        ? readPs.bind(pk, ck, ck + rowsPerRequest).setConsistencyLevel(consistencyLevel)
                        : readPs.bind(pk, ck).setConsistencyLevel(consistencyLevel);
                // LWT serial read
                if (lwtEnabled)
                    bound = bound.setConsistencyLevel(DefaultConsistencyLevel.LOCAL_SERIAL);

                long elapsed = executeReadWithRetry(tableName, bound, readCql, rb, true);
                rb.recordReadCoFixedLatency(Duration.between(expectedStart, Instant.now()));
                return elapsed;
            }
        });
    }

    // ======================== HELPERS ========================

    private synchronized PreparedStatement prepareOnce(String cql) {
        // Simple cache: for the main CQL statements that are reused
        if (cql.startsWith("INSERT INTO " + keyspaceName + "." + tableName)) {
            if (writePS == null)
                writePS = session.prepare(SimpleStatement.newInstance(cql));
            return writePS;
        }
        if (cql.startsWith("UPDATE " + keyspaceName + "." + counterTableName)) {
            if (counterUpdatePS == null)
                counterUpdatePS = session.prepare(SimpleStatement.newInstance(cql));
            return counterUpdatePS;
        }
        if (cql.startsWith("SELECT * FROM " + keyspaceName + "." + tableName + " WHERE token")) {
            if (scanPS == null)
                scanPS = session.prepare(SimpleStatement.newInstance(cql));
            return scanPS;
        }
        return session.prepare(SimpleStatement.newInstance(cql));
    }

    /**
     * Executes a statement with application-level retry (when retryHandler=sb).
     * Returns elapsed nanoseconds. Runs onSuccess afterward to update thread result
     * counters.
     */
    /** Applies a per-statement timeout to a BoundStatement; only overrides when duration > 0. */
    private BoundStatement withTimeout(BoundStatement stmt, Duration timeout) {
        if (timeout != null && !timeout.isZero()) {
            return stmt.setTimeout(timeout);
        }
        return stmt;
    }

    /** Applies a per-statement timeout to any Statement; only overrides when duration > 0. */
    @SuppressWarnings({"unchecked","rawtypes"})
    private <S extends Statement<S>> S withTimeoutGeneric(Statement stmt, Duration timeout) {
        if (timeout != null && !timeout.isZero()) {
            return (S) stmt.setTimeout(timeout);
        }
        return (S) stmt;
    }

    private long executeWriteWithRetry(Statement<?> stmt, String queryDesc, Runnable onSuccess)
            throws Exception {
        Duration t = lwtEnabled ? casTimeout : writeTimeout;
        if (t != null && !t.isZero()) {
            stmt = withTimeoutGeneric(stmt, t);
        }
        return executeWithRetry(stmt, queryDesc, onSuccess);
    }

    private long executeWithRetry(Statement<?> stmt, String queryDesc, Runnable onSuccess)
            throws Exception {
        for (int attempt = 0;; attempt++) {
            long startNanos = System.nanoTime();
            try {
                session.execute(stmt);
                long elapsed = System.nanoTime() - startNanos;
                onSuccess.run();
                return elapsed;
            } catch (Exception e) {
                if ("sb".equals(retryHandler) && attempt < retryNumber) {
                    Duration sleep = getExponentialBackoff(attempt);
                    LOG.warn("Retry #{} sleeping {} for: {}", attempt, sleep, queryDesc);
                    Thread.sleep(sleep.toMillis());
                } else {
                    throw e;
                }
            }
        }
    }

    private Duration getExponentialBackoff(int attempt) {
        long minMs = retryMin.toMillis();
        long maxMs = retryMax.toMillis();
        if (minMs <= 0)
            minMs = 100;
        if (maxMs <= 0)
            maxMs = 10_000;
        double jitter = (Math.random() - 0.5) * minMs;
        long ms = (long) (minMs * Math.pow(2, attempt) + jitter);
        return Duration.ofMillis(Math.min(ms, maxMs));
    }

    private static byte[] getBytes(java.nio.ByteBuffer buf) {
        byte[] bytes = new byte[buf.remaining()];
        buf.duplicate().get(bytes);
        return bytes;
    }
}
