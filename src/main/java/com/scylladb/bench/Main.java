package com.scylladb.bench;

import com.scylladb.bench.usermode.UserModeRunner;
import com.scylladb.bench.usermode.UserProfile;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.scylladb.bench.results.ResultConfiguration;
import com.scylladb.bench.results.TestResults;
import com.scylladb.bench.workloads.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import javax.net.ssl.*;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * scylla-bench-java: a ScyllaDB benchmarking tool using the ScyllaDB Java
 * Driver.
 *
 * A Java port of https://github.com/scylladb/scylla-bench using
 * https://github.com/scylladb/java-driver.
 *
 * Usage:
 * java -jar scylla-bench-java.jar -mode write -workload sequential -nodes
 * 127.0.0.1
 *
 * To build with a specific driver version:
 * mvn package -Dscylla.driver.version=4.18.0.0
 */
@Command(name = "scylla-bench-java", mixinStandardHelpOptions = true, versionProvider = Main.VersionProvider.class, description = {
        "ScyllaDB benchmarking tool using the ScyllaDB Java Driver.",
        "Port of https://github.com/scylladb/scylla-bench",
        "",
        "Driver version is selected at build time:",
        "  mvn package -Dscylla.driver.version=4.18.0.0"
}, sortOptions = false)
public class Main implements Callable<Integer> {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    // ==================== MODE & WORKLOAD ====================

    @Option(names = { "-mode",
            "--mode" }, required = false, description = "Operating mode: write, counter_update, read, counter_read, scan, mixed, user")
    public String mode = "";

    @Option(names = { "-workload", "--workload" }, description = "Workload: sequential, uniform, timeseries (not used in user mode)")
    public String workload = "";

    @Option(names = { "-profile", "--profile" }, description = "Path to YAML profile file for user mode (required when -mode user)")
    public String profilePath = "";

    // ==================== CLUSTER CONNECTION ====================

    @Option(names = { "-nodes", "--nodes" }, description = "Comma-separated contact nodes (default: 127.0.0.1)")
    public String nodes = "127.0.0.1";

    @Option(names = { "-port", "--port" }, description = "CQL native port (default: 9042)")
    public int port = 9042;

    @Option(names = { "-username", "--username" }, description = "CQL username for authentication")
    public String username = "";

    @Option(names = { "-password", "--password" }, description = "CQL password for authentication", interactive = false)
    public String password = "";

    @Option(names = { "-datacenter", "--datacenter" }, description = "Local datacenter for DC-aware routing")
    public String datacenter = "datacenter1";

    @Option(names = { "-connection-count",
            "--connection-count" }, description = "Number of connections per host (default: 4)")
    public int connectionCount = 4;

    // ==================== TLS ====================

    @Option(names = { "-tls", "--tls" }, description = "Enable TLS encryption")
    public boolean tlsEncryption = false;

    @Option(names = { "-tls-host-verification",
            "--tls-host-verification" }, description = "Verify server TLS certificate hostname")
    public boolean hostVerification = false;

    @Option(names = { "-tls-ca-cert-file",
            "--tls-ca-cert-file" }, description = "Path to CA certificate file (PEM) for TLS")
    public String caCertFile = "";

    @Option(names = { "-tls-client-cert-file",
            "--tls-client-cert-file" }, description = "Path to client certificate file (PEM) for mutual TLS")
    public String clientCertFile = "";

    @Option(names = { "-tls-client-key-file",
            "--tls-client-key-file" }, description = "Path to client key file (PEM) for mutual TLS")
    public String clientKeyFile = "";

    // ==================== BENCHMARK CONTROL ====================

    @Option(names = { "-consistency-level",
            "--consistency-level" }, description = "Consistency level: any, one, two, three, quorum, all, local_quorum, each_quorum, local_one (default: quorum)")
    public String consistencyLevelStr = "quorum";

    @Option(names = { "-replication-factor",
            "--replication-factor" }, description = "Keyspace replication factor (default: 1)")
    public int replicationFactor = 1;

    @Option(names = { "-concurrency",
            "--concurrency" }, description = "Number of parallel worker threads (default: 16)")
    public int concurrency = 16;

    @Option(names = { "-max-rate",
            "--max-rate" }, description = "Maximum request rate in ops/s, 0 = unlimited (default: 0)")
    public int maximumRate = 0;

    @Option(names = { "-duration", "--duration" }, description = "Test duration, e.g. 30s, 5m, 1h (0 = unlimited)")
    public String durationStr = "0";

    @Option(names = { "-iterations",
            "--iterations" }, description = "Number of workload iterations, 0 = unlimited (default: 1)")
    public long iterations = 1;

    @Option(names = {"-timeout", "--timeout"}, description = "Default request timeout (default: 10s). Overridden by -read-timeout/-write-timeout/-cas-timeout if set.")
    public String timeoutStr = "10s";

    @Option(names = {"-read-timeout", "--read-timeout"}, description = "Timeout for SELECT queries (default: 10s)")
    public String readTimeoutStr = "10s";

    @Option(names = {"-write-timeout", "--write-timeout"}, description = "Timeout for INSERT/UPDATE/DELETE queries (default: 5s)")
    public String writeTimeoutStr = "5s";

    @Option(names = {"-cas-timeout", "--cas-timeout"}, description = "Timeout for lightweight transaction (CAS) queries (default: 2s)")
    public String casTimeoutStr = "2s";

    // ==================== SCHEMA ====================

    @Option(names = { "-keyspace", "--keyspace" }, description = "Keyspace name (default: scylla_bench)")
    public String keyspaceName = "scylla_bench";

    @Option(names = { "-table", "--table" }, description = "Table name (default: test)")
    public String tableName = "test";

    @Option(names = { "-truncate-table",
            "--truncate-table" }, description = "Truncate the table before running write/counter_update")
    public boolean truncateTable = false;

    @Option(names = { "-validate-data",
            "--validate-data" }, description = "Write meaningful data and validate during reads")
    public boolean validateData = false;

    @Option(names = { "-lwt", "--lwt" }, description = "Enable lightweight transactions: yes or no (default: no)")
    public String lwt = "no";

    // ==================== DATA SHAPE ====================

    @Option(names = { "-partition-count", "--partition-count" }, description = "Number of partitions (default: 10000)")
    public long partitionCount = 10_000;

    @Option(names = { "-partition-offset",
            "--partition-offset" }, description = "Starting partition key offset (default: 0)")
    public long partitionOffset = 0;

    @Option(names = { "-clustering-row-count",
            "--clustering-row-count" }, description = "Clustering rows per partition (default: 100)")
    public long clusteringRowCount = 100;

    @Option(names = { "-clustering-row-size",
            "--clustering-row-size" }, description = "Clustering row size: fixed:N or uniform:MIN..MAX (default: fixed:4)")
    public String clusteringRowSizeStr = "fixed:4";

    @Option(names = { "-rows-per-request",
            "--rows-per-request" }, description = "Rows per request / batch (default: 1)")
    public int rowsPerRequest = 1;

    // ==================== READ OPTIONS ====================

    @Option(names = { "-provide-upper-bound",
            "--provide-upper-bound" }, description = "Provide upper bound ck in read queries")
    public boolean provideUpperBound = false;

    @Option(names = { "-in-restriction", "--in-restriction" }, description = "Use IN restriction in read queries")
    public boolean inRestriction = false;

    @Option(names = { "-no-lower-bound",
            "--no-lower-bound" }, description = "Do not provide lower bound ck in read queries")
    public boolean noLowerBound = false;

    @Option(names = { "-select-order-by",
            "--select-order-by" }, description = "ORDER BY for reads: none, asc, desc, or comma-separated e.g. none,asc (default: none)")
    public String selectOrderBy = "none";

    @Option(names = { "-bypass-cache", "--bypass-cache" }, description = "Add BYPASS CACHE to read queries")
    public boolean bypassCache = false;

    @Option(names = { "-page-size", "--page-size" }, description = "Paging size for reads (default: 1000)")
    public int pageSize = 1000;

    // ==================== SCAN ====================

    @Option(names = { "-range-count",
            "--range-count" }, description = "Number of token ranges for scan mode (default: 1). " +
                    "Recommended: nodes × cores × 300")
    public int rangeCount = 1;

    // ==================== TIMESERIES ====================

    @Option(names = { "-write-rate", "--write-rate" }, description = "Per-partition write rate for timeseries reads")
    public long writeRate = 0;

    @Option(names = { "-start-timestamp",
            "--start-timestamp" }, description = "Start timestamp in nanoseconds for timeseries reads")
    public long startTimestampNanos = 0;

    @Option(names = { "-distribution",
            "--distribution" }, description = "Key distribution for timeseries reads: uniform, hnormal (default: uniform)")
    public String distribution = "uniform";

    // ==================== LATENCY ====================

    @Option(names = { "-measure-latency",
            "--measure-latency" }, description = "Measure per-request latency (default: true)", negatable = true, defaultValue = "true")
    public boolean measureLatency = true;

    @Option(names = { "-latency-type",
            "--latency-type" }, description = "Latency type for periodic display: raw, fixed-coordinated-omission (default: raw)")
    public String latencyType = "raw";

    @Option(names = { "-hdr-latency-file",
            "--hdr-latency-file" }, description = "File path for HDR histogram log output")
    public String hdrLatencyFile = "";

    @Option(names = { "-hdr-latency-units",
            "--hdr-latency-units" }, description = "HDR log time units: ns, us, ms (default: ns)")
    public String hdrLatencyUnits = "ns";

    @Option(names = { "-hdr-latency-sig",
            "--hdr-latency-sig" }, description = "HDR histogram significant figures 1-5 (default: 3)")
    public int hdrLatencySigFig = 3;

    // ==================== ERRORS & RETRIES ====================

    @Option(names = { "-retry-number", "--retry-number" }, description = "Number of retries per request (default: 10)")
    public int retryNumber = 10;

    @Option(names = { "-retry-interval",
            "--retry-interval" }, description = "Retry interval: single value '1s' or min,max '100ms,5s' (default: 80ms,1s)")
    public String retryInterval = "80ms,1s";

    @Option(names = { "-retry-handler",
            "--retry-handler" }, description = "Retry handler: sb (application-level) or driver (driver policy) (default: sb)")
    public String retryHandler = "sb";

    @Option(names = { "-error-at-row-limit",
            "--error-at-row-limit" }, description = "Max consecutive errors in one thread before stopping (0=unlimited)")
    public int maxErrorsAtRow = 0;

    @Option(names = { "-error-limit",
            "--error-limit" }, description = "Total error count before stopping (0=unlimited)")
    public int maxErrors = 0;

    // ==================== COMPRESSION ====================

    @Option(names = { "-client-compression",
            "--client-compression" }, description = "Enable client↔coordinator compression (default: true)", negatable = true, defaultValue = "true")
    public boolean clientCompression = true;

    // ==================== VERSION INFO ====================

    @Option(names = { "-version", "--version" }, description = "Show version information and exit")
    public boolean showVersion = false;

    @Option(names = { "-driver-version",
            "--driver-version" }, description = "Show the ScyllaDB Java driver version and exit")
    public boolean showDriverVersion = false;

    // ==================== PARSED VALUES (set during validation)
    // ====================

    public DefaultConsistencyLevel parsedConsistencyLevel;
    public Distribution parsedClusteringRowSize;
    public Duration timeout;
    public Duration readTimeout;
    public Duration writeTimeout;
    public Duration casTimeout;
    public Duration testDuration;
    public Duration parsedRetryMin;
    public Duration parsedRetryMax;
    public boolean lwtEnabled;
    public UserProfile parsedProfile;

    // ==================== ENTRY POINT ====================

    public static void main(String[] args) {
        // Suppress noisy driver INFO logs when not explicitly configured
        System.setProperty("logback.configurationFile", "logback.xml");
        System.exit(new CommandLine(new Main()).execute(args));
    }

    @Override
    public Integer call() throws Exception {
        // Handle version flags early
        if (showVersion || showDriverVersion) {
            new VersionProvider().getVersion(); // printed by picocli already
            return 0;
        }

        // ----- Parse and validate options -----
        parseAndValidate();

        // ----- Setup ScyllaDB session -----
        CqlSession session = buildSession();

        AtomicBoolean stopAll = new AtomicBoolean(false);

        // SIGINT handler: first interrupt stops the test, second kills
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\ninterrupted");
            stopAll.set(true);
        }));

        if (!testDuration.isZero()) {
            Thread timer = new Thread(() -> {
                try {
                    Thread.sleep(testDuration.toMillis());
                    stopAll.set(true);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }, "duration-timer");
            timer.setDaemon(true);
            timer.start();
        }

        Instant startTime = startTimestampNanos != 0
                ? Instant.ofEpochSecond(0, startTimestampNanos)
                : Instant.now();

        // ----- Print configuration -----
        printConfig(startTime);

        // ----- Setup result tracking -----
        ResultConfiguration resCfg = buildResultConfig();
        System.out.printf("Hdr memory consumption:\t%d bytes%n", resCfg.estimateHdrMemory());

        TestResults testResults = new TestResults(resCfg);
        testResults.init(concurrency);

        // ----- Create runner & prepare schema -----
        BenchmarkRunner runner = new BenchmarkRunner(session, this, stopAll);

        if ("user".equals(mode)) {
            // ----- User mode path -----
            UserModeRunner userRunner = new UserModeRunner(
                    session, parsedProfile, parsedConsistencyLevel,
                    readTimeout, writeTimeout, casTimeout, lwtEnabled);
            userRunner.prepareSchema(replicationFactor, truncateTable);
            runner.runConcurrently(maximumRate, testResults,
                    (threadId, ttr, rl) -> runner.runTest(ttr,
                            new com.scylladb.bench.workloads.UniformWorkload(
                                    threadId, Math.max(partitionCount, 1), partitionOffset,
                                    Math.max(clusteringRowCount, 1)),
                            rl, iterations,
                            rb -> userRunner.executeOp(userRunner.selectOperation(), rb)));
        } else {
            runner.prepareDatabase(replicationFactor, truncateTable);
            // ----- Launch benchmark -----
            runner.runConcurrently(maximumRate, testResults,
                    (threadId, ttr, rl) -> runModeForThread(runner, threadId, ttr, rl, startTime));
        }

        // ----- Collect and print results -----
        testResults.collectResults();
        testResults.printTotalResults();

        session.close();
        return testResults.getFinalStatus();
    }

    /** Executes the chosen mode for a single worker thread. */
    private void runModeForThread(BenchmarkRunner runner, int threadId,
            com.scylladb.bench.results.TestThreadResult ttr,
            RateLimiter rl, Instant startTime) {
        WorkloadGenerator wg = buildWorkload(threadId, startTime);

        switch (mode) {
            case "user":
                throw new IllegalStateException("user mode should be handled before runModeForThread");
            case "write":
                if (rowsPerRequest == 1)
                    runner.doWrites(ttr, wg, rl, iterations);
                else
                    runner.doBatchedWrites(ttr, wg, rl, iterations);
                break;
            case "counter_update":
                runner.doCounterUpdates(ttr, wg, rl, iterations);
                break;
            case "read":
                runner.doReads(ttr, wg, rl, iterations);
                break;
            case "counter_read":
                runner.doCounterReads(ttr, wg, rl, iterations);
                break;
            case "scan":
                runner.doScanTable(ttr, wg, rl, iterations);
                break;
            case "mixed":
                runner.doMixed(ttr, wg, rl, iterations);
                break;
            default:
                throw new IllegalArgumentException("Unknown mode: " + mode);
        }
    }

    /** Constructs the workload generator for a given thread. */
    private WorkloadGenerator buildWorkload(int threadId, Instant startTime) {
        switch (workload) {
            case "sequential": {
                long totalRows = partitionCount * clusteringRowCount;
                long rowCount = totalRows / concurrency;
                long remainder = totalRows % concurrency;
                long extra = (threadId < remainder) ? 1 : 0;
                long partialOffset = (threadId < remainder) ? threadId : remainder;
                long rowOffset = partitionOffset * clusteringRowCount
                        + threadId * rowCount + partialOffset;
                return new SequentialWorkload(rowOffset, rowCount + extra, clusteringRowCount);
            }
            case "uniform":
                return new UniformWorkload(threadId, partitionCount, partitionOffset, clusteringRowCount);
            case "timeseries":
                switch (mode) {
                    case "write":
                    case "mixed":
                        return new TimeSeriesWriteWorkload(threadId, concurrency,
                                partitionCount, partitionOffset, clusteringRowCount,
                                startTime, maximumRate / concurrency);
                    case "read":
                        int dist = "hnormal".equals(distribution)
                                ? TimeSeriesReadWorkload.DISTRIBUTION_HNORMAL
                                : TimeSeriesReadWorkload.DISTRIBUTION_UNIFORM;
                        return new TimeSeriesReadWorkload(threadId, concurrency,
                                partitionCount, partitionOffset, clusteringRowCount,
                                writeRate, dist, startTime);
                    default:
                        throw new IllegalArgumentException("timeseries unsupported with mode: " + mode);
                }
            case "scan": {
                int perThread = rangeCount / concurrency;
                int myOffset = perThread * threadId;
                int myCount = (threadId + 1 == concurrency)
                        ? rangeCount - myOffset
                        : perThread;
                return new ScanWorkload(rangeCount, myOffset, myCount);
            }
            default:
                throw new IllegalArgumentException("Unknown workload: " + workload);
        }
    }

    // ==================== SESSION BUILDER ====================

    private CqlSession buildSession() throws Exception {
        // Use -timeout as fallback base; per-type timeouts take effect on individual statements
        Duration baseTimeout = timeout;
        DriverConfigLoader driverConfig = DriverConfigLoader.programmaticBuilder()
                .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, connectionCount)
                .withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, connectionCount)
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, baseTimeout)
                .withInt(DefaultDriverOption.REQUEST_PAGE_SIZE, pageSize)
                .withString(DefaultDriverOption.PROTOCOL_COMPRESSION,
                        clientCompression ? "lz4" : "none")
                .build();

        CqlSessionBuilder builder = CqlSession.builder()
                .withConfigLoader(driverConfig)
                .withLocalDatacenter(datacenter);

        // Contact points
        for (String node : nodes.split(",")) {
            String[] parts = node.trim().split(":");
            String host = parts[0];
            int p = parts.length > 1 ? Integer.parseInt(parts[1]) : port;
            builder.addContactPoint(new InetSocketAddress(host, p));
        }

        // Authentication
        if (!username.isEmpty() && !password.isEmpty()) {
            builder.withAuthCredentials(username, password);
        }

        // TLS
        if (tlsEncryption) {
            builder.withSslContext(buildSslContext());
        }

        return builder.build();
    }

    private SSLContext buildSslContext() throws Exception {
        KeyManager[] keyManagers = null;
        TrustManager[] trustManagers;

        // Custom CA certificate
        if (!caCertFile.isEmpty()) {
            KeyStore ts = KeyStore.getInstance(KeyStore.getDefaultType());
            ts.load(null, null);
            java.security.cert.CertificateFactory cf = java.security.cert.CertificateFactory.getInstance("X.509");
            try (FileInputStream fis = new FileInputStream(caCertFile)) {
                X509Certificate cert = (X509Certificate) cf.generateCertificate(fis);
                ts.setCertificateEntry("ca", cert);
            }
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(
                    TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ts);
            trustManagers = tmf.getTrustManagers();
        } else if (!hostVerification) {
            // Trust all - only use when host verification is explicitly disabled
            trustManagers = new TrustManager[] { new X509TrustManager() {
                public X509Certificate[] getAcceptedIssuers() {
                    return new X509Certificate[0];
                }

                public void checkClientTrusted(X509Certificate[] c, String a) {
                }

                public void checkServerTrusted(X509Certificate[] c, String a) {
                }
            } };
        } else {
            trustManagers = null; // Use JVM default trust store
        }

        // Client certificate (mutual TLS)
        if (!clientKeyFile.isEmpty() && !clientCertFile.isEmpty()) {
            // Load PKCS12 or use a PEM-based approach via bouncycastle if needed.
            // For simplicity, use system keystore fallback:
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(
                    KeyManagerFactory.getDefaultAlgorithm());
            KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
            ks.load(null, null);
            kmf.init(ks, new char[0]);
            keyManagers = kmf.getKeyManagers();
            LOG.warn("Client certificate from PEM files requires manual keystore setup. " +
                    "Use a PKCS12 keystore with -Djavax.net.ssl.keyStore instead.");
        }

        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(keyManagers, trustManagers, new SecureRandom());
        return ctx;
    }

    // ==================== VALIDATION ====================

    private void parseAndValidate() {
        // Consistency level
        parsedConsistencyLevel = parseConsistency(consistencyLevelStr);

        // Clustering row size distribution
        parsedClusteringRowSize = Distribution.parse(clusteringRowSizeStr);

        // Timeout
        timeout = parseDuration(timeoutStr, "timeout");
        readTimeout  = parseDuration(readTimeoutStr,  "read-timeout");
        writeTimeout = parseDuration(writeTimeoutStr, "write-timeout");
        casTimeout   = parseDuration(casTimeoutStr,   "cas-timeout");

        // Test duration
        testDuration = parseDuration(durationStr, "duration");

        // Retry interval
        String[] parts = retryInterval.replaceAll("\\s", "").split(",");
        if (parts.length > 2)
            throw new IllegalArgumentException("retry-interval: expected 1 or 2 values");
        parsedRetryMin = parseMillis(parts[0]);
        parsedRetryMax = parts.length == 2 ? parseMillis(parts[1]) : parsedRetryMin;

        // Workload
        if ("scan".equals(mode)) {
            if (!workload.isEmpty())
                throw new IllegalArgumentException(
                        "workload cannot be specified for scan mode");
            workload = "scan";
            if (concurrency > rangeCount) {
                System.out.printf("Adjusting concurrency from %d to %d (range-count)%n",
                        concurrency, rangeCount);
                concurrency = rangeCount;
            }
        } else if ("user".equals(mode)) {
            if (workload.isEmpty()) workload = "user";
        } else if (workload.isEmpty()) {
            throw new IllegalArgumentException("workload must be specified");
        }

        // Uniform requires a duration
        if ("uniform".equals(workload) && testDuration.isZero()) {
            throw new IllegalArgumentException("uniform workload requires -duration to be set");
        }

        // Timeseries validation
        if ("timeseries".equals(workload)) {
            if ("read".equals(mode) && writeRate == 0)
                throw new IllegalArgumentException("timeseries read requires -write-rate");
            if ("write".equals(mode) && concurrency > partitionCount)
                throw new IllegalArgumentException("timeseries write requires concurrency <= partition-count");
            if ("write".equals(mode) && maximumRate == 0)
                throw new IllegalArgumentException("timeseries write requires -max-rate");
        }

        // Read mode tweaks are mutually exclusive
        int readTweaks = (inRestriction ? 1 : 0) + (provideUpperBound ? 1 : 0) + (noLowerBound ? 1 : 0);
        if (!mode.contains("read") && readTweaks > 0)
            throw new IllegalArgumentException(
                    "-in-restriction, -no-lower-bound, -provide-upper-bound only apply to read mode");
        if (readTweaks > 1)
            throw new IllegalArgumentException(
                    "-in-restriction, -no-lower-bound, -provide-upper-bound are mutually exclusive");

        // HDR sig figs
        if (hdrLatencySigFig < 1 || hdrLatencySigFig > 5)
            throw new IllegalArgumentException("hdr-latency-sig must be 1-5");

        // Retry handler
        if (!"sb".equals(retryHandler) && !"driver".equals(retryHandler))
            throw new IllegalArgumentException("retry-handler must be 'sb' or 'driver'");

        // LWT
        switch (lwt.toLowerCase()) {
            case "yes":
                lwtEnabled = true;
                break;
            case "no":
                lwtEnabled = false;
                break;
            default:
                throw new IllegalArgumentException("lwt must be 'yes' or 'no'");
        }
        if (lwtEnabled && (mode.equals("counter_update") || mode.equals("counter_read")))
            throw new IllegalArgumentException("LWT is not supported for counter modes");

        // User mode: load profile
        if ("user".equals(mode)) {
            if (profilePath == null || profilePath.isEmpty())
                throw new IllegalArgumentException("-mode user requires -profile <path-to-yaml>");
            try {
                parsedProfile = UserModeRunner.loadProfile(profilePath);
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to load profile '" + profilePath + "': " + e.getMessage(), e);
            }
        } else {
            if (mode.isEmpty())
                throw new IllegalArgumentException("-mode is required");
        }

        // Iterations
        if (iterations > 1 && !"sequential".equals(workload) && !"scan".equals(workload) && !"user".equals(workload))
            throw new IllegalArgumentException("iterations > 1 only supported for sequential, scan, and user workloads");
    }

    private DefaultConsistencyLevel parseConsistency(String s) {
        switch (s.toLowerCase()) {
            case "any":
                return DefaultConsistencyLevel.ANY;
            case "one":
                return DefaultConsistencyLevel.ONE;
            case "two":
                return DefaultConsistencyLevel.TWO;
            case "three":
                return DefaultConsistencyLevel.THREE;
            case "quorum":
                return DefaultConsistencyLevel.QUORUM;
            case "all":
                return DefaultConsistencyLevel.ALL;
            case "local_quorum":
                return DefaultConsistencyLevel.LOCAL_QUORUM;
            case "each_quorum":
                return DefaultConsistencyLevel.EACH_QUORUM;
            case "local_one":
                return DefaultConsistencyLevel.LOCAL_ONE;
            default:
                throw new IllegalArgumentException("Unknown consistency level: " + s);
        }
    }

    private Duration parseDuration(String s, String name) {
        if ("0".equals(s))
            return Duration.ZERO;
        try {
            if (s.endsWith("ns"))
                return Duration.ofNanos(Long.parseLong(s.replace("ns", "")));
            if (s.endsWith("ms"))
                return Duration.ofMillis(Long.parseLong(s.replace("ms", "")));
            if (s.endsWith("s"))
                return Duration.ofSeconds(Long.parseLong(s.replace("s", "")));
            if (s.endsWith("m"))
                return Duration.ofMinutes(Long.parseLong(s.replace("m", "")));
            if (s.endsWith("h"))
                return Duration.ofHours(Long.parseLong(s.replace("h", "")));
            return Duration.ofSeconds(Long.parseLong(s));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Cannot parse " + name + ": " + s);
        }
    }

    private Duration parseMillis(String s) {
        s = s.trim();
        try {
            if (s.endsWith("ms"))
                return Duration.ofMillis(Long.parseLong(s.replace("ms", "")));
            if (s.endsWith("s"))
                return Duration.ofSeconds(Long.parseLong(s.replace("s", "")));
            return Duration.ofMillis(Long.parseLong(s));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Cannot parse retry interval: " + s);
        }
    }

    // ==================== CONFIG PRINT ====================

    private void printConfig(Instant startTime) {
        System.out.println("Configuration");
        System.out.printf("Nodes:\t\t\t\t%s%n", nodes);
        System.out.printf("Datacenter:\t\t\t%s%n", datacenter);
        System.out.printf("Mode:\t\t\t\t%s%n", mode);
        System.out.printf("Workload:\t\t\t%s%n", workload);
        System.out.printf("Timeout (default):\t\t%s%n", timeout);
        System.out.printf("Timeout (read):\t\t\t%s%n", readTimeout);
        System.out.printf("Timeout (write):\t\t%s%n", writeTimeout);
        System.out.printf("Timeout (CAS):\t\t\t%s%n", casTimeout);
        System.out.printf("Consistency level:\t\t%s%n", consistencyLevelStr);
        System.out.printf("Partition count:\t\t%d%n", partitionCount);
        if ("sequential".equals(workload) && partitionOffset != 0)
            System.out.printf("Partition offset:\t\t%d%n", partitionOffset);
        System.out.printf("Clustering rows:\t\t%d%n", clusteringRowCount);
        System.out.printf("Clustering row size:\t\t%s%n", clusteringRowSizeStr);
        System.out.printf("Rows per request:\t\t%d%n", rowsPerRequest);
        if ("read".equals(mode)) {
            System.out.printf("Provide upper bound:\t\t%b%n", provideUpperBound);
            System.out.printf("IN queries:\t\t\t%b%n", inRestriction);
            System.out.printf("Order by:\t\t\t%s%n", selectOrderBy);
            System.out.printf("No lower bound:\t\t\t%b%n", noLowerBound);
        }
        System.out.printf("Page size:\t\t\t%d%n", pageSize);
        System.out.printf("Concurrency:\t\t\t%d%n", concurrency);
        System.out.printf("Connections:\t\t\t%d%n", connectionCount);
        if (maximumRate > 0)
            System.out.printf("Maximum rate:\t\t\t%d op/s%n", maximumRate);
        else
            System.out.println("Maximum rate:\t\t\tunlimited");
        System.out.printf("Client compression:\t\t%b%n", clientCompression);
        System.out.printf("Max error/row:\t\t\t%s%n", maxErrorsAtRow == 0 ? "unlimited" : maxErrorsAtRow);
        System.out.printf("Max errors total:\t\t%s%n", maxErrors == 0 ? "unlimited" : maxErrors);
        System.out.printf("Retries (number):\t\t%d%n", retryNumber);
        System.out.printf("Retries (interval):\t\t%s%n", retryInterval);
        System.out.printf("Retry handler:\t\t\t%s%n", retryHandler);
        if ("timeseries".equals(workload)) {
            System.out.printf("Start timestamp:\t\t%d%n", startTime.toEpochMilli() * 1_000_000L);
            System.out.printf("Write rate:\t\t\t%d%n", maximumRate / partitionCount);
        }
    }

    private ResultConfiguration buildResultConfig() {
        ResultConfiguration cfg = new ResultConfiguration();
        cfg.measureLatency = measureLatency;
        cfg.concurrency = concurrency;
        cfg.hdrLatencyFile = hdrLatencyFile;
        cfg.setHdrLatencyUnits(hdrLatencyUnits);
        cfg.setHistogramConfiguration(
                50_000L, // 50 microseconds in nanoseconds
                timeout.multipliedBy(3).toNanos(),
                hdrLatencySigFig);
        cfg.latencyTypeToPrint = "fixed-coordinated-omission".equals(latencyType)
                ? ResultConfiguration.LATENCY_TYPE_CO_FIXED
                : ResultConfiguration.LATENCY_TYPE_RAW;
        return cfg;
    }

    // ==================== VERSION PROVIDER ====================

    /** Reads version info embedded at build time via Maven resource filtering. */
    static class VersionProvider implements CommandLine.IVersionProvider {
        @Override
        public String[] getVersion() throws Exception {
            java.util.Properties props = new java.util.Properties();
            try (InputStream is = VersionProvider.class.getResourceAsStream("/version.properties")) {
                if (is != null)
                    props.load(is);
            }
            String toolVersion = props.getProperty("tool.version", "unknown");
            String driverVersion = props.getProperty("driver.version", "unknown");
            String buildTime = props.getProperty("build.time", "unknown");

            return new String[] {
                    "scylla-bench-java " + toolVersion,
                    "ScyllaDB Java Driver: " + driverVersion,
                    "Build time: " + buildTime,
                    "",
                    "To build with a specific driver version:",
                    "  mvn package -Dscylla.driver.version=<version>"
            };
        }
    }
}
