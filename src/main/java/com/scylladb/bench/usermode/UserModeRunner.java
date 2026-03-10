package com.scylladb.bench.usermode;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.scylladb.bench.results.TestThreadResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.FileReader;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Executes user-mode benchmarks driven by a YAML profile.
 *
 * A "user mode" benchmark is defined by:
 *   - A DDL profile (keyspace/table definitions)
 *   - Named queries with CQL text and bind parameter strategy
 *   - Relative ratios for how often each query is selected
 *   - Optional column generators for INSERT
 *
 * Mirrors cassandra-stress's `user profile=` mode.
 */
public class UserModeRunner {

    private static final Logger LOG = LoggerFactory.getLogger(UserModeRunner.class);

    private final CqlSession session;
    private final UserProfile profile;
    private final DefaultConsistencyLevel defaultCL;
    private final Duration readTimeout;
    private final Duration writeTimeout;
    private final Duration casTimeout;
    private final boolean lwtEnabled;

    /** Per-query prepared statements, lazily populated. */
    private final Map<String, PreparedStatement> prepared = new ConcurrentHashMap<>();

    /** Column generators keyed by column name. */
    private final Map<String, ColumnGenerator> generators = new LinkedHashMap<>();

    /** Ordered list of all columns in the table (from schema reflection). */
    private final List<String> allColumns = new ArrayList<>();

    /** Ordered list of partition key column names. */
    private final List<String> partitionKeys = new ArrayList<>();

    /** Ordered list of clustering column names. */
    private final List<String> clusteringCols = new ArrayList<>();

    // operation selector
    private final String[] opNames;
    private final double[] opCumWeights;

    // auto-generated INSERT statement
    private PreparedStatement insertPs;
    private List<String> insertColumns;  // column order in the auto-INSERT

    // global op counter for round-robin style selection
    private final AtomicLong opCounter = new AtomicLong(0);

    public UserModeRunner(CqlSession session, UserProfile profile,
                          DefaultConsistencyLevel defaultCL,
                          Duration readTimeout, Duration writeTimeout,
                          Duration casTimeout, boolean lwtEnabled) {
        this.session = session;
        this.profile = profile;
        this.defaultCL = defaultCL;
        this.readTimeout = readTimeout;
        this.writeTimeout = writeTimeout;
        this.casTimeout = casTimeout;
        this.lwtEnabled = lwtEnabled;

        // Build weighted op selector
        Map<String, UserProfile.QuerySpec> queries = profile.queries != null
                ? profile.queries : Collections.emptyMap();
        opNames = new String[queries.size()];
        opCumWeights = new double[queries.size()];
        double total = 0;
        int i = 0;
        for (Map.Entry<String, UserProfile.QuerySpec> e : queries.entrySet()) {
            opNames[i] = e.getKey();
            total += e.getValue().ratio > 0 ? e.getValue().ratio : 1.0;
            opCumWeights[i] = total;
            i++;
        }
        // normalize
        for (int j = 0; j < opCumWeights.length; j++) {
            opCumWeights[j] /= total;
        }
    }

    // ── schema setup ─────────────────────────────────────────────────────

    public void prepareSchema(int replicationFactor, boolean truncate) {
        // Create keyspace
        String ksdef = profile.keyspace_definition;
        if (ksdef != null && !ksdef.isBlank()) {
            session.execute(ksdef.trim());
        } else {
            session.execute(String.format(
                "CREATE KEYSPACE IF NOT EXISTS %s " +
                "WITH REPLICATION = {'class':'NetworkTopologyStrategy','replication_factor':%d}",
                profile.keyspace, replicationFactor));
        }

        // Create table
        String tbdef = profile.table_definition;
        if (tbdef != null && !tbdef.isBlank()) {
            session.execute(tbdef.trim());
        }

        // Extra definitions (indexes etc.)
        if (profile.extra_definitions != null) {
            for (String ddl : profile.extra_definitions) {
                if (ddl != null && !ddl.isBlank()) session.execute(ddl.trim());
            }
        }

        if (truncate) {
            session.execute("TRUNCATE TABLE " + profile.keyspace + "." + profile.table);
        }

        // Reflect table schema to build generators and insert statement
        reflectSchema();
    }

    private void reflectSchema() {
        Optional<KeyspaceMetadata> ksMeta = session.getMetadata()
                .getKeyspace(profile.keyspace);
        if (!ksMeta.isPresent()) {
            throw new IllegalStateException("Keyspace not found after creation: " + profile.keyspace);
        }
        Optional<TableMetadata> tbMeta = ksMeta.get().getTable(profile.table);
        if (!tbMeta.isPresent()) {
            throw new IllegalStateException("Table not found after creation: " + profile.table);
        }
        TableMetadata tm = tbMeta.get();

        // Build ordered column lists
        for (ColumnMetadata col : tm.getPartitionKey()) {
            partitionKeys.add(col.getName().asInternal());
        }
        for (ColumnMetadata col : tm.getClusteringColumns().keySet()) {
            clusteringCols.add(col.getName().asInternal());
        }
        for (Map.Entry<com.datastax.oss.driver.api.core.CqlIdentifier, ColumnMetadata> e
                : tm.getColumns().entrySet()) {
            allColumns.add(e.getKey().asInternal());
        }

        // Build generators for each column
        Map<String, UserProfile.ColumnSpec> specByName = new HashMap<>();
        if (profile.columnspec != null) {
            for (UserProfile.ColumnSpec cs : profile.columnspec) {
                if (cs.name != null) specByName.put(cs.name, cs);
            }
        }

        for (Map.Entry<com.datastax.oss.driver.api.core.CqlIdentifier, ColumnMetadata> e
                : tm.getColumns().entrySet()) {
            String colName = e.getKey().asInternal();
            String cqlType = e.getValue().getType().asCql(false, false);
            UserProfile.ColumnSpec spec = specByName.get(colName);
            generators.put(colName, new ColumnGenerator(colName, cqlType, spec));
        }

        // Build auto-INSERT statement: UPDATE ... SET non-key-cols WHERE pk AND ck
        buildInsertStatement();
    }

    private void buildInsertStatement() {
        List<String> pkAndCk = new ArrayList<>(partitionKeys);
        pkAndCk.addAll(clusteringCols);
        List<String> valueCols = new ArrayList<>();
        for (String c : allColumns) {
            if (!pkAndCk.contains(c)) valueCols.add(c);
        }

        insertColumns = new ArrayList<>();
        insertColumns.addAll(pkAndCk);
        insertColumns.addAll(valueCols);

        if (valueCols.isEmpty()) {
            // key-only table
            StringBuilder sb = new StringBuilder("INSERT INTO ")
                .append(profile.keyspace).append(".").append(profile.table)
                .append(" (");
            for (int i = 0; i < insertColumns.size(); i++) {
                if (i > 0) sb.append(", ");
                sb.append(insertColumns.get(i));
            }
            sb.append(") VALUES (");
            for (int i = 0; i < insertColumns.size(); i++) {
                if (i > 0) sb.append(", ");
                sb.append("?");
            }
            sb.append(")");
            if (lwtEnabled) sb.append(" IF NOT EXISTS");
            insertPs = session.prepare(sb.toString());
        } else {
            // UPDATE form: supports normal columns and counter appends
            StringBuilder sb = new StringBuilder("UPDATE ")
                .append(profile.keyspace).append(".").append(profile.table)
                .append(" SET ");
            for (int i = 0; i < valueCols.size(); i++) {
                if (i > 0) sb.append(", ");
                sb.append(valueCols.get(i)).append(" = ?");
            }
            sb.append(" WHERE ");
            for (int i = 0; i < pkAndCk.size(); i++) {
                if (i > 0) sb.append(" AND ");
                sb.append(pkAndCk.get(i)).append(" = ?");
            }
            if (lwtEnabled) sb.append(" IF EXISTS");
            // INSERT columns order: value cols first, then where-clause cols
            insertColumns = new ArrayList<>();
            insertColumns.addAll(valueCols);
            insertColumns.addAll(pkAndCk);
            insertPs = session.prepare(sb.toString());
        }
        LOG.info("Auto-generated INSERT/UPDATE: {}", insertPs.getQuery());
    }

    // ── op selection ─────────────────────────────────────────────────────

    /**
     * Selects the next operation name according to the configured ratios.
     */
    public String selectOperation() {
        if (opNames.length == 0) return null;
        if (opNames.length == 1) return opNames[0];
        double r = ThreadLocalRandom.current().nextDouble();
        for (int i = 0; i < opCumWeights.length; i++) {
            if (r <= opCumWeights[i]) return opNames[i];
        }
        return opNames[opNames.length - 1];
    }

    // ── execute one operation ────────────────────────────────────────────

    /**
     * Executes one operation (INSERT or a named query) and returns elapsed nanoseconds.
     * Pass null opName to run the auto-INSERT.
     */
    public long executeOp(String opName, TestThreadResult rb) throws Exception {
        // If the profile defines an explicit query with this name, always use it.
        UserProfile.QuerySpec spec = (opName != null && profile.queries != null)
                ? profile.queries.get(opName) : null;
        if (spec != null) {
            return executeQuery(opName, spec, rb);
        }
        // Fall back to auto-generated insert for null or "insert" when not explicitly defined.
        if (opName == null || opName.equals("insert")) {
            return executeInsert(rb);
        }
        throw new IllegalArgumentException("Unknown query name: " + opName);
    }

    // ── INSERT ───────────────────────────────────────────────────────────

    private long executeInsert(TestThreadResult rb) throws Exception {
        // Generate one row of values
        Object[] values = new Object[insertColumns.size()];
        for (int i = 0; i < insertColumns.size(); i++) {
            ColumnGenerator gen = generators.get(insertColumns.get(i));
            values[i] = gen != null ? gen.generateValue() : null;
        }

        BoundStatement bound = insertPs.bind(values)
                .setConsistencyLevel(defaultCL);
        if (lwtEnabled) {
            bound = bound.setSerialConsistencyLevel(DefaultConsistencyLevel.LOCAL_SERIAL);
        }
        bound = applyTimeout(bound, false);

        long start = System.nanoTime();
        session.execute(bound);
        long elapsed = System.nanoTime() - start;
        rb.incOps();
        rb.incRows();
        return elapsed;
    }

    // ── SELECT/QUERY ─────────────────────────────────────────────────────

    private long executeQuery(String name, UserProfile.QuerySpec spec,
                               TestThreadResult rb) throws Exception {
        PreparedStatement ps = prepared.computeIfAbsent(name, k -> {
            DefaultConsistencyLevel cl = spec.consistencyLevel != null
                    ? parseConsistency(spec.consistencyLevel) : defaultCL;
            return session.prepare(SimpleStatement.newInstance(spec.cql)
                    .setConsistencyLevel(cl));
        });

        // Determine how many bind variables the statement has
        int bindCount = ps.getVariableDefinitions().size();
        Object[] args = new Object[bindCount];

        boolean sameRow = "samerow".equalsIgnoreCase(spec.fields) || spec.fields == null;
        if (sameRow) {
            // Bind from one generated row
            Map<String, Object> row = generateRow();
            List<String> varNames = new ArrayList<>();
            ps.getVariableDefinitions().forEach(vd -> varNames.add(vd.getName().asInternal()));
            for (int i = 0; i < bindCount; i++) {
                String vn = varNames.get(i);
                Object v = row.get(vn);
                args[i] = v != null ? v : generateColumnValue(vn);
            }
        } else {
            // multirow: each ? comes from a randomly generated row
            List<String> varNames = new ArrayList<>();
            ps.getVariableDefinitions().forEach(vd -> varNames.add(vd.getName().asInternal()));
            for (int i = 0; i < bindCount; i++) {
                args[i] = generateColumnValue(varNames.get(i));
            }
        }

        // Convert byte[] to ByteBuffer for blob columns
        for (int i = 0; i < args.length; i++) {
            if (args[i] instanceof byte[]) {
                args[i] = ByteBuffer.wrap((byte[]) args[i]);
            }
        }

        BoundStatement bound = ps.bind(args);
        DefaultConsistencyLevel cl = spec.consistencyLevel != null
                ? parseConsistency(spec.consistencyLevel) : defaultCL;
        bound = bound.setConsistencyLevel(cl);
        bounded: {
            if (lwtEnabled) {
                bound = bound.setConsistencyLevel(DefaultConsistencyLevel.LOCAL_SERIAL);
            }
            bound = applyTimeout(bound, true);
        }

        long start = System.nanoTime();
        ResultSet rs = session.execute(bound);
        long elapsed = System.nanoTime() - start;
        for (Row row : rs) rb.incRows();
        rb.incOps();
        return elapsed;
    }

    // ── helpers ──────────────────────────────────────────────────────────

    /** Generates a full row as a Map of colName → value. */
    private Map<String, Object> generateRow() {
        Map<String, Object> row = new LinkedHashMap<>();
        for (Map.Entry<String, ColumnGenerator> e : generators.entrySet()) {
            row.put(e.getKey(), e.getValue().generateValue());
        }
        return row;
    }

    private Object generateColumnValue(String colName) {
        ColumnGenerator gen = generators.get(colName);
        if (gen != null) return gen.generateValue();
        return 0L; // fallback
    }

    private BoundStatement applyTimeout(BoundStatement stmt, boolean isRead) {
        Duration t = lwtEnabled ? casTimeout : (isRead ? readTimeout : writeTimeout);
        if (t != null && !t.isZero()) {
            return stmt.setTimeout(t);
        }
        return stmt;
    }

    private DefaultConsistencyLevel parseConsistency(String s) {
        if (s == null) return defaultCL;
        switch (s.toUpperCase()) {
            case "ANY":          return DefaultConsistencyLevel.ANY;
            case "ONE":          return DefaultConsistencyLevel.ONE;
            case "TWO":          return DefaultConsistencyLevel.TWO;
            case "THREE":        return DefaultConsistencyLevel.THREE;
            case "QUORUM":       return DefaultConsistencyLevel.QUORUM;
            case "ALL":          return DefaultConsistencyLevel.ALL;
            case "LOCAL_QUORUM": return DefaultConsistencyLevel.LOCAL_QUORUM;
            case "EACH_QUORUM":  return DefaultConsistencyLevel.EACH_QUORUM;
            case "LOCAL_ONE":    return DefaultConsistencyLevel.LOCAL_ONE;
            case "LOCAL_SERIAL": return DefaultConsistencyLevel.LOCAL_SERIAL;
            case "SERIAL":       return DefaultConsistencyLevel.SERIAL;
            default:
                LOG.warn("Unknown consistency level '{}', using default", s);
                return defaultCL;
        }
    }

    // ── profile loading ──────────────────────────────────────────────────

    /**
     * Loads and parses a YAML user profile from a file path.
     */
    public static UserProfile loadProfile(String path) throws Exception {
        Yaml yaml = new Yaml();
        try (FileReader reader = new FileReader(path)) {
            Map<String, Object> raw = yaml.load(reader);
            return mapToProfile(raw);
        }
    }

    @SuppressWarnings("unchecked")
    private static UserProfile mapToProfile(Map<String, Object> raw) {
        UserProfile p = new UserProfile();
        p.keyspace             = getString(raw, "keyspace");
        p.table                = getString(raw, "table");
        p.keyspace_definition  = getString(raw, "keyspace_definition");
        p.table_definition     = getString(raw, "table_definition");
        p.extra_definitions    = (List<String>) raw.get("extra_definitions");

        // columnspec
        Object csRaw = raw.get("columnspec");
        if (csRaw instanceof List) {
            p.columnspec = new ArrayList<>();
            for (Object item : (List<?>) csRaw) {
                if (item instanceof Map) {
                    Map<String,Object> m = (Map<String,Object>) item;
                    UserProfile.ColumnSpec cs = new UserProfile.ColumnSpec();
                    cs.name       = getString(m, "name");
                    cs.size       = getString(m, "size");
                    cs.population = getString(m, "population");
                    cs.cluster    = getString(m, "cluster");
                    p.columnspec.add(cs);
                }
            }
        }

        // insert
        Object insRaw = raw.get("insert");
        if (insRaw instanceof Map) {
            Map<String,Object> m = (Map<String,Object>) insRaw;
            p.insert = new UserProfile.InsertSpec();
            p.insert.partitions       = getString(m, "partitions");
            p.insert.batchtype        = getString(m, "batchtype");
            p.insert.consistencyLevel = getString(m, "consistencyLevel");
        }

        // queries
        Object qRaw = raw.get("queries");
        if (qRaw instanceof Map) {
            p.queries = new LinkedHashMap<>();
            for (Map.Entry<?,?> e : ((Map<?,?>)qRaw).entrySet()) {
                String qName = e.getKey().toString();
                if (e.getValue() instanceof Map) {
                    Map<String,Object> qm = (Map<String,Object>) e.getValue();
                    UserProfile.QuerySpec qs = new UserProfile.QuerySpec();
                    qs.cql              = getString(qm, "cql");
                    qs.fields           = getString(qm, "fields");
                    qs.consistencyLevel = getString(qm, "consistencyLevel");
                    Object ratioObj = qm.get("ratio");
                    if (ratioObj instanceof Number) qs.ratio = ((Number) ratioObj).doubleValue();
                    p.queries.put(qName, qs);
                }
            }
        }

        return p;
    }

    private static String getString(Map<?,?> m, String key) {
        Object v = m.get(key);
        return v != null ? v.toString() : null;
    }
}
