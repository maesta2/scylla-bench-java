package com.scylladb.bench.usermode;

import java.util.List;
import java.util.Map;

/**
 * POJO that maps to the YAML user-mode profile file.
 * Modelled after cassandra-stress's cqlstress YAML format.
 *
 * Example profile.yaml:
 * <pre>
 * keyspace: my_bench
 * keyspace_definition: "CREATE KEYSPACE ..."
 * table: events
 * table_definition: "CREATE TABLE ..."
 * columnspec:
 *   - name: user_id
 *     size: fixed:16
 *     population: uniform:1..1000000
 *   - name: ts
 *     cluster: fixed:100
 * insert:
 *   partitions: fixed:1
 *   batchtype: UNLOGGED
 * queries:
 *   read:
 *     cql: "SELECT * FROM events WHERE user_id = ?"
 *     fields: samerow
 *     ratio: 1
 * </pre>
 */
public class UserProfile {

    // ── required ──────────────────────────────────────────────────────────
    public String keyspace;
    public String table;

    // ── optional DDL ──────────────────────────────────────────────────────
    public String keyspace_definition;
    public String table_definition;
    public List<String> extra_definitions;

    // ── column generator tuning ───────────────────────────────────────────
    public List<ColumnSpec> columnspec;

    // ── insert behaviour ─────────────────────────────────────────────────
    public InsertSpec insert;

    // ── named queries ─────────────────────────────────────────────────────
    /** Map of queryName → QuerySpec */
    public Map<String, QuerySpec> queries;

    // ─────────────────────────────────────────────────────────────────────────

    public static class ColumnSpec {
        /** Column name in the table. Required. */
        public String name;

        /**
         * Distribution controlling how many distinct values this column ever takes.
         * Format: "fixed:N", "uniform:MIN..MAX", "gaussian:MIN..MAX", "seq:MIN..MAX"
         * Default: uniform:1..1000000
         */
        public String population;

        /**
         * Distribution controlling generated value size (bytes) for text/blob columns.
         * Default: fixed:4
         */
        public String size;

        /**
         * Distribution controlling the number of clustering rows generated per partition.
         * Only meaningful for the first clustering key column.
         * Default: fixed:1
         */
        public String cluster;
    }

    public static class InsertSpec {
        /**
         * How many partitions per batch write.
         * Format: distribution string e.g. "fixed:1", "uniform:1..10"
         * Default: fixed:1
         */
        public String partitions;

        /**
         * Batch type: UNLOGGED, LOGGED, COUNTER.
         * Default: UNLOGGED
         */
        public String batchtype;

        /**
         * Optional consistency level override for inserts.
         */
        public String consistencyLevel;
    }

    public static class QuerySpec {
        /**
         * The CQL query string. Use ? for bind parameters.
         * Parameter names are resolved from the column list in left-to-right order.
         * Required.
         */
        public String cql;

        /**
         * How to bind parameters:
         *   "samerow"  – all ? come from the same generated row (default, good for exact lookups)
         *   "multirow" – each ? picks from a random row in the partition (good for range queries)
         */
        public String fields;

        /**
         * Relative weight for operation selection when multiple queries are defined.
         * Default: 1
         */
        public double ratio = 1.0;

        /**
         * Optional consistency level override for this query.
         */
        public String consistencyLevel;
    }
}
