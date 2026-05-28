=============
Release 0.298
=============

**Breaking Changes**
====================

**Highlights**
==============

**Details**
===========

General Changes
_______________
* Fix 'planningTime' and 'finishingTime' are no longer added to the 'executionTime'. 'executionTime' is now a true execution time - how long it took the query to run the compute. It can be used to measure the efficiency of the workers w/o added planning time or the time spent on final steps, like partition registration. `#27691 <https://github.com/prestodb/presto/pull/27691>`_
* Fix RPC options argument parsing to use the last argument instead of hardcoding index 3. `#27700 <https://github.com/prestodb/presto/pull/27700>`_
* Fix UnsupportedOperationException when using `remote_function_names_for_fixed_parallelism` with queries containing UNION ALL below the remote function projection. `#27714 <https://github.com/prestodb/presto/pull/27714>`_
* Fix a bug in PushProjectionThroughCrossJoin optimizer rule where cascading projections above a cross join could cause validation errors by dropping pushed variables from intermediate residual projects. `#27568 <https://github.com/prestodb/presto/pull/27568>`_
* Fix a gap in query commit for DELETE queries when running on Spark. `#26195 <https://github.com/prestodb/presto/pull/26195>`_
* Fix data correctness bugs in ``MaterializedViewQueryOptimizer`` where queries without ``GROUP BY`` could be incorrectly rewritten to use materialized views with ``GROUP BY``, producing fewer rows than expected. Alias mismatches and scalar expression bypasses allowed invalid rewrites that silently collapsed duplicate rows. `#27778 <https://github.com/prestodb/presto/pull/27778>`_
* Fix failure during INSERT into Iceberg tables partitioned by day() when using timestamp with time zone columns. `#27645 <https://github.com/prestodb/presto/pull/27645>`_
* Fix materialized view query rewriting for ``CUBE``, ``ROLLUP``, and ``GROUPING SETS`` clauses. Column references inside these grouping elements are now correctly rewritten to materialized view columns. `#27538 <https://github.com/prestodb/presto/pull/27538>`_
* Fix race condition in pruneFinishedQueryInfo causing task memory leak. `#27597 <https://github.com/prestodb/presto/pull/27597>`_
* Fixed bug wtih http remote task with event loop opening futures that are never closed every cycle when the split queue is full. `#27673 <https://github.com/prestodb/presto/pull/27673>`_
* Improve PrefilterForLimitingAggregation optimizer to exclude partition keys from the DistinctLimit, improving convergence speed for GROUP BY + LIMIT queries on partitioned tables. `#27678 <https://github.com/prestodb/presto/pull/27678>`_
* Improve PrefilterForLimitingAggregation to use scan limiting instead of timeouts for more predictable performance. The optimization now limits the source scan to 1000 * LIMIT rows before applying DISTINCT LIMIT. `#27819 <https://github.com/prestodb/presto/pull/27819>`_
* Improve ``SimplifyPlanWithEmptyInput`` to prune empty subtrees that the connector PHYSICAL stage produces under local exchanges. Multi-source exchanges with mixed empty / non-empty children now drop the empty branches, eliminating idle no-op operators at runtime for wide ``UNION ALL`` queries where some branches are pruned to empty by partition / snapshot filtering. Single-source exchanges and write-side subtrees (``TableWriter`` / ``TableFinish``) are preserved. `#27765 <https://github.com/prestodb/presto/pull/27765>`_
* Improve efficiency of coordinator-to-worker communication with 20-40% smaller payload sizes and 2-3x faster serialization compared to JSON. `#27486 <https://github.com/prestodb/presto/pull/27486>`_
* Improve logical planner performance for wide-column queries by indexing RelationType.resolveFields() for O(1) field lookup instead of O(N) linear scan. `#27553 <https://github.com/prestodb/presto/pull/27553>`_
* Improve query planning performance for wide-column projections by adding fast paths that skip unnecessary processing for variable references, constants, and identity assignments across multiple optimizer rules. `#27547 <https://github.com/prestodb/presto/pull/27547>`_
* Add N <= 1000 limit guard to PrefilterForLimitingAggregation to restrict the optimization to small limits. `#27678 <https://github.com/prestodb/presto/pull/27678>`_
* Add ``ALTER MATERIALIZED VIEW <name> SET PROPERTIES (...)`` SQL statement to update materialized view properties after creation. :pr:`27806`. `#27806 <https://github.com/prestodb/presto/pull/27806>`_
* Add ``push_aggregation_through_disjoint_union`` session property (default off) that pushes a ``GROUP BY`` aggregation completely below ``UNION ALL`` when at least one grouping key has constant values that are pairwise distinct across the union branches, eliminating the final aggregation. `#27764 <https://github.com/prestodb/presto/pull/27764>`_
* Add ``rpc_dispatch_batch_size`` session property to control batch size for RPC dispatch in ``BATCH`` mode. Default: ``128``. A value of ``0`` collects all rows before dispatching. `#27700 <https://github.com/prestodb/presto/pull/27700>`_
* Add ``rpc_streaming_mode`` session property to control RPC function execution mode (``PER_ROW`` or ``BATCH``). Default: ``PER_ROW``. `#27700 <https://github.com/prestodb/presto/pull/27700>`_
* Add `partition_aware_grouped_execution` session property to schedule each (bucket, partition) as a separate lifespan in grouped execution, reducing per-lifespan data volumes for bucketed tables. Disabled by default. `#27663 <https://github.com/prestodb/presto/pull/27663>`_
* Add incremental refresh for materialized views. `#26959 <https://github.com/prestodb/presto/pull/26959>`_
* Add new session property `join_prefilter_build_side_with_complex_probe_side` (default false) to extend join prefilter optimization to support complex probe-side patterns including UNION ALL, cross join, unnest, and aggregation. `#27598 <https://github.com/prestodb/presto/pull/27598>`_
* Add optimizer rule RewriteBucketedSemiJoinToJoin that rewrites semi-joins into left joins with distinct aggregation when both sides are bucketed on the join key, avoiding data shuffle. Gated behind session property rewrite_bucketed_semi_join_to_join (default disabled). `#27510 <https://github.com/prestodb/presto/pull/27510>`_
* Add optimizer rule RewriteRowConstructorInToDisjunction that rewrites ROW IN ROW predicates into OR of AND equality chains when all ROW fields are partition keys, enabling per-column TupleDomain extraction for partition pruning. Gated behind session property rewrite_row_constructor_in_to_disjunction (default disabled). `#27500 <https://github.com/prestodb/presto/pull/27500>`_
* Add session property :ref:`admin/properties-session:\`\`always_analyze_create_table_query_enabled\`\`` to enable analyzing inner queries on ``CREATE TABLE AS SELECT IF NOT EXISTS`` statements when the target table already exists. `#27504 <https://github.com/prestodb/presto/pull/27504>`_
* Add support for ALTER COLUMN SET DATA TYPE in the Iceberg connector. `#25418 <https://github.com/prestodb/presto/pull/25418>`_
* Add support for Thrift serialization (`application/x-thrift-binary`, `application/x-thrift-compact`, `application/x-thrift-fb-compact`) to all TaskResource endpoints for consistent internal communication protocol. `#27486 <https://github.com/prestodb/presto/pull/27486>`_
* Add support for `ALTER TABLE ... ALTER COLUMN ... SET DEFAULT` syntax to update Iceberg column write-default values. `#27810 <https://github.com/prestodb/presto/pull/27810>`_
* Add support for ``GROUP BY`` and ``ORDER BY`` ordinal references in materialized view query rewriting. Previously, queries like ``SELECT a, SUM(b) FROM t GROUP BY 1`` would silently skip materialized view optimization. `#27422 <https://github.com/prestodb/presto/pull/27422>`_
* Adding presto-flight-shim server module for connector federation. `#26369 <https://github.com/prestodb/presto/pull/26369>`_
* Remove configuration property `use-new-nan-definition`. `#27829 <https://github.com/prestodb/presto/pull/27829>`_
* Remove the `warn-on-common-nan-patterns` configuration property and `warn_on_common_nan_patterns` session property. These properties controlled warnings for comparisons and divisions involving DOUBLE/REAL types during the NaN definition migration. The migration is complete and the warnings are no longer needed. `#27830 <https://github.com/prestodb/presto/pull/27830>`_
* Allow HAVING in queries that are transparently rewritten onto a materialized view. `#27677 <https://github.com/prestodb/presto/pull/27677>`_
* Optimize `map_from_entries(ARRAY[ROW(...), ...])` by rewriting to `MAP(ARRAY[keys], ARRAY[values])` at plan time, avoiding intermediate ROW construction. `#27491 <https://github.com/prestodb/presto/pull/27491>`_
* Update Google BigQuery Storage API SDK from v1beta1 to v1. `#27797 <https://github.com/prestodb/presto/pull/27797>`_
* Update the default behavior of ``field_names_in_json_cast_enabled`` from false to true. When ``field_names_in_json_cast_enabled = true``, JSON fields are assigned to ROW fields by matching field names regardless of their order in the JSON object. Queries that rely on JSON field order when casting to ROW may return different results after upgrading. If your workload depends on the previous positional behavior, restore it by setting: ``SET SESSION field_names_in_json_cast_enabled = false;``. `#26833 <https://github.com/prestodb/presto/pull/26833>`_

Prestissimo (native Execution) Changes
______________________________________
* Add support for iceberg V3 initialDefaultValue. `#27767 <https://github.com/prestodb/presto/pull/27767>`_

Security Changes
________________
* Add optional authorizedPrincipal to AuthorizedIdentity to support gateway identity propagation, allowing the session principal to reflect the original client instead of the gateway. `#27639 <https://github.com/prestodb/presto/pull/27639>`_
* Upgrade Netty to 4.2.13.Final in response to `CVE-2026-41417  <https://github.com/advisories/GHSA-fghv-69vj-qj49>` , `CVE-2026-44248  <https://github.com/advisories/GHSA-jfg9-48mv-9qgx>` , `CVE-2026-42577  <https://github.com/advisories/GHSA-rwm7-x88c-3g2p>` , `CVE-2026-42578  <https://github.com/advisories/GHSA-45q3-82m4-75jr>` , `CVE-2026-42579  <https://github.com/advisories/GHSA-cm33-6792-r9fm>` , `CVE-2026-42580  <https://github.com/advisories/GHSA-m4cv-j2px-7723>`, `CVE-2026-42581  <https://github.com/advisories/GHSA-xxqh-mfjm-7mv9>` , `CVE-2026-42582  <https://github.com/advisories/GHSA-2c5c-chwr-9hqw>` , `CVE-2026-42583  <https://github.com/advisories/GHSA-mj4r-2hfc-f8p6>` , `CVE-2026-42584  <https://github.com/advisories/GHSA-57rv-r2g8-2cj3>` , `CVE-2026-42585  <https://github.com/advisories/GHSA-38f8-5428-x5cv>` , `CVE-2026-42586  <https://github.com/advisories/GHSA-rgrr-p7gp-5xj7>` and `CVE-2026-42587  <https://github.com/advisories/GHSA-f6hv-jmp6-3vwv>`_. `#27769 <https://github.com/prestodb/presto/pull/27769>`_
* Upgrade async-http-client to version 3.0.9 to address `CVE-2026-40490 <https://github.com/advisories/GHSA-cmxv-58fp-fm3g>`_. `#27613 <https://github.com/prestodb/presto/pull/27613>`_
* Upgrade google-oauth-client version to 1.34.1 to address `CVE-2020-7692 <https://github.com/advisories/GHSA-f263-c949-w85g>`_ and `CVE-2021-22573 <https://github.com/advisories/GHSA-hw42-3568-wj87>`_. `#25424 <https://github.com/prestodb/presto/pull/25424>`_
* Upgrade http-proxy-middleware from 2.0.7 to 2.0.9  in /presto-ui/src to resolve `CVE-2025-32996 <https://nvd.nist.gov/vuln/detail/CVE-2025-32996>`_. `#27715 <https://github.com/prestodb/presto/pull/27715>`_
* Upgrade jackson dependency from 2.15.4 to version 2.18.6 to address `GHSA-72hv-8253-57qq <https://github.com/advisories/GHSA-72hv-8253-57qq>`_. `#27293 <https://github.com/prestodb/presto/pull/27293>`_
* Upgrade jetty dependency from 0.27 to version 2.0.2 to address `CVE-2025-11143 <https://github.com/advisories/GHSA-wjpw-4j6x-6rwh>` and `CVE-2026-1605 <https://github.com/advisories/GHSA-xxh7-fcf3-rj7f>`_. `#27294 <https://github.com/prestodb/presto/pull/27294>`_
* Upgrade libthrift 0.23.0 in response to `CVE-2026-41604 <https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2026-41604>`_. `#27777 <https://github.com/prestodb/presto/pull/27777>`_
* Upgrade lodash from 4.17.23 to 4.18.1 to address multiple security vulnerabilities: - `CVE-2026-4800 <https://nvd.nist.gov/vuln/detail/CVE-2026-4800>`_. This dependency is used for local development only and does not affect production runtime. `#27497 <https://github.com/prestodb/presto/pull/27497>`_
* Upgrade lodash-es from 4.17.23 to 4.18.1 to address `CVE-2026-4800 <https://nvd.nist.gov/vuln/detail/CVE-2026-4800>`_. This dependency is used for local development only and does not affect production runtime. `#27496 <https://github.com/prestodb/presto/pull/27496>`_
* Upgrade opentelemetry-api  to 1.62.0 in response to `CVE-2026-45292  <https://github.com/advisories/GHSA-fmxf-pm6p-7xgm>`_. `#27865 <https://github.com/prestodb/presto/pull/27865>`_
* Upgrade org.apache.kafka:kafka-clients from 3.9.1 to 3.9.2 inorder to address `CVE-2026-35554 <https://github.com/advisories/GHSA-5qcv-4rpc-jp93>`_. `#27574 <https://github.com/prestodb/presto/pull/27574>`_
* Upgrade org.apache.logging.log4j:log4j-core from 2.25.3 to 2.25.4 inorder to address `CVE-2026-34480 <https://nvd.nist.gov/vuln/detail/CVE-2026-34480>`_. `#27583 <https://github.com/prestodb/presto/pull/27583>`_
* Upgrade org.bouncycastle:bcprov-jdk18on from 1.81 to 1.84 to resolve `CVE-2026-0636 <https://nvd.nist.gov/vuln/detail/CVE-2026-0636>`_. `#27606 <https://github.com/prestodb/presto/pull/27606>`_
* Upgrade org.postgresql:postgresql from 42.7.9 to 42.7.11 to resolve `CVE-2026-42198 <https://nvd.nist.gov/vuln/detail/CVE-2026-42198>`_. `#27722 <https://github.com/prestodb/presto/pull/27722>`_
* Upgrade parquet-jackson to 1.17.1 in response to `GHSA-72hv-8253-57qq <https://github.com/advisories/GHSA-72hv-8253-57qq>`_. `#27803 <https://github.com/prestodb/presto/pull/27803>`_
* Upgrade redshift-jdbc42 to 2.2.7 in response to `CVE-2026-8178  <https://github.com/advisories/GHSA-wmmv-vvg5-993q>`_. `#27828 <https://github.com/prestodb/presto/pull/27828>`_

JDBC Driver Changes
___________________
* Add connection validation feature to enhance connection reliability. This can be enabled with the ``validateConnection`` connection property to execute a validation query immediately after establishing the connection. `#27002 <https://github.com/prestodb/presto/pull/27002>`_
* Add support for `execute` procedure in JDBC connectors. `#27282 <https://github.com/prestodb/presto/pull/27282>`_

Delta Lake Connector Changes
____________________________
* Fix a bug that made the metastore inconsistent if created a Delta Lake table to an inaccessible location. `#27129 <https://github.com/prestodb/presto/pull/27129>`_
* Add support for reading Delta Lake tables with column mapping enabled. `#27483 <https://github.com/prestodb/presto/pull/27483>`_

Hive Connector Changes
______________________
* Fix race where concurrent ``REFRESH MATERIALIZED VIEW`` on the same Hive-backed Iceberg materialized view could lose a watermark update. `#27835 <https://github.com/prestodb/presto/pull/27835>`_
* Add support for Azure Blob Storage and Azure Data Lake Storage Gen2 in the Hive connector. `#25107 <https://github.com/prestodb/presto/pull/25107>`_
* Add support for partition-aware grouped execution in the Hive connector, creating per-(bucket, partition) split queues and compound partition handles. `#27663 <https://github.com/prestodb/presto/pull/27663>`_
* Add support for shared key and OAuth2 authentication for Azure storage. `#25107 <https://github.com/prestodb/presto/pull/25107>`_
* Add support for wasb[s]:// and abfs[s]:// URI schemes. `#25107 <https://github.com/prestodb/presto/pull/25107>`_

Iceberg Connector Changes
_________________________
* Fix access control for materialized view storage tables when ``legacy_materialized_views=false``: storage-table access control is bypassed during MV expansion, while direct queries by name still go through access control. `#27728 <https://github.com/prestodb/presto/pull/27728>`_
* Add ``iceberg.materialized-view-default-max-snapshots-per-refresh`` config property and matching session property to set the default bound. `#27774 <https://github.com/prestodb/presto/pull/27774>`_
* Add ``iceberg.materialized-view-default-storage-schema`` config to route storage tables into a single schema. Defaults to the materialized view's own schema; per-MV ``storage_schema`` overrides. `#27728 <https://github.com/prestodb/presto/pull/27728>`_
* Add ``max_snapshots_per_refresh`` materialized view property to bound how far each base table advances per ``REFRESH MATERIALIZED VIEW``. Defaults to ``0`` (unbounded). Requires Iceberg V3 row lineage; V2 tables fall back to unbounded refresh. `#27774 <https://github.com/prestodb/presto/pull/27774>`_
* Add `materialized_view_stitching_strategy` and `materialized_view_incremental_refresh_strategy` session properties (values: `ALWAYS`, `NEVER`, `AUTOMATIC`; default: `ALWAYS`). Under `AUTOMATIC`, the optimizer selects between the rewrite and the full alternative based on cost; when stats are unavailable it falls back to row-count comparison. `#27820 <https://github.com/prestodb/presto/pull/27820>`_
* Add changes for passing iceberg V3 initialDefaultValue while read. `#27659 <https://github.com/prestodb/presto/pull/27659>`_
* Add incremental refresh for materialized views in the Iceberg connector. `#26959 <https://github.com/prestodb/presto/pull/26959>`_
* Add low and high values for varchar/char columns of Iceberg tables. `#27357 <https://github.com/prestodb/presto/pull/27357>`_
* Add metastore cache invalidation procedure for Iceberg connector. `#27200 <https://github.com/prestodb/presto/pull/27200>`_
* Add predicate push down on ``_last_updated_sequence_number`` for file-level pruning. `#27766 <https://github.com/prestodb/presto/pull/27766>`_
* Add read support for Iceberg V3 row lineage hidden columns `_row_id` and `_last_updated_sequence_number`. `#27240 <https://github.com/prestodb/presto/pull/27240>`_
* Add support for SMALLINT and TINYINT columns in presto-iceberg by mapping them to Iceberg INTEGER type. `#27461 <https://github.com/prestodb/presto/pull/27461>`_
* Add support for ``min/max/count`` aggregation push down based on file stats. This can be toggled with the ``aggregate_push_down_enabled`` session property or the ``iceberg.aggregate-push-down-enabled`` configuration property. `#27085 <https://github.com/prestodb/presto/pull/27085>`_
* Add support for updating column write-default values using `ALTER TABLE ... SET DEFAULT` (requires Iceberg format version 3+). `#27810 <https://github.com/prestodb/presto/pull/27810>`_
* Add warning when predicate stitching or incremental refresh falls back to full recompute. `#27816 <https://github.com/prestodb/presto/pull/27816>`_
* Update write-default operations to preserve existing initial-default values as metadata-only changes. `#27810 <https://github.com/prestodb/presto/pull/27810>`_

Lance Connector Changes
_______________________
* Add SQL filter pushdown to reduce data read from disk for selective queries. Supports equality, comparisons, IN lists, IS NULL, and range predicates on Boolean, Integer, Bigint, Real, Double, Varchar, Date, and Timestamp types. `#27430 <https://github.com/prestodb/presto/pull/27430>`_
* Add configurable index and metadata cache sizes via lance.index-cache-size and lance.metadata-cache-size. `#27325 <https://github.com/prestodb/presto/pull/27325>`_
* Add version-aware dataset caching with snapshot isolation for consistent query reads. `#27325 <https://github.com/prestodb/presto/pull/27325>`_

MongoDB Connector Changes
_________________________
* Add view querying capabilities in the Mongo connector. `#26995 <https://github.com/prestodb/presto/pull/26995>`_

Mongodb Connector Changes
_________________________
* Upgrade mongo-java-driver to mongodb-driver-sync. `#27685 <https://github.com/prestodb/presto/pull/27685>`_

Oracle Connector Changes
________________________
* Add Oracle i18n character set support. `#27670 <https://github.com/prestodb/presto/pull/27670>`_
* Add documentation for jdbc-fetch-size property in docs. `#27669 <https://github.com/prestodb/presto/pull/27669>`_
* Add fetch size to both PreparedStatement and ResultSet operations. `#27669 <https://github.com/prestodb/presto/pull/27669>`_

Prometheus Connector Changes
____________________________
* Add mixed case-sensitive identifier support for Prometheus connector. `#26260 <https://github.com/prestodb/presto/pull/26260>`_

Singlestore Connector Changes
_____________________________
* Fix TINYINT type mapping to preserve TINYINT semantics instead of incorrectly mapping to BOOLEAN after JDBC driver upgrade. `#27790 <https://github.com/prestodb/presto/pull/27790>`_
* Fix varchar type mapping for TEXT types to use byte-based thresholds matching the JDBC driver's COLUMN_SIZE reporting. `#27790 <https://github.com/prestodb/presto/pull/27790>`_

Native Sidecar Plugin Changes
_____________________________
* Add support for adding plugin loaded types in sidecar plugin. `#27748 <https://github.com/prestodb/presto/pull/27748>`_

Iceberg Changes
_______________
* Allow updating ``stale_read_behavior``, ``staleness_window``, and ``refresh_type`` on existing materialized views via ``ALTER MATERIALIZED VIEW ... SET PROPERTIES`` (requires ``legacy_materialized_views=false``). :pr:`27806`. `#27806 <https://github.com/prestodb/presto/pull/27806>`_

**Credits**
===========

Aditi Pandit, Allen Shen, Amit Dutta, Apurva Kumar, Arjun Gupta, Asish Kumar, Auden Woolfson, Ben Hu, Bryan Cutler, Chandrakant Vankayalapati, Christian Zentgraf, Daniel Bauer, Deepak Majeti, Deepak Mehra, Dilli Babu Godari, Dong Wang, Gary Helmling, Glerin Pinhero, Han Yan, Henry Dikeman, Jalpreet Singh Nanda, Jamille Shao-Ni, Jianjian Xie, Joe Abraham, Ke Wang, Kevin Tang, Konjac Huang, Li, Maria Basmanova, Miguel Blanco Godón, Nandakumar Balagopal, Natasha Sehgal, Naveen Mahadevuni, Nivin C S, Pramod Satya, Prashant Sharma, Pratik Joseph Dabre, Pratyaksh Sharma, Rebecca Schlussel, Reetika Agrawal, Rui Mo, Saurabh Mahawar, Sayari Mukherjee, Sergey Pershin, Shahim Sharafudeen, Shakyan Kushwaha, Shrinidhi Joshi, Sreeni Viswanadha, Steve Burnett, Swapnil, Timothy Meehan, Tirumala Saiteja Goruganthu, XiaoDu, Xiaoxuan, Yabin Ma, Yihong Wang, Zac, Zac Blanco, abhinavmuk04, bibith4, dependabot[bot], feilong-liu, inf, jkhaliqi, join-theory-de, mohsaka, nishithakbhaskaran, peterenescu, shelton408, sumi-mathew, vhsu14, zhichenxu-meta
