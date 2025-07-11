=============
Release 0.294
=============

**Highlights**
==============

**Details**
===========

General Changes
_______________
* Fix PushdownSubfields optimizer to enable subfield pushdown for maps which is accessed with negative keys. `#25445 <https://github.com/prestodb/presto/pull/25445>`_
* Fix error classification for unsupported array comparison with null elements, converting it as a user error. `#25187 <https://github.com/prestodb/presto/pull/25187>`_
* Fix randomize null join optimizer to keep HBO information for join input. `#25466 <https://github.com/prestodb/presto/pull/25466>`_
* Fix subfield pushdown arg index for scalar functions to support selecting whole struct column. `#25471 <https://github.com/prestodb/presto/pull/25471>`_
* Fix: Revert the Write mapping support. `#25369 <https://github.com/prestodb/presto/pull/25369>`_
* Fix: Revert the move of boostrap fromm presto-main to presto-bytecode. `#25260 <https://github.com/prestodb/presto/pull/25260>`_
* Improve `MinMaxByToWindowFunction` optimizer to cover cases where aggregation is on both map/array and non map/array types. `#25435 <https://github.com/prestodb/presto/pull/25435>`_
* Improve efficiency of output buffer implementation to reduce memory usage in writer. `#24913 <https://github.com/prestodb/presto/pull/24913>`_
* Improve query resource usage by enabling subfield pushdown for :func:`map_filter` when selected keys are constants. `#25451 <https://github.com/prestodb/presto/pull/25451>`_
* Improve query resource usage by enabling subfield pushdown for :func:`map_subset` when the input array is a constant array. `#25394 <https://github.com/prestodb/presto/pull/25394>`_
* Improve remove map cast rule to cover map_subset function. `#25395 <https://github.com/prestodb/presto/pull/25395>`_
* Improve semi join performance for large filtering tables. `#25236 <https://github.com/prestodb/presto/pull/25236>`_
* Add a function to Constraint to return the input arguments for the predicate. `#25248 <https://github.com/prestodb/presto/pull/25248>`_
* Add a new optimization `MinMaxByToWindowFunction` to rewrite min_by/max_by aggregations with row_number window function. `#25190 <https://github.com/prestodb/presto/pull/25190>`_
* Add a session property to disable the ReplicateSemiJoinInDelete optimizer. `#25256 <https://github.com/prestodb/presto/pull/25256>`_
* Add a session property to have HBO estimate plan node output size using individual variables. `#25400 <https://github.com/prestodb/presto/pull/25400>`_
* Add an optimizer to add distinct aggregation on build side of semi join. `#25238 <https://github.com/prestodb/presto/pull/25238>`_
* Add mixed case support for schema and table names. `#24551 <https://github.com/prestodb/presto/pull/24551>`_
* Add property ```native_query_memory_reclaimer_priority```  which controls which queries are killed first when a worker is running low on memory. Higher value means lower priority to be consistent with velox memory reclaimer's convention. `#25325 <https://github.com/prestodb/presto/pull/25325>`_
* Add pushdownSubfieldArgIndex parameter to ComplexTypeFunctionDescriptor for subfield optimization during query planning. `#25175 <https://github.com/prestodb/presto/pull/25175>`_
* Adds aggregation tests from ``presto-tests`` to run with native query runner in ``presto-native-tests``. `#24809 <https://github.com/prestodb/presto/pull/24809>`_
* Replace the parameters in router schedulers to use `RouterRequestInfo` to get the URL destination. `#25244 <https://github.com/prestodb/presto/pull/25244>`_
* ... `#25223 <https://github.com/prestodb/presto/pull/25223>`_
* ... `#25223 <https://github.com/prestodb/presto/pull/25223>`_
* Enable check_access_control_on_utilized_columns_only session flag to default true. `#25469 <https://github.com/prestodb/presto/pull/25469>`_
* Extend  MergePartialAggregationsWithFilter to work for queries where all aggregations have mask. `#25171 <https://github.com/prestodb/presto/pull/25171>`_
* Move UnnestNode to SPI, make it available in collector optimizer. `#25317 <https://github.com/prestodb/presto/pull/25317>`_
* Update ProtocolToThrift files to be generated for cpp thrift serde. `#25162 <https://github.com/prestodb/presto/pull/25162>`_
* Update router UI to eliminate vulnerabilities. `#25206 <https://github.com/prestodb/presto/pull/25206>`_

General Change Changes
______________________
* Improve memory usage in writer by feeing unused buffers. `#23724 <https://github.com/prestodb/presto/pull/23724>`_

Prestissimo (Native Execution) Changes
______________________________________
* Fix Native Plan Checker for CTAS and Insert queries. `#25115 <https://github.com/prestodb/presto/pull/25115>`_
* Fix PrestoExchangeSource 400 Bad Request by adding the "Host" header. `#25272 <https://github.com/prestodb/presto/pull/25272>`_

Prestissimo (native Execution) Changes
______________________________________
* Improve memory usage in PartitionAndSerialize Operator by pre-determine the serialized byte size of a given sort key at 'rowId'. This allows PartitionAndSerialize Operator to pre-allocated the exact output buffer size needed for serialization, avoid wasted memory allocation. User should expect lower memory usage and up to 20% runtime increase when serializing a sort key. `#25393 <https://github.com/prestodb/presto/pull/25393>`_
* Add BinarySortableSerializer::serializedSizeInBytes method that returns the serialized byte size of a given input row at 'rowId'. This allows us to pre-allocated the exact output buffer size needed for serialization, avoiding wasted memory space. `#25359 <https://github.com/prestodb/presto/pull/25359>`_
* Add sidecar in presto-native-tests module. `#25174 <https://github.com/prestodb/presto/pull/25174>`_

Security Changes
________________
* Upgrade commons-beanutils dependency to address 'CVE-2025-48734  <https://github.com/advisories/GHSA-wxr5-93ph-8wr9>'. `#25235 <https://github.com/prestodb/presto/pull/25235>`_
* Upgrade kafka to 3.9.1 in response to `CVE-2025-27817 <https://github.com/advisories/GHSA-vgq5-3255-v292>`_. :pr:`25312`. `#25312 <https://github.com/prestodb/presto/pull/25312>`_

JDBC Driver Changes
___________________
* Improve type mapping API to add WriteMapping functionality. `#25124 <https://github.com/prestodb/presto/pull/25124>`_
* Add mixed case support related catalog property in JDBC connector ``case-sensitive-name-matching``. `#24551 <https://github.com/prestodb/presto/pull/24551>`_

Hive Connector Changes
______________________
* Fix an issue while accessing Symlink tables. `#25307 <https://github.com/prestodb/presto/pull/25307>`_
* Fix incorrectly ignoring computed table statistics in `ANALYZE`. `#24973 <https://github.com/prestodb/presto/pull/24973>`_
* Improve split generation and read throughput for Symlink Tables. `#25277 <https://github.com/prestodb/presto/pull/25277>`_
* Update default value of `hive.copy-on-first-write-configuration-enabled` to false (:issue:`25404`). `#25420 <https://github.com/prestodb/presto/pull/25420>`_

Iceberg Connector Changes
_________________________
* Fix error querying ``$data_sequence_number`` metadata column for table with equality deletes. `#25293 <https://github.com/prestodb/presto/pull/25293>`_
* Fix the remove_orphan_files procedure after deletion operations. `#25220 <https://github.com/prestodb/presto/pull/25220>`_
* Add ``iceberg.delete-as-join-rewrite-max-delete-columns`` configuration property and ``delete_as_join_rewrite_max_delete_columns`` session property to control when equality delete as join optimization is applied. The optimization is now only applied when the number of equality delete columns is less than or equal to this threshold (default: 400). Setting this to 0 disables the optimization. See :doc:`/connector/iceberg` for details. `#25462 <https://github.com/prestodb/presto/pull/25462>`_
* Add support for ``$delete_file_path`` metadata column. `#25280 <https://github.com/prestodb/presto/pull/25280>`_
* Add support for ``$deleted`` metadata column. `#25280 <https://github.com/prestodb/presto/pull/25280>`_
* Add support of ``rename view`` for Iceberg connector when configured with ``REST`` and ``NESSIE``. `#25202 <https://github.com/prestodb/presto/pull/25202>`_
* Deprecate ``iceberg.delete-as-join-rewrite-enabled`` configuration property and ``delete_as_join_rewrite_enabled`` session property. Use ``iceberg.delete-as-join-rewrite-max-delete-columns`` instead. `#25462 <https://github.com/prestodb/presto/pull/25462>`_

JDBC Connector Changes
______________________
* Add skippable-schemas config option for jdbc connectors. `#24994 <https://github.com/prestodb/presto/pull/24994>`_

Mongodb Connector Changes
_________________________
* Add support for Json type in MongoDB. `#25089 <https://github.com/prestodb/presto/pull/25089>`_

Mysql Connector Changes
_______________________
* Add support for mixed-case in MySQL. It can be enabled by setting ``case-sensitive-name-matching=true`` configuration in the catalog configuration. `#24551 <https://github.com/prestodb/presto/pull/24551>`_

Redshift Connector Changes
__________________________
* Fix Redshift VARBYTE column handling for JDBC driver version 2.1.0.32+ by mapping jdbcType=1111 and jdbcTypeName="binary varying" to Presto's VARBINARY type. `#25488 <https://github.com/prestodb/presto/pull/25488>`_
* Fix Redshift connector runtime failure due to missing dependency on ``com.amazonaws.util.StringUtils``. Add ``aws-java-sdk-core`` as a runtime dependency to support Redshift JDBC driver (v2.1.0.32) which relies on this class for metadata operations. `#25265 <https://github.com/prestodb/presto/pull/25265>`_

Documentation Changes
_____________________
* Add :ref:`connector/hive:Avro Configuration Properties` to Hive Connector documentation. `#25311 <https://github.com/prestodb/presto/pull/25311>`_

Arrow Flight Connector Template Changes
_______________________________________
* Added support for mTLS authentication in Arrow Flight client. `#25179 <https://github.com/prestodb/presto/pull/25179>`_

Router Changes
______________
* Add a new custom router scheduler plugin, the `Presto Plan Checker Router Scheduler Plugin <https://github.com/prestodb/presto/tree/master/presto-plan-checker-router-plugin/README.md>`_. `#25035 <https://github.com/prestodb/presto/pull/25035>`_

**Credits**
===========

Amit Dutta, Anant Aneja, Andrew Xie, Andrii Rosa, Arjun Gupta, Auden Woolfson, Beinan, Chandra Vankayalapati, Chandrashekhar Kumar Singh, Chen Yang, Christian Zentgraf, Deepak Majeti, Denodo Research Labs, Elbin Pallimalil, Emily (Xuetong) Sun, Facebook Community Bot, Feilong Liu, Gary Helmling, Hazmi, HeidiHan0000, Henry Edwin Dikeman, Jalpreet Singh Nanda (:imjalpreet), Jialiang Tan, Joe Abraham, Ke Wang, Kevin Tang, Li Zhou, Mahadevuni Naveen Kumar, Mariam Almesfer, Natasha Sehgal, NivinCS, Ping Liu, Pramod Satya, Pratik Joseph Dabre, Rebecca Schlussel, Sebastiano Peluso, Sergey Pershin, Sergii Druzkin, Shahim Sharafudeen, Shakyan Kushwaha, Shang Ma, Shelton Cai, Soumya Duriseti, Steve Burnett, Thanzeel Hassan, Timothy Meehan, Wei He, XiaoDu, Xiaoxuan, Yihong Wang, Ying, Zac Blanco, Zac Wen, Zhichen Xu, Zhiying Liang, Zoltan Arnold Nagy, aditi-pandit, ajay kharat, jay.narale, lingbin, martinsander00, mima0000, mohsaka, namya28, pratyakshsharma, vhsu14, wangd
