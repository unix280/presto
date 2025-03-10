=============
Release 0.292
=============

**Highlights**
==============

**Details**
===========

General Changes
_______________
* Fix Hive ``UUID`` type parsing. `#24538 <https://github.com/prestodb/presto/pull/24538>`_
* Fix Iceberg date column filtering. `#24583 <https://github.com/prestodb/presto/pull/24583>`_
* Fix OSS connectors affected by changes. `#24528 <https://github.com/prestodb/presto/pull/24528>`_
* Fix a security bug when ``check_access_control_for_utlized_columns`` is true for queries that uses a ``WITH`` clause. Previously we would sometime not check permissions for certain columns that were used in the query.  Now we will always check permissions for all columns used in the query. There are some corner cases for CTEs with the same name where we may check more columns than are used or fall back to checking all columns referenced in the query. `#24647 <https://github.com/prestodb/presto/pull/24647>`_
* Fix silently returning incorrect results when trying to construct a TimestampWithTimeZone from a value that has a unix timestamp that is too large/small. `#24674 <https://github.com/prestodb/presto/pull/24674>`_
* Improve error handling of ``INTERVAL DAY``, ``INTERVAL HOUR``, and ``INTERVAL SECOND`` operators when experiencing overflows. :pr:`24353`. `#24353 <https://github.com/prestodb/presto/pull/24353>`_
* Improve scheduling by using long instead of DataSize for critical path. `#24582 <https://github.com/prestodb/presto/pull/24582>`_
* Add :doc:`../troubleshoot` topic to the Presto documentation. `#24601 <https://github.com/prestodb/presto/pull/24601>`_
* Add Arrow Flight connector :pr:`24427`. `#24427 <https://github.com/prestodb/presto/pull/24427>`_
* Add a MySQL-compatible function ``bit_length`` that returns the count of bits for the given string. `#24531 <https://github.com/prestodb/presto/pull/24531>`_
* Add configuration property ``exclude-invalid-worker-session-properties``. `#23968 <https://github.com/prestodb/presto/pull/23968>`_
* Add documentation for file-based Hive metastore to :doc:`/installation/deployment`. `#24620 <https://github.com/prestodb/presto/pull/24620>`_
* Add documentation for the :doc:`/connector/base-arrow-flight`  :pr:`24427`. `#24427 <https://github.com/prestodb/presto/pull/24427>`_
* Add pagesink for DELETES to support future use. `#24528 <https://github.com/prestodb/presto/pull/24528>`_
* Add serialization for new types. `#24528 <https://github.com/prestodb/presto/pull/24528>`_
* Add support to build Presto with JDK 17. `#24677 <https://github.com/prestodb/presto/pull/24677>`_
* Added a new optimizer rule to add exchanges below a combination of partial aggregation+ GroupId . Enabled with the boolean session property `enable_forced_exchange_below_group_id`. `#24047 <https://github.com/prestodb/presto/pull/24047>`_
* Replace depreciated ``dagre-d3`` with ``dagre-d3-es`` in response to a high severity vulnerability [WS-2022-0322](https://github.com/opensearch-project/OpenSearch-Dashboards/issues/2482). `#24167 <https://github.com/prestodb/presto/pull/24167>`_
* Enable file-based hive metastore to use HDFS/S3 location as warehouse dir. `#24660 <https://github.com/prestodb/presto/pull/24660>`_
* Enable node pool type specification when reporting to the coordinator from a C++ worker. `#24569 <https://github.com/prestodb/presto/pull/24569>`_
* Make the number of task event loop configurable via a configuration file. `#24565 <https://github.com/prestodb/presto/pull/24565>`_
* Refactored org.apache.logging.log4j:log4j-api out if root POM. `#24605 <https://github.com/prestodb/presto/pull/24605>`_
* Refactored org.apache.logging.log4j:log4j-core out of root POM. `#24605 <https://github.com/prestodb/presto/pull/24605>`_
* Update beginDelete to return the new types, and finishDelete to accept the new types. `#24528 <https://github.com/prestodb/presto/pull/24528>`_
* Upgrade bootstrap to version 5. `#24167 <https://github.com/prestodb/presto/pull/24167>`_
* Upgrade jQuery to version 3.7.1. `#24167 <https://github.com/prestodb/presto/pull/24167>`_
* Upgrade libthrift to 0.14.1 in response to `CVE-2020-13949 <https://github.com/advisories/GHSA-g2fg-mr77-6vrm>`_. `#24462 <https://github.com/prestodb/presto/pull/24462>`_
* Upgrade netty dependencies to version 4.1.115.Final in response to `CVE-2024-47535 <https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2024-47535>`. `#24586 <https://github.com/prestodb/presto/pull/24586>`_
* Upgraded org.apache.logging.log4j:log4j-api from 2.17.1 to 2.24.3 in response to `CVE-2024-47554 <https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2024-47554>`. `#24507 <https://github.com/prestodb/presto/pull/24507>`_
* Upgraded org.apache.logging.log4j:log4j-core from 2.17.1 to 2.24.3 in response to `CVE-2024-47554<https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2024-47554>`. `#24507 <https://github.com/prestodb/presto/pull/24507>`_

Security Changes
________________
* Upgrade commons-text  to 1.13.0 in response to `CVE-2024-47554<https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2024-47554>`_. `#24467 <https://github.com/prestodb/presto/pull/24467>`_
* Upgrade org.apache.ratis  to 3.1.3 in response to `CVE-2020-15250<https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2020-15250>`_. `#24496 <https://github.com/prestodb/presto/pull/24496>`_

Hive Connector Changes
______________________
* Fix Parquet read failing for nested Decimal types :pr:`24440`. `#24440 <https://github.com/prestodb/presto/pull/24440>`_
* Fix getting views for Hive metastore 2.3+. `#24466 <https://github.com/prestodb/presto/pull/24466>`_
* Add session property ``hive.stats_based_filter_reorder_disabled`` for disabling reader stats based filter reordering. `#24630 <https://github.com/prestodb/presto/pull/24630>`_
* Replaced return type of beginDelete. `#24528 <https://github.com/prestodb/presto/pull/24528>`_
* Rename session property ``hive.stats_based_filter_reorder_disabled`` to ``hive.native_stats_based_filter_reorder_disabled``. `#24637 <https://github.com/prestodb/presto/pull/24637>`_

Iceberg Connector Changes
_________________________
* Fix IcebergTableHandle implementation to work with new types used in begin/finishDelete. `#24528 <https://github.com/prestodb/presto/pull/24528>`_
* Fix bug with missing statistics when the statistics file cache has a partial miss. `#24480 <https://github.com/prestodb/presto/pull/24480>`_
* Add ``read.split.target-size`` table property. `#24417 <https://github.com/prestodb/presto/pull/24417>`_
* Add ``target_split_size_bytes`` session property. `#24417 <https://github.com/prestodb/presto/pull/24417>`_
* Add a dedicated subclass of `FileHiveMetastore` for Iceberg connector to capture and isolate the differences in behavior. `#24573 <https://github.com/prestodb/presto/pull/24573>`_
* Add connector configuration property ``iceberg.catalog.hadoop.warehouse.datadir`` for Hadoop catalog to specify root data write path for its new created tables. `#24397 <https://github.com/prestodb/presto/pull/24397>`_
* Add logic to iceberg type converter for timestamp with timezone :pr:`23534`. `#23534 <https://github.com/prestodb/presto/pull/23534>`_
* Add manifest file caching for deployments which use the Hive metastore. `#24481 <https://github.com/prestodb/presto/pull/24481>`_
* Add support for the ``hive.affinity-scheduling-file-section-size`` configuration property and ``affinity_scheduling_file_section_size`` session property. `#24598 <https://github.com/prestodb/presto/pull/24598>`_
* Add support of ``renaming table`` for Iceberg connector when configured with ``HIVE`` file catalog. `#24312 <https://github.com/prestodb/presto/pull/24312>`_
* Add table properties ``write.data.path`` to specify independent data write paths for Iceberg tables. `#24397 <https://github.com/prestodb/presto/pull/24397>`_
* Enable manifest caching by default. `#24481 <https://github.com/prestodb/presto/pull/24481>`_
* Support for Iceberg table sort orders. Tables can be created to add a list of `sorted_by` columns which will be used to order files written to the table. `#21977 <https://github.com/prestodb/presto/pull/21977>`_

Kudu Connector Changes
______________________
* Replaced return type of beginDelete. `#24528 <https://github.com/prestodb/presto/pull/24528>`_

Tpc-ds Connector Changes
________________________
* Add config property ``tpcds.use-varchar-type`` to allow toggling of char columns to varchar columns. `#24406 <https://github.com/prestodb/presto/pull/24406>`_

SPI Changes
___________
* Add ConnectorSession as an argument to PlanChecker.validate and PlanChecker.validateFragment. `#24557 <https://github.com/prestodb/presto/pull/24557>`_
* Add DeleteTableHandle support these changes in Metadata. `#24528 <https://github.com/prestodb/presto/pull/24528>`_
* Add ``CoordinatorPlugin#getExpressionOptimizerFactories`` to customize expression evaluation in the Presto coordinator. :pr:`24144`. `#24144 <https://github.com/prestodb/presto/pull/24144>`_
* Add a separate ConnectorDeleteTableHandle interface for `ConnectorMetadata.beginDelete` and `ConnectorMetadata.finishDelete`, replacing the previous usage of ConnectorTableHandle. `#24528 <https://github.com/prestodb/presto/pull/24528>`_
* Move IndexSourceNode to the SPI. `#24678 <https://github.com/prestodb/presto/pull/24678>`_

Elastic Search Changes
______________________
* Improve cryptographic protocol in response to `java:S4423 <https://sonarqube.ow2.org/coding_rules?open=java%3AS4423&rule_key=java%3AS4423>`_. `#24474 <https://github.com/prestodb/presto/pull/24474>`_

Iceberg Changes
_______________
* Iceberg connector support for ``UPDATE`` SQL statements. `#24281 <https://github.com/prestodb/presto/pull/24281>`_

**Credits**
===========

Abe Varghese, Amit Dutta, Anant Aneja, Andrii Rosa, Bryan Cutler, Christian Zentgraf, Deepak Majeti, Denodo Research Labs, Dilli-Babu-Godari, Elbin Pallimalil, Eric Liu, Ge Gao, Jalpreet Singh Nanda, Joe Giardino, Ke, Kevin Tang, Kevin Wilfong, Krishna Pai, Mahadevuni Naveen Kumar, Mariam AlMesfer, Minhan Cao, Natasha Sehgal, Nicholas Ormrod, Nidhin Varghese, Nikhil Collooru, Patrick Sullivan, Pradeep Vaka, Pramod Satya, Pratik Joseph Dabre, Rebecca Schlussel, Reetika Agrawal, Sagar Sumit, Sayari Mukherjee, Sergey Pershin, Shahim Sharafudeen, Shakyan Kushwaha, Shang Ma, Shelton Cai, Steve Burnett, Sumi Mathew, Swapnil, Timothy Meehan, XiaoDu, Xiaoxuan Meng, Yihong Wang, Yihong Wang, Ying, Yuanda (Yenda) Li, Zac Blanco, Zac Wen, aditi-pandit, ajay-kharat, auden-woolfson, dnskr, inf, jay.narale, librian415, namya28, shenh062326, unidevel, vhsu14, wangd, wypb
