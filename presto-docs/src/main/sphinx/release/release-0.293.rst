=============
Release 0.293
=============

**Highlights**
==============

**Details**
===========

General Changes
_______________
* Fix the issue of sensitive data such as passwords and access keys being exposed in logs by redacting sensitive field values. `#24886 <https://github.com/prestodb/presto/pull/24886>`_
* Improve how we merge multiple operator stats together. `#24921 <https://github.com/prestodb/presto/pull/24921>`_
* Improve metrics creation by refactoring local variables to a dedicated class. `#24921 <https://github.com/prestodb/presto/pull/24921>`_
* Replace `exchange.compression-enabled`,  `fragment-result-cache.block-encoding-compression-enabled`, `experimental.spill-compression-enabled` with `exchange.compression-codec`, `fragment-result-cache.block-encoding-compression-codec` to enable compression codec configurations. Supported codecs include GZIP, LZ4, LZO, SNAPPY, ZLIB and ZSTD. `#24670 <https://github.com/prestodb/presto/pull/24670>`_

Prestissimo (Native Execution) Changes
______________________________________
* Fix REST API call ``v1/operator/task/getDetails?id=`` crash. `#24839 <https://github.com/prestodb/presto/pull/24839>`_

Prestissimo (native Execution) Changes
______________________________________
* Add runtime metrics collection for S3 Filesystem. `#24554 <https://github.com/prestodb/presto/pull/24554>`_
* Removes worker config `register-test-functions`. `#24853 <https://github.com/prestodb/presto/pull/24853>`_

Elasticsearch Connector Changes
_______________________________
* Upgrade elasticsearch to 7.17.27 in response to `CVE-2024-43709 <https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2024-43709>`_. `#23894 <https://github.com/prestodb/presto/pull/23894>`_

Hive Connector Changes
______________________
* Add support for Web Identity authentication in S3 security mapping with the ``hive.s3.webidentity.enabled`` property. `#24645 <https://github.com/prestodb/presto/pull/24645>`_

Iceberg Connector Changes
_________________________
* Fix to pass full session to avoid ``Unknown connector`` errors using the Nessie catalog. `#24803 <https://github.com/prestodb/presto/pull/24803>`_

Sql Server Connector Changes
____________________________
* Note: Starting from this version, the driver sets the encrypt property to true by default. If you are connecting to a non-SSL SQL Server instance, you must explicitly set encrypt=false in your connection configuration to avoid connectivity issues. This is a breaking change for existing connections. `#24686 <https://github.com/prestodb/presto/pull/24686>`_
* Upgraded SQL Server driver to version 12.8.1 to support NTLM authentication. See :ref:connector/sqlserver:authentication for more details. `#24686 <https://github.com/prestodb/presto/pull/24686>`_

**Credits**
===========

Akinori Musha, Amit Dutta, Anant Aneja, Bryan Cutler, Christian Zentgraf, Deepak Majeti, Denodo Research Labs, Elbin Pallimalil, Facebook Community Bot, Haritha Koloth, Hazmi, Jay Narale, Jialiang Tan, Li Zhou, Najib Adan, Nikhil Collooru, Nivin C S, Pradeep Vaka, Pramod Satya, Prashant Golash, Pratik Joseph Dabre, Rebecca Schlussel, Reetika Agrawal, Samuel Majoros, Sayari Mukherjee, Serge Druzkin, Sergey Pershin, Shang Ma, Shelton Cai, Shijin K, Steve Burnett, XiaoDu, Xin Zhang, Ying, Yuanda (Yenda) Li, Zac Blanco, Zac Wen, aditi-pandit, ebonnal, lukmanulhakkeem, wangd
