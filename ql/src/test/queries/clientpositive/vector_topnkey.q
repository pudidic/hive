--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.vectorized.execution.enabled=true;
set hive.optimize.topnkey=true;

set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=true;
set hive.tez.dynamic.partition.pruning=true;
set hive.optimize.metadataonly=false;
set hive.optimize.index.filter=true;
set hive.tez.min.bloom.filter.entries=1;

set hive.tez.dynamic.partition.pruning=true;
set hive.stats.fetch.column.stats=true;
set hive.cbo.enable=true;

explain vectorization
SELECT key, SUM(CAST(SUBSTR(value,5) AS INT)) FROM src GROUP BY key ORDER BY key LIMIT 5;

SELECT key, SUM(CAST(SUBSTR(value,5) AS INT)) FROM src GROUP BY key ORDER BY key LIMIT 5;

explain vectorization
SELECT key FROM src GROUP BY key ORDER BY key LIMIT 5;

SELECT key FROM src GROUP BY key ORDER BY key LIMIT 5;

explain vectorization
SELECT src1.key, src2.value FROM src src1 JOIN src src2 ON (src1.key = src2.key) GROUP BY src1.key, src2.value ORDER BY src1.key LIMIT 5;

SELECT src1.key, src2.value FROM src src1 JOIN src src2 ON (src1.key = src2.key) GROUP BY src1.key, src2.value ORDER BY src1.key LIMIT 5;

explain vectorization
SELECT src1.key, src2.value FROM src src1 LEFT OUTER JOIN src src2 ON (src1.key = src2.key) GROUP BY src1.key, src2.value ORDER BY src1.key LIMIT 5;

SELECT src1.key, src2.value FROM src src1 LEFT OUTER JOIN src src2 ON (src1.key = src2.key) GROUP BY src1.key, src2.value ORDER BY src1.key LIMIT 5;

explain vectorization
SELECT src1.key, src2.value FROM src src1 RIGHT OUTER JOIN src src2 ON (src1.key = src2.key) GROUP BY src1.key, src2.value ORDER BY src1.key LIMIT 5;

SELECT src1.key, src2.value FROM src src1 RIGHT OUTER JOIN src src2 ON (src1.key = src2.key) GROUP BY src1.key, src2.value ORDER BY src1.key LIMIT 5;

explain vectorization
SELECT src1.key, src2.value FROM src src1 FULL OUTER JOIN src src2 ON (src1.key = src2.key) GROUP BY src1.key, src2.value ORDER BY src1.key LIMIT 5;

SELECT src1.key, src2.value FROM src src1 FULL OUTER JOIN src src2 ON (src1.key = src2.key) GROUP BY src1.key, src2.value ORDER BY src1.key LIMIT 5;

set hive.optimize.topnkey=false;

explain
SELECT src1.key, src2.value FROM src src1 FULL OUTER JOIN src src2 ON (src1.key = src2.key) GROUP BY src1.key, src2.value ORDER BY src1.key LIMIT 5;

SELECT src1.key, src2.value FROM src src1 FULL OUTER JOIN src src2 ON (src1.key = src2.key) GROUP BY src1.key, src2.value ORDER BY src1.key LIMIT 5;
