DROP DATABASE IF EXISTS codex_db0;
CREATE DATABASE codex_db0;
USE codex_db0;
SET SESSION sql_mode =
'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';
SET SESSION optimizer_switch =
'index_merge=on,index_merge_union=on,index_merge_sort_union=on,index_merge_intersection=on,engine_condition_pushdown=on,index_condition_pushdown=on,mrr=on,mrr_cost_based=on,block_nested_loop=on,batched_key_access=off,materialization=on,semijoin=on,loosescan=on,firstmatch=on,duplicateweedout=on,subquery_materialization_cost_based=on,use_index_extensions=on,condition_fanout_filter=on,derived_merge=on,use_invisible_indexes=off,skip_scan=on,hash_join=on,subquery_to_derived=off,prefer_ordering_index=on,hypergraph_optimizer=off,derived_condition_pushdown=on,hash_set_operations=on';
SET SESSION big_tables = 1;
SET SESSION internal_tmp_mem_storage_engine = 'MEMORY';
CREATE TABLE s(
  vp_rowid BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  c0 TEXT NULL,
  c1 DECIMAL(65,30) NULL,
  c2 BIGINT NULL,
  c3 BIGINT NULL,
  KEY idx_c3_c2(c3, c2)
) ENGINE=InnoDB;
INSERT INTO s(c0, c1, c2, c3) VALUES
  (NULL, NULL, 1, NULL),
  (NULL, 1143839529.000000000000000000000000000000, -268005709, 1);
CREATE TABLE l(
  vp_rowid BIGINT NOT NULL PRIMARY KEY,
  c1 DECIMAL(65,30) NULL,
  c2 BIGINT NULL,
  c3 BIGINT NULL
) ENGINE=InnoDB;
CREATE TABLE r(
  vp_rowid BIGINT NOT NULL PRIMARY KEY,
  c0 TEXT NULL
) ENGINE=InnoDB;
INSERT INTO l(vp_rowid, c1, c2, c3)
SELECT vp_rowid, c1, c2, c3 FROM s;
INSERT INTO r(vp_rowid, c0)
SELECT vp_rowid, c0 FROM s;

-- ORIGINAL SINGLE-TABLE QUERY
SELECT MIN(DISTINCT CAST((CAST(s.c1 AS SIGNED)) IN ((s.c0) != (s.c2)) AS SIGNED)) AS ref0
FROM s
WHERE IFNULL(0.9349479012631875, -1058210595) IN (s.c3)
GROUP BY EXISTS (SELECT 1);

-- SKIPPED TLP/NoREC: GROUP BY is not handled by this batch rewriter
